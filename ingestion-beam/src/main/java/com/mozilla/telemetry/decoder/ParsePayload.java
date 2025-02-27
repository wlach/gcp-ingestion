/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.decoder;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.mozilla.telemetry.metrics.PerDocTypeCounter;
import com.mozilla.telemetry.schemas.JSONSchemaStore;
import com.mozilla.telemetry.schemas.SchemaNotFoundException;
import com.mozilla.telemetry.transforms.MapElementsWithErrors;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import com.mozilla.telemetry.util.Json;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.zip.CRC32;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.Validator;
import org.json.JSONObject;

/**
 * A {@code PTransform} that parses the message's payload as a {@link JSONObject}, sets
 * some attributes based on the content, and validates that it conforms to its schema.
 *
 * <p>There are several unrelated concerns all packed into this single transform so that we
 * incur the cost of parsing the JSON only once.
 */
public class ParsePayload extends MapElementsWithErrors.ToPubsubMessageFrom<PubsubMessage> {

  public static final String CLIENT_ID = "client_id";
  public static final String SAMPLE_ID = "sample_id";
  public static final String OS = "os";
  public static final String OS_VERSION = "os_version";

  public static ParsePayload of(ValueProvider<String> schemasLocation,
      ValueProvider<String> schemaAliasesLocation) {
    return new ParsePayload(schemasLocation, schemaAliasesLocation);
  }

  ////////

  private final Distribution parseTimer = Metrics.distribution(ParsePayload.class,
      "json_parse_millis");
  private final Distribution validateTimer = Metrics.distribution(ParsePayload.class,
      "json_validate_millis");

  private final ValueProvider<String> schemasLocation;
  private final ValueProvider<String> schemaAliasesLocation;

  private transient Validator validator;
  private transient JSONSchemaStore schemaStore;
  private transient CRC32 crc32;

  private ParsePayload(ValueProvider<String> schemasLocation,
      ValueProvider<String> schemaAliasesLocation) {
    this.schemasLocation = schemasLocation;
    this.schemaAliasesLocation = schemaAliasesLocation;
  }

  @Override
  protected PubsubMessage processElement(PubsubMessage message)
      throws SchemaNotFoundException, IOException, MessageShouldBeDroppedException {
    message = PubsubConstraints.ensureNonNull(message);
    Map<String, String> attributes = new HashMap<>(message.getAttributeMap());

    if (schemaStore == null) {
      schemaStore = JSONSchemaStore.of(schemasLocation, schemaAliasesLocation);
    }

    final int submissionBytes = message.getPayload().length;

    JSONObject json;
    try {
      json = parseTimed(message.getPayload());
    } catch (IOException e) {
      Map<String, String> attrs = schemaStore.docTypeExists(message.getAttributeMap())
          ? message.getAttributeMap()
          : null; // null attributes will cause docType to show up as "unknown_doc_type" in metrics
      PerDocTypeCounter.inc(attrs, "error_json_parse");
      PerDocTypeCounter.inc(attrs, "error_submission_bytes", submissionBytes);
      throw e;
    }

    if (MessageScrubber.shouldScrub(attributes, json)) {
      // Prevent the message from going to success or error output.
      throw new MessageShouldBeDroppedException();
    }

    // In case this message is being replayed from an error output where AddMetadata has already
    // been applied, we strip out any existing metadata fields and put them into attributes.
    AddMetadata.stripPayloadMetadataToAttributes(attributes, json);

    boolean validDocType = schemaStore.docTypeExists(attributes);
    if (!validDocType) {
      PerDocTypeCounter.inc(null, "error_invalid_doc_type");
      PerDocTypeCounter.inc(null, "error_submission_bytes", submissionBytes);
      throw new SchemaNotFoundException(String.format("No such docType: %s/%s",
          attributes.get("document_namespace"), attributes.get("document_type")));
    }

    // If no "document_version" attribute was parsed from the URI, this element must be from the
    // /submit/telemetry endpoint and we now need to grab version from the payload.
    if (!attributes.containsKey("document_version")) {
      if (json.has("version")) {
        String version = json.get("version").toString();
        attributes.put(ParseUri.DOCUMENT_VERSION, version);
      } else if (json.has("v")) {
        String version = json.get("v").toString();
        attributes.put(ParseUri.DOCUMENT_VERSION, version);
      } else {
        PerDocTypeCounter.inc(attributes, "error_missing_version");
        PerDocTypeCounter.inc(attributes, "error_submission_bytes", submissionBytes);
        throw new SchemaNotFoundException("Element was assumed to be a telemetry message because"
            + " it contains no document_version attribute, but the payload does not include"
            + " the top-level 'version' or 'v' field expected for a telemetry document");
      }
    }

    // Throws SchemaNotFoundException if there's no schema
    Schema schema;
    try {
      schema = schemaStore.getSchema(attributes);
    } catch (SchemaNotFoundException e) {
      PerDocTypeCounter.inc(attributes, "error_schema_not_found");
      PerDocTypeCounter.inc(attributes, "error_submission_bytes", submissionBytes);
      throw e;
    }

    try {
      validateTimed(schema, json);
    } catch (ValidationException e) {
      PerDocTypeCounter.inc(attributes, "error_schema_validation");
      PerDocTypeCounter.inc(attributes, "error_submission_bytes", submissionBytes);
      throw e;
    }

    addAttributesFromPayload(attributes, json);

    byte[] normalizedPayload = json.toString().getBytes();

    PerDocTypeCounter.inc(attributes, "valid_submission");
    PerDocTypeCounter.inc(attributes, "valid_submission_bytes", submissionBytes);

    return new PubsubMessage(normalizedPayload, attributes);
  }

  private void addAttributesFromPayload(Map<String, String> attributes, JSONObject json) {

    // Try to get glean-style client_info object.
    Optional<JSONObject> gleanClientInfo = Optional.of(json) //
        .map(j -> j.optJSONObject("client_info"));

    // Try to get "common ping"-style os object.
    Optional<JSONObject> commonPingOs = Optional.of(json) //
        .map(j -> j.optJSONObject("environment")) //
        .map(j -> j.optJSONObject("system")) //
        .map(j -> j.optJSONObject("os"));

    if (gleanClientInfo.isPresent()) {
      // See glean ping structure in:
      // https://github.com/mozilla-services/mozilla-pipeline-schemas/blob/da4a1446efd948399eb9eade22f6fcbc5557f588/schemas/glean/baseline/baseline.1.schema.json
      Optional.ofNullable(gleanClientInfo.get().optString("app_channel"))
          .filter(v -> !Strings.isNullOrEmpty(v))
          .ifPresent(v -> attributes.put(ParseUri.APP_UPDATE_CHANNEL, v));
      Optional.ofNullable(gleanClientInfo.get().optString(OS))
          .filter(v -> !Strings.isNullOrEmpty(v)).ifPresent(v -> attributes.put(OS, v));
      Optional.ofNullable(gleanClientInfo.get().optString(OS_VERSION))
          .filter(v -> !Strings.isNullOrEmpty(v)).ifPresent(v -> attributes.put(OS_VERSION, v));
      Optional.ofNullable(gleanClientInfo.get().optString(CLIENT_ID))
          .filter(v -> !Strings.isNullOrEmpty(v)).ifPresent(v -> attributes.put(CLIENT_ID, v));
    } else if (commonPingOs.isPresent()) {
      // See common ping structure in:
      // https://firefox-source-docs.mozilla.org/toolkit/components/telemetry/telemetry/data/common-ping.html
      Optional.ofNullable(commonPingOs.get().optString("name"))
          .filter(v -> !Strings.isNullOrEmpty(v)).ifPresent(v -> attributes.put(OS, v));
      Optional.ofNullable(commonPingOs.get().optString("version"))
          .filter(v -> !Strings.isNullOrEmpty(v)).ifPresent(v -> attributes.put(OS_VERSION, v));
    } else {
      // Try to extract "core ping"-style values; see
      // https://github.com/mozilla-services/mozilla-pipeline-schemas/blob/da4a1446efd948399eb9eade22f6fcbc5557f588/schemas/telemetry/core/core.10.schema.json
      Optional.ofNullable(json.optString(OS)).filter(v -> !Strings.isNullOrEmpty(v))
          .ifPresent(v -> attributes.put(OS, v));
      Optional.ofNullable(json.optString("osversion")).filter(v -> !Strings.isNullOrEmpty(v))
          .ifPresent(v -> attributes.put(OS_VERSION, v));
    }

    // Try extracting variants of top-level client id.
    Optional.ofNullable(json.optString(CLIENT_ID)).filter(v -> !Strings.isNullOrEmpty(v))
        .ifPresent(v -> attributes.put(CLIENT_ID, v));
    Optional.ofNullable(json.optString("clientId")).filter(v -> !Strings.isNullOrEmpty(v))
        .ifPresent(v -> attributes.put(CLIENT_ID, v));

    // Add sample id.
    Optional.ofNullable(attributes.get(CLIENT_ID)) //
        .ifPresent(v -> attributes.put(SAMPLE_ID, Long.toString(calculateSampleId(v))));
  }

  @VisibleForTesting
  long calculateSampleId(String clientId) {
    if (crc32 == null) {
      crc32 = new CRC32();
    }
    crc32.reset();
    crc32.update(clientId.getBytes());
    return crc32.getValue() % 100;
  }

  private JSONObject parseTimed(byte[] bytes) throws IOException {
    long startTime = System.currentTimeMillis();
    final JSONObject json = Json.readJSONObject(bytes);
    long endTime = System.currentTimeMillis();
    parseTimer.update(endTime - startTime);
    return json;
  }

  private void validateTimed(Schema schema, JSONObject json) {
    if (validator == null) {
      // Without failEarly(), a pathological payload may cause the validator to consume all memory;
      // https://github.com/mozilla/gcp-ingestion/issues/374
      validator = Validator.builder().failEarly().build();
    }
    long startTime = System.currentTimeMillis();
    validator.performValidation(schema, json);
    long endTime = System.currentTimeMillis();
    validateTimer.update(endTime - startTime);
  }
}
