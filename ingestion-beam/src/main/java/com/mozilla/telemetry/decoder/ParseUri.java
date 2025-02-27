/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.decoder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.mozilla.telemetry.transforms.MapElementsWithErrors;
import com.mozilla.telemetry.transforms.PubsubConstraints;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.json.JSONObject;

public class ParseUri extends MapElementsWithErrors.ToPubsubMessageFrom<PubsubMessage> {

  public static ParseUri of() {
    return INSTANCE;
  }

  ////////

  private static class InvalidUriException extends Exception {

    InvalidUriException() {
      super();
    }

    InvalidUriException(String message) {
      super(message);
    }
  }

  private static class UnexpectedPathElementsException extends InvalidUriException {

    UnexpectedPathElementsException(int numUnexpectedElements) {
      super(String.format("Found %d more path elements in the URI than expected for this endpoint",
          numUnexpectedElements));
    }
  }

  private static class NullUriException extends InvalidUriException {
  }

  private ParseUri() {
  }

  private static final ParseUri INSTANCE = new ParseUri();

  public static final String DOCUMENT_NAMESPACE = "document_namespace";
  public static final String DOCUMENT_TYPE = "document_type";
  public static final String DOCUMENT_VERSION = "document_version";
  public static final String DOCUMENT_ID = "document_id";
  public static final String APP_NAME = "app_name";
  public static final String APP_VERSION = "app_version";
  public static final String APP_UPDATE_CHANNEL = "app_update_channel";
  public static final String APP_BUILD_ID = "app_build_id";
  public static final String TELEMETRY = "telemetry";

  public static final String TELEMETRY_URI_PREFIX = "/submit/telemetry/";
  public static final String[] TELEMETRY_URI_SUFFIX_ELEMENTS = new String[] { //
      DOCUMENT_ID, DOCUMENT_TYPE, APP_NAME, APP_VERSION, APP_UPDATE_CHANNEL, APP_BUILD_ID };
  public static final String GENERIC_URI_PREFIX = "/submit/";
  public static final String[] GENERIC_URI_SUFFIX_ELEMENTS = new String[] { //
      DOCUMENT_NAMESPACE, DOCUMENT_TYPE, DOCUMENT_VERSION, DOCUMENT_ID };

  private static Map<String, String> zip(String[] keys, String[] values)
      throws InvalidUriException {
    Map<String, String> map = new HashMap<>();
    if (keys.length != values.length) {
      throw new UnexpectedPathElementsException(values.length - keys.length);
    }
    for (int i = 0; i < keys.length; i++) {
      map.put(keys[i], values[i]);
    }
    return map;
  }

  @Override
  protected PubsubMessage processElement(PubsubMessage message) throws InvalidUriException {
    message = PubsubConstraints.ensureNonNull(message);
    // Copy attributes
    final Map<String, String> attributes = new HashMap<>(message.getAttributeMap());
    byte[] payload = message.getPayload();

    // parse uri based on prefix
    final String uri = attributes.get("uri");
    if (uri == null) {
      throw new NullUriException();
    } else if (uri.startsWith(TELEMETRY_URI_PREFIX)) {
      // We don't yet have access to the version field, so we delay populating the document_version
      // attribute until the ParsePayload step where we have map-like access to the JSON content.
      attributes.put(DOCUMENT_NAMESPACE, TELEMETRY);
      attributes.putAll(zip(TELEMETRY_URI_SUFFIX_ELEMENTS,
          uri.substring(TELEMETRY_URI_PREFIX.length()).split("/")));
    } else if (uri.startsWith(GENERIC_URI_PREFIX)) {
      attributes.putAll(
          zip(GENERIC_URI_SUFFIX_ELEMENTS, uri.substring(GENERIC_URI_PREFIX.length()).split("/")));
    } else if (uri.startsWith(StubUri.PREFIX)) {
      payload = StubUri.parse(uri, attributes);
    } else {
      throw new InvalidUriException("Unknown URI prefix");
    }
    return new PubsubMessage(payload, attributes);
  }

  /**
   * Class for parsing stub installer pings.
   *
   * <p>Support for stub installer pings as per
   * https://github.com/mozilla/gcp-ingestion/blob/master/docs/edge.md#legacy-systems
   *
   * <p>Reimplementation of
   * https://github.com/whd/dsmo_load/blob/master/heka/usr/share/heka/lua_filters/nginx_redshift.lua#L71-L194
   *
   * <p>Note that some fields have been renamed or modified from the lua implementation
   * to match the firefox-installer.install.1 schema
   * https://github.com/mozilla-services/mozilla-pipeline-schemas/blob/dev/schemas/firefox-installer/install/install.1.schema.json
   *
   * <p>For info about funnelcake see https://wiki.mozilla.org/Funnelcake
   */
  public static class StubUri {

    private StubUri() {
    }

    private static class UnknownPingVersionException extends InvalidUriException {

      UnknownPingVersionException(String version) {
        super("For input string: " + version);
      }
    }

    private static class InvalidIntegerException extends InvalidUriException {

      InvalidIntegerException(int position, String value) {
        super(String.format("Path element #%d: %s", position, value));
      }
    }

    public static final String PREFIX = "/stub/";
    public static final String VERSION = "version";
    public static final String FUNNELCAKE = "funnelcake";

    public static final Pattern PING_VERSION_PATTERN = Pattern
        .compile(String.format("^v(?<%s>[6-8])(-(?<%s>\\d+))?$", VERSION, FUNNELCAKE));

    public static final Map<String, Integer> SUFFIX_LENGTH = ImmutableMap.of("6", 36, "7", 37, "8",
        39);

    public static final List<BiConsumer<String, JSONObject>> HANDLERS = ImmutableList.of(//
        ignore(), // ping_version handled by parsePingVersion
        putString("build_channel"), putString("update_channel"), putString("locale"),
        // Build architecture code
        putBoolPerCode(ImmutableMap.of("64bit_build", 1)),
        // OS architecture code
        putBoolPerCode(ImmutableMap.of("64bit_os", 1)),
        // Join three fields on "." to get os version
        appendString(ParsePayload.OS_VERSION, "."), //
        appendString(ParsePayload.OS_VERSION, "."), //
        appendString(ParsePayload.OS_VERSION, "."), //
        putString("service_pack"), putBool("server_os"),
        // Exit code
        putBoolPerCodeSet(new ImmutableMap.Builder<String, Set<Integer>>()
            .put("succeeded", ImmutableSet.of(0)).put("user_cancelled", ImmutableSet.of(10))
            .put("out_of_retries", ImmutableSet.of(11)).put("file_error", ImmutableSet.of(20))
            .put("sig_not_trusted", ImmutableSet.of(21, 23))
            .put("sig_unexpected", ImmutableSet.of(22, 23))
            .put("install_timeout", ImmutableSet.of(30)).build()),
        // Launch code
        putBoolPerCode(ImmutableMap.of("old_running", 1, "new_launched", 2)),
        putInteger("download_retries"), putInteger("bytes_downloaded"), putInteger("download_size"),
        putInteger("intro_time"), putInteger("options_time"), //
        putInteger("download_time"), // formerly download_phase_time
        ignore(), // formerly download_time
        putInteger("download_latency"), putInteger("preinstall_time"), putInteger("install_time"),
        putInteger("finish_time"),
        // Initial install requirements code
        putBoolPerCode(ImmutableMap.of("disk_space_error", 1, "no_write_access", 2)),
        putBool("manual_download"), putBool("had_old_install"), putString("old_version"),
        putString("old_build_id"), putString("version"), putString("build_id"),
        putBool("default_path"), putBool("admin_user"),
        // Default browser status code
        putBoolPerCode(ImmutableMap.of("new_default", 1, "old_default", 2)),
        // Default browser setting code
        putBoolPerCode(ImmutableMap.of("set_default", 2)), //
        putString("download_ip"), putString("attribution"),
        putIntegerAsString("profile_cleanup_prompt"), putBool("profile_cleanup_requested"));

    private static BiConsumer<String, JSONObject> ignore() {
      return (value, payload) -> {
      };
    }

    private static BiConsumer<String, JSONObject> putString(String key) {
      return (value, payload) -> payload.put(key, value);
    }

    private static BiConsumer<String, JSONObject> appendString(String key, String separator) {
      return (value, payload) -> payload.put(key,
          Optional.ofNullable(payload.optString(key, null)).map(v -> v + separator).orElse("")
              + value);
    }

    private static BiConsumer<String, JSONObject> putBool(String key) {
      return (value, payload) -> payload.put(key, value.equals("1"));
    }

    private static BiConsumer<String, JSONObject> putInteger(String key) {
      return (value, payload) -> payload.put(key, Integer.parseInt(value));
    }

    private static BiConsumer<String, JSONObject> putIntegerAsString(String key) {
      return (value, payload) -> payload.put(key, Integer.toString(Integer.parseInt(value)));
    }

    private static BiConsumer<String, JSONObject> putBoolPerCode(Map<String, Integer> fieldCodes) {
      return (string, payload) -> {
        Integer value = Integer.parseInt(string);
        fieldCodes.forEach((key, code) -> payload.put(key, value.equals(code)));
      };
    }

    private static BiConsumer<String, JSONObject> putBoolPerCodeSet(
        Map<String, Set<Integer>> fieldCodes) {
      return (string, payload) -> {
        Integer value = Integer.parseInt(string);
        fieldCodes.forEach((key, set) -> payload.put(key, set.contains(value)));
      };
    }

    private static JSONObject parsePingVersion(String[] elements) throws InvalidUriException {
      // Parse ping version using a regex pattern
      String pingVersion = elements[0];
      Matcher matcher = PING_VERSION_PATTERN.matcher(pingVersion);
      if (!matcher.find()) {
        throw new UnknownPingVersionException(pingVersion);
      }
      // Check length of elements is valid for this version
      int unexpectedElements = elements.length - SUFFIX_LENGTH.get(matcher.group(VERSION));
      if (unexpectedElements != 0) {
        throw new UnexpectedPathElementsException(unexpectedElements);
      }
      // Initialize new payload with ping version and funnelcake ID
      JSONObject payload = new JSONObject();
      payload.put("installer_type", "stub");
      payload.put("installer_version", ""); // it's required but stub pings don't have it
      payload.put("ping_version", pingVersion);
      Optional.ofNullable(matcher.group(FUNNELCAKE)).ifPresent(v -> payload.put(FUNNELCAKE, v));
      return payload;
    }

    private static byte[] parse(String uri, Map<String, String> attributes)
        throws InvalidUriException {
      attributes.put("document_namespace", "firefox-installer");
      attributes.put("document_type", "install");
      attributes.put("document_version", "1");
      // Split uri into path elements
      String[] elements = uri.substring(PREFIX.length()).split("/");
      // Initialize payload based on ping version
      JSONObject payload = parsePingVersion(elements);
      // Update payload with values from path elements
      for (int i = 0; i < elements.length; i++) {
        try {
          HANDLERS.get(i).accept(elements[i], payload);
        } catch (NumberFormatException e) {
          // Rethrow exception from Integer.parseInt in putBoolPerCode and putBoolPerCodeSet
          // Position is greater than index to account for uri prefix and 1-based indexing
          throw new InvalidIntegerException(i + 2, elements[i]);
        }
      }
      // Serialize new payload as json
      return payload.toString().getBytes();
    }
  }
}
