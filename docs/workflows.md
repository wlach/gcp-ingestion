# Workflows

This document describes how various tasks are accomplished under GCP Ingestion.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->


- [Ingesting a new data source](#ingesting-a-new-data-source)
- [Operational visibility](#operational-visibility)
- [Building a derived dataset](#building-a-derived-dataset)
- [Backfilling a dataset](#backfilling-a-dataset)
- [Building a streaming job](#building-a-streaming-job)
- [Ad-hoc data analysis](#ad-hoc-data-analysis)
- [Self-serve querying](#self-serve-querying)
- [Building a dashboard / bespoke visualization](#building-a-dashboard--bespoke-visualization)
- [Deploying a change](#deploying-a-change)
- [Managing metadata](#managing-metadata)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Ingesting a new data source

## Operational visibility

Notes from 2018/06/14

PubSub metrics logged to Stackdriver automatically. Support for alerting
policies, etc.

Kubernetes - write to stdout and it can be accessed, alerted on etc in
Stackdriver

Logs are searchable (via 2 different UIs)

Latency for Stackdriver graphs about 1 min

What kind of overhead for the Stackdriver monitoring agent? May not even need
an agent.  Some internal VMs run it by default and resource usage is minimal

BigQuery has a bunch of built in monitoring - concurrent

## Building a derived dataset

Derived datasets are easily created from SQL queries using BigQuery destination
tables. For more complicated transformations Dataflow jobs have good support
for reading and writing to all of our potential data sources.

## Backfilling a dataset

Landfill stores messages in Cloud Storage the same format as PubSub, so that
Dataflow jobs consuming PubSub can be simply modified (or accept parameters) to
backfill from there.

For derived datasets using SQL and destination tables, if a single partition
date is processed by the SQL query then a destination table can include the
partition table in the table name spec to and use overwrite mode to atomically
overwrite a single day at a time.

For other cases it should only be a matter of re-running a job in batch mode
for a time period of data.

## Building a streaming job

Dataflow is the preferred method for running streaming jobs. Dataflow jobs can
be written using the apache beam API available in Go, Python, and Java (and
Scala via Java or https://github.com/spotify/scio). Jobs written in Java (or
Scala via Java) are preferable because they support autoscaling

## Ad-hoc data analysis

## Self-serve querying

## Building a dashboard / bespoke visualization

## Deploying a change

## Managing metadata

Avro schemas - keep them in your beam job

