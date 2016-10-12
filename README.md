# DO NOT USE THIS

Although it can work this was just a proof of concept that I wrote for a job interview.

# Cassandra <-> ElasticSearch Synchronization

- [Intro](#intro)
- [Installing](#installing)
- [Running](#running)
	- [Running tests](#running-tests)
- [Challenges](#challenges)
	- [Bidirectional Replication](#bidirectional-replication)
	- [Race conditions](#race-conditions)
		- [Cassandra](#cassandra)
		- [Elasticsearch](#elasticsearch)
	- [Parallelized / Distributed](#parallelized--distributed)
	- [Fault tolerance](#fault-tolerance)
	- [Full and Incremental Synchronization](#full-and-incremental-synchronization)
- [Implementation Details](#implementation-details)
	- [Full Sync](#full-sync)
	- [Partial Sync](#partial-sync)
	- [Better Partial Sync](#better-partial-sync)

# Intro

You have an Elasticsearch and a Cassandra cluster and you have to sync between them. The catch? It's bidirectional!
Writes can come at any time to Cassandra or Elasticsearch and eventually you want data to converge and be available in both databases.
All records have a **version** field which can be used to distinguish newer from older records.

To complicate things a little the toolset is restricted to the two databases and Python scripts, so no Kafka, Hadoop, Spark nor Storm...

# Installing

The source code is compatible with python 2.7+/3.3+.

The Dependencies are listed in the requirements.txt file.

Use pip to install the dependencies to your interpreter/virtual env.

```
pip install -r requirements.txt
```

# Running

Run with the following command:

```
./run.py config.yaml ACTION
OR
python run.py config.yaml ACTION
```

*ACTION* can be one of:

* **sync_forever**: repeat sync every {config.interval} seconds
* **sync_once**: sync once and exit
* **reset**: reset the incremental synchronization checkpoint

A sample config.yaml

```
interval: 60 # interval in seconds
id_field: id # name of the id field
version_field: version # name of the version field
docs_per_batch: 1000 # number of document per batch insert
sync_fields: # list of fields that will also be syncronized
  - "data_int"
  - "data_float"
  - "data_str"

cassandra:
  hosts:
    - 127.0.0.1
  keyspace: default_art
  table: t_data
  changes_table: c_data

elasticsearch:
  hosts:
    - 127.0.0.1
  index: i_data
  type: document
```

## Running tests

To run the test suite

```
./test.sh
```

# Challenges

## Bidirectional Replication

The bidirectional replication obliges the sync process to inspect both databases to distinguish the newer record (even the entire dataset if running a full sync).

This immediately excludes the naive approach of pulling all the data from both databases to memory (or disk) to figure out which inserts/update are needed.

Why? Both Cassandra and ElasticSearch are distributed and can easily exceed the memory/disk capacity of the worker machine. Even if run a distributed version of the sync program it's still not practical.

## Race conditions

Race conditions are a big problem in this case as writes are still going to both ends during the sync process. Any RMW (Read Modify Write) will be a problem.

Consider the example:

1. Worker reads record from Cassandra with version 19
2. Worker reads record from Elasticsearch with version 18
3. Worker figures it should write version 19 to Es as it's newer
4. A client writes version 20 to Es
5. Worker writes version 19 to Es and ***we lose version 20***

Each database has it's own mechanism to avoid race conditions:

### Cassandra

* Last Write Wins (LWW) based on the write timestamp
* Light Weight Transaction (LWT) in the form of CAS

### Elasticsearch

* Optimistic concurrency control in the form of version CAS

LWT are relatively expensive in Cassandra, so if you possibly have a timestamp in the record you may use it
as the write timestamp in Cassandra. The LWW mechanism will make race conditions highly unlikely.

## Parallelized / Distributed

Prallelization is needed otherwise syncing anything over a multi gigabyte cluster will take a looonnnggg time.
It's relatively easy to parallelize a scan procedure on both databases by:

* For Cassandra each worker can scan different parts of the partitioner ring in different machines/processes.
* For ElasticSearch each worker can scan different ranges of ids in different machines/processes

## Fault tolerance

If a part of the sync process fails one must be able to resume somewhere but not from the start.

## Full and Incremental Synchronization

Full synchronization is necessary for the first run and from time to time to ensure both databases are in full sync.
Elasticsearch is known to lose writes after healing partitions for example.

Incremental synchronization is the prefered way to run continuously as it only considers the changes since last sync thus putting less stress on the clusters.

# Implementation Details

In this implementation I assumed all versions of the record carry a timestamp along it
(it can be the version field itself or another field as longs as if version increases the timestamp shall increase as well)
which is reasonable and will make things a lot simpler/faster. ***If it's not the case the only change required is using a
Cassandra LWT with the version field.***

*For the sake of simplicity the implementation is also not distributed.*

## Full Sync

The script runs a full table scan on Cassandra (done cheaply with cassandra 2.0+ pagination) and then send bulk writes to Elasticsearch using the external version flag.
This way the record will only be inserted if it doesn't exists or it's newer. Conciliating on write like this is MUCH faster than querying for the
records versions in Elasticsearch and then deciding to insert or not.

Syncing from Elasticsearch to Cassandra similar, the worker issue a match_all filter and then scroll (using the scan type) through all the records, issuing
batch writes to Cassandra. Each insert in the batch is done with the record timestamp to avoid race conditions (as stated previously LWT would also work, just slower).
The advantages for conciliating on write are the same as before.

## Partial Sync

Although trivial in Elasticsearch you can't easily run a range query on a non-primary key field in Cassandra, so you can't get all records with version/timestamp greater than X. Secondary indexes are of no use in this case nor ALLOW FILTERING.

The current implementation runs a full table scan in Cassandra filtering the records in the worker (thus very expensive).

## Better Partial Sync

We ned an alternative to query changed records in Cassandra. One option is when inserting/updating a record, insert it into it's table AND into a changes table.

Queue like workloads and hotpots are a common pitfall in Cassandra, so this must be done with caution.

Consider the following changes table:

```
CREATE TABLE t_data_changes (
  time_shard int,
  cluster_shard int,
  timestamp bigint,
  id uuid,
  version int,
  PRIMARY KEY ((time_shard, cluster_shard), timestamp)
);
```

* **time_shard**: distribute the writes based on a time portion. Example: day of the week
* **cluster_shard**: distribute the writes from the same time portion to different parts of the cluster. Example: random(0, 4096)
* **timestamp**: as the name suggests the change timestamp, used to efficiently select changes since last sync checkpoint

Carefully selecting time_shard and cluster_shard ranges allows a good distribution of the load in the cluster avoiding both hotspots and very wide rows.
Writing with a short enough TTL avoids the need for deleting older changes.

Concurrency in this case is achieved by splitting the cluster_shard range among the workers.
