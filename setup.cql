DROP KEYSPACE default_art;

CREATE KEYSPACE "default_art" WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor' : 1} ;

USE default_art;

CREATE TABLE t_data (
  id uuid PRIMARY KEY,
  version int,
  data_int int,
  data_str text,
  data_float float
);

CREATE TABLE t_data_changes (
  time_shard int,
  cluster_shard int,
  timestamp bigint,
  id uuid,
  version int,
  PRIMARY KEY ((time_shard, cluster_shard), timestamp)
);
