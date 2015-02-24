import logging
import uuid
import time
import six

from elasticsearch import Elasticsearch, JSONSerializer as EsJSONSerializer, Transport as EsTransport
from elasticsearch.helpers import bulk as es_bulk, scan as es_scan
from cassandra.cluster import Cluster as CaCluster
from cassandra.query import BatchType, BatchStatement, dict_factory

logger = logging.getLogger("synchronizer")


class Synchronizer(object):
    CHECKPOINT_FILENAME = "checkpoint.txt"

    def __init__(self, config_d):
        self.id_field = config_d["id_field"]
        self.version_field = config_d["version_field"]
        self.sync_fields = config_d["sync_fields"]
        self.batch_size = config_d["docs_per_batch"]

        self.interval = config_d["interval"]

        self.ca_table = config_d["cassandra"]["table"]
        self.ca_changes_table = config_d["cassandra"]["changes_table"]
        self.es_index = config_d["elasticsearch"]["index"]
        self.es_type = config_d["elasticsearch"]["type"]

        self.ca_cluster = CaCluster(config_d["cassandra"]["hosts"])
        self.ca_session = self.ca_cluster.connect(config_d["cassandra"]["keyspace"])
        # set row factory to dicts for easier interaction to ES
        self.ca_session.row_factory = dict_factory
        # patch cql types
        patch_cql_types()

        # init ES with a custom transport/encoder that serializes UUID instances
        self.es_session = Elasticsearch(config_d["elasticsearch"]["hosts"], ESTransportEx)

        self._prepare_ca_queries()

    def _prepare_ca_queries(self):
        fields = [self.id_field, self.version_field] + self.sync_fields

        self.ca_ps_select_all = self.ca_session.prepare(
            "SELECT %s FROM %s;" % (",".join(fields), self.ca_table)
        )

        wildcards = [":%s" % field for field in fields]
        self.ca_ps_upsert_with_ts = self.ca_session.prepare(
            "INSERT INTO %s (%s) VALUES (%s) USING TIMESTAMP :timestamp_;" % (
                self.ca_table, ",".join(fields), ",".join(wildcards))
        )

    def make_es_filter_all(self):
        return {
            "_source": [self.id_field, self.version_field] + self.sync_fields,
            "version": True,
            "filter": {"match_all": {}}
        }

    def make_es_filter_version_range(self, a, b):
        return {
            "filter": {
                "range": {
                    self.version_field: {
                        "gte": a,
                        "lte": b
                    }
                }
            }
        }

    def es_bulk_insert_versioned(self, docs):
        """"
        Insert docs in the corresponding ElasticSearch index using the bulk method
        :param docs: list of dicts representing documents
        :return: tuple of (num successful, num failed or up to date) writes
        """
        return es_bulk(
            self.es_session,
            (
                {
                    "_index": self.es_index,
                    "_type": self.es_type,
                    "_id": doc["id"],
                    "_version": doc["version"],
                    "_version_type": "external",
                    "_source": doc
                }
                for doc in docs
            ),
            stats_only=True
        )

    def ca_batch_insert_with_ts(self, docs):
        """
        Insert docs in the corresponding Cassandra table using the batch inserts
        :param docs: list of dicts representing documents
        :return: tuple of (num successful or up to date, num errors) writes
        """
        batch = BatchStatement(BatchType.LOGGED)
        for doc in docs:
            doc = doc.copy()
            doc["timestamp_"] = doc["version"] * 1e6  # s to us, assuming version is a unix timestamp (seconds)
            batch.add(self.ca_ps_upsert_with_ts, doc)

        try:
            self.ca_session.execute(batch)
            # logged batch writes are atomic, so it all succeeds or fails
            return len(docs), 0
        except Exception:  # FIXME: catch only specific Cassandra errors
            return 0, len(docs)

    def checkpoint_reset(self):
        """
        Reset the checkpoint for this worker
        """
        self.checkpoint_save(0)

    def checkpoint_load(self):
        """
        Returns the last sync timestamp for this worker
        """
        try:
            with open(self.CHECKPOINT_FILENAME, "r") as f:
                checkpoint = int(f.read())
        except IOError:
            checkpoint = 0

        logger.info("Loaded checkpoint %d", checkpoint)

        return checkpoint

    def checkpoint_save(self, checkpoint):
        """
        Save the checkpoint for this worker
        """
        # FIXME: in the real world save to etcd, zookeper or another reliable storage.
        with open(self.CHECKPOINT_FILENAME, "w") as f:
            f.write(str(checkpoint))

        logger.info("Saving checkpoint %d", checkpoint)

    def sync(self):
        """
        Syncs ElasticSearch and Cassandra in both directions.

        It uses each database conflict resolution properties to avoid race conditions
        with concurrent writes running during the sync. This is:
            * Cassandra LWW (last write wins)
            * ElasticSearch doc versions

        The function also prefers to let the other Database decides when to insert/replace the document or not
        using the same semantics as above. This is MUCH faster than querying for the records and then decide to
        send insert/updates or do nothing.
        The drawback is that in C* it's impossible to distinguish if the doc was updated or not.
        """
        # TODO: paralyze C* scanning by going through different parts of the ring in different machines/processes
        # TODO: paralyze ES scanning by scanning different ranges of ids in different machines/processes

        last_checkpoint = self.checkpoint_load()
        next_checkpoint = int(time.time())

        # sync from ES to C*
        docs = []
        if not last_checkpoint:
            es_cursor = es_scan(self.es_session, index=self.es_index, doc_type=self.es_type,
                                query=self.make_es_filter_all())
        else:
            es_cursor = es_scan(self.es_session, index=self.es_index, doc_type=self.es_type,
                                query=self.make_es_filter_version_range(last_checkpoint, next_checkpoint))
        for hit in es_cursor:
            docs.append(hit["_source"])
            if len(docs) >= self.batch_size:
                successful, failed = self.ca_batch_insert_with_ts(docs)
                logger.info("ElasticSearch -> Cassandra: %d successful or up to date, %d failed", successful, failed)
                docs = []
        if docs:
            successful, failed = self.ca_batch_insert_with_ts(docs)
            logger.info("ElasticSearch -> Cassandra: %d successful or up to date, %d failed", successful, failed)

        # sync from C* to ES
        docs = []
        ca_cursor = self.ca_session.execute(self.ca_ps_select_all)
        for doc in ca_cursor:
            if last_checkpoint and (doc["version"] < last_checkpoint or doc["version"] > next_checkpoint):
                continue
            docs.append(doc)
            if len(docs) >= self.batch_size:
                successful, failed = self.es_bulk_insert_versioned(docs)
                logger.info("Cassandra -> ElasticSearch: %d successful, %d failed or up to date", successful, failed)
                docs = []
        if docs:
            successful, failed = self.es_bulk_insert_versioned(docs)
            logger.info("Cassandra -> ElasticSearch: %d successful, %d failed or up to date", successful, failed)

        self.checkpoint_save(next_checkpoint)

    def sync_incremental(self):
        pass

    def run_once(self):
        logger.info("Starting sync job")
        start = time.time()
        self.sync()
        end = time.time()
        took = end - start
        logger.info("Took %f seconds to sync", took)

    def run_forever(self):
        while True:
            self.run_once()
            if self.interval >= 0:
                logger.info("Resting for %f seconds", self.interval)
                time.sleep(self.interval)


def patch_cql_types():
    """
    Monkey patch CQL uuid serializer to accept hex strings as well.
    Despite the driver allowing to customize the serializer for non-prepared statements it doesn't provide a way
    to do the same for prepared-statements bindings.
    """
    from cassandra.cqltypes import UUIDType

    saved_serialize = UUIDType.serialize

    @staticmethod
    def serialize_ex(obj, protocol_version):
        if isinstance(obj, six.string_types):
            obj = uuid.UUID(hex=obj)
        return saved_serialize(obj, protocol_version)

    UUIDType.serialize = serialize_ex


class ESTransportEx(EsTransport):
    def __init__(self, *args, **kwargs):
        kwargs['serializer'] = ESJsonSerializerEx()
        super(ESTransportEx, self).__init__(*args, **kwargs)


class ESJsonSerializerEx(EsJSONSerializer):
    def default(self, data):
        if isinstance(data, uuid.UUID):
            return str(data)
        return super(ESJsonSerializerEx, self).default(data)
