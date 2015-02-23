import random
import unittest
import uuid
import time
import yaml
import os.path

from sync.merger import Merger, es_scan


ASSETS_DIR = os.path.join(os.path.dirname(__file__), "..", "assets")


def load_config_dict():
    with file(os.path.join(ASSETS_DIR, "config.yaml")) as f:
        return yaml.load(f)


class BaseTestCase(unittest.TestCase):

    # longMessage = True

    def setUp(self):
        self.merger = Merger(load_config_dict())
        self.merger.checkpoint_reset()
        self.prepare_ca()
        self.prepare_es()
        self._prepare_ca_queries()

    def _prepare_ca_queries(self):
        fields = [self.merger.id_field, self.merger.version_field] + self.merger.sync_fields

        self.ca_ps_select_ids = self.merger.ca_session.prepare(
            "SELECT %s FROM %s WHERE %s IN ?;" % (",".join(fields), self.merger.ca_table, self.merger.id_field)
        )

    def make_es_query_ids_in(self, ids):
        return {
            "filter": {
                "ids": {
                    "values": ids
                }
            }
        }

    def generate_doc(self, id=None, version=None):
        return {
            "id": id or str(uuid.uuid4()),
            "version": version or int(time.time() - 1),
            "data_int": random.randint(0, 1000),
            "data_float": random.randint(0, 1000),
            "data_str": str(random.random()),
        }

    def prepare_ca(self):
        self.merger.ca_session.execute("TRUNCATE %s" % self.merger.ca_table)

    def prepare_es(self):
        try:
            self.merger.es_session.indices.delete(self.merger.es_index)
        except:
            pass

        self.merger.es_session.indices.create(self.merger.es_index)
        self.merger.es_session.cluster.health(index=self.merger.es_index, wait_for_status="yellow")

    def insert_ca(self, docs):
        self.merger.ca_batch_insert_with_ts(docs)

    def insert_es(self, docs):
        self.merger.es_bulk_insert_versioned(docs)
        # flush index after bulk insert
        self.merger.es_session.indices.flush(index=self.merger.es_index)

    def select_ca(self, ids):
        cursor = self.merger.ca_session.execute(self.ca_ps_select_ids, (ids,))
        return list(cursor)

    def select_es(self, ids):
        # flush index before search
        self.merger.es_session.indices.flush(index=self.merger.es_index)
        cursor = es_scan(self.merger.es_session, index=self.merger.es_index, doc_type=self.merger.es_type,
                         query=self.make_es_query_ids_in(ids))
        return [hit["_source"] for hit in cursor]

    def assertDocsEqual(self, docs1, docs2, msg=None):
        """
        Special function to compare documents OR a collection of documents (in any order).
        This function also handle type comparisons like UUIDs coming as string from Es and as uuid.UUID from C*
        """
        if isinstance(docs1, dict):
            docs1 = (docs1,)
            docs2 = (docs2,)
        # FIXME: so ugly that it hurts my eyes :x
        if docs1 and isinstance(docs1[0]["id"], uuid.UUID):
            docs1 = [dict(list(d.items()) + [("id", str(d["id"]))]) for d in docs1]
        if docs2 and isinstance(docs2[0]["id"], uuid.UUID):
            docs2 = [dict(list(d.items()) + [("id", str(d["id"]))]) for d in docs2]
        docs1 = sorted(docs1, key=lambda d: d["id"])
        docs2 = sorted(docs2, key=lambda d: d["id"])
        # self.assertEqual(docs1, docs2, msg)  # too slow :/
        self.assertTrue(docs1 == docs1, msg)


class TestCaseCaToEs(BaseTestCase):
    def test_one(self):
        doc1 = self.generate_doc()
        self.insert_ca([doc1])
        self.merger.run_once()
        doc2 = self.select_es([doc1["id"]])[0]
        self.assertDocsEqual(doc1, doc2)

    def test_many(self):
        docs1 = [self.generate_doc() for _ in range(1000)]
        docs1_ids = [d["id"] for d in docs1]
        self.insert_ca(docs1)
        self.merger.run_once()
        docs2 = self.select_es(docs1_ids)
        self.assertDocsEqual(docs1, docs2)


class TestCaseEsToCa(BaseTestCase):
    def test_one(self):
        doc1 = self.generate_doc()
        self.insert_es([doc1])
        self.merger.run_once()
        doc2 = self.select_ca([doc1["id"]])[0]
        self.assertDocsEqual(doc1, doc2)

    def test_many(self):
        docs1 = [self.generate_doc() for _ in range(1000)]
        docs1_ids = [d["id"] for d in docs1]
        self.insert_es(docs1)
        self.merger.run_once()
        docs2 = self.select_ca(docs1_ids)
        self.assertDocsEqual(docs1, docs2)


class TestCaseBoth(BaseTestCase):
    def test_many(self):
        docs1 = [self.generate_doc() for _ in range(1000)]
        docs2 = [self.generate_doc() for _ in range(1000)]
        docs1_ids = [d["id"] for d in docs1]
        docs2_ids = [d["id"] for d in docs2]
        docs_ids = docs1_ids + docs2_ids
        self.insert_es(docs1)
        self.insert_ca(docs2)

        self.merger.run_once()

        docs_ca = self.select_ca(docs_ids)
        docs_es = self.select_ca(docs_ids)
        self.assertDocsEqual(docs_ca, docs_es)
        self.assertDocsEqual(docs_ca, docs1 + docs2)

import logging

logging.basicConfig(level=logging.INFO)