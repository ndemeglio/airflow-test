import json
import os
import unittest

from google.cloud import bigquery





class TestBigQuerySchema(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        SCHEMA_DIR = os.getenv('SCHEMA_DIR')
        # print('Schema_dir: {}'.format(SCHEMA_DIR))
        cls.EVENT_SCHEMA_FILE = os.path.join(
            SCHEMA_DIR,
            'event_schema.json'
        )
        # print(cls.EVENT_SCHEMA_FILE)
        cls.PEOPLE_SCHEMA_FILE = os.path.join(
            SCHEMA_DIR,
            'people_schema.json'
        )
        # print(cls.PEOPLE_SCHEMA_FILE)

    def setUp(self):
        # print("in setup -- {}".format(self.EVENT_SCHEMA_FILE))
        with open(self.EVENT_SCHEMA_FILE) as f:
            # j = ''.join(f.readlines())
            # print(j)
            # e_schema = json.loads(j)
            e_schema = json.load(f)
        self.event_schema = {'fields': e_schema}

        with open(self.PEOPLE_SCHEMA_FILE) as f:
            p_schema = json.load(f)
        self.people_schema = {'fields': p_schema}

    def test_parse_event_schema(self):
        bigquery.schema._parse_schema_resource(self.event_schema)

    def test_parse_people_schema(self):
        bigquery.schema._parse_schema_resource(self.people_schema)
