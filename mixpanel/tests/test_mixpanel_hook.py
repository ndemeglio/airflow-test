import unittest

from unittest.mock import patch

from airflow import configuration
from airflow import models
from airflow.utils import db

from mixpanel.plugins.mixpanel_plugin import MixpanelHook

METHOD = 'method'
HEADER = 'header'

VALID_HEADER = {'Content-Type': 'application/x-www-form-urlencoded'}

JQL_ENDPOINT = 'jql'
JQL_METHOD = 'POST'
JQL_HEADER = VALID_HEADER


VALUES_BY_ENDPOINT_MOCK = {
    JQL_ENDPOINT: {
        HEADER: JQL_HEADER,
        METHOD: JQL_METHOD
    }
}

mixpanel_connection = models.Connection(
    conn_id="mixpanel",
    conn_type="http",
    host='https://mixpanel.com',
    login='api_key'
)


class TestMixpanelHook(unittest.TestCase):

    def setUp(self):
        configuration.load_test_config()
        db.merge_conn(
            mixpanel_connection
        )

    def test_mixpanel_connection(self):
        mph = MixpanelHook()
        connection = mph.get_connection('mixpanel')
        self.assertEqual(connection.conn_id, mixpanel_connection.conn_id)

    @patch('mixpanel.plugins.mixpanel_plugin.MixpanelHook.values_by_endpoint', VALUES_BY_ENDPOINT_MOCK)
    def test_default_arguments_set_header(self):
        mph = MixpanelHook()
        self.assertIsNotNone(mph.headers)

    @patch('mixpanel.plugins.mixpanel_plugin.MixpanelHook.values_by_endpoint',
           VALUES_BY_ENDPOINT_MOCK)
    def test_default_arguments_set_method(self):
        mph = MixpanelHook()
        self.assertIn(mph.method, ['POST', 'GET'])

    @patch('mixpanel.plugins.mixpanel_plugin.MixpanelHook.values_by_endpoint'
           '', VALUES_BY_ENDPOINT_MOCK)
    def test_arguments_set_header(self):
        mph = MixpanelHook(endpoint=JQL_ENDPOINT)
        self.assertEqual(mph.headers, JQL_HEADER)

    @patch('mixpanel.plugins.mixpanel_plugin.MixpanelHook.values_by_endpoint',
           VALUES_BY_ENDPOINT_MOCK)
    def test_arguments_set_method(self):
        mph = MixpanelHook(endpoint=JQL_ENDPOINT)
        self.assertEqual(mph.method, JQL_METHOD)

    @patch('mixpanel.plugins.mixpanel_plugin.MixpanelHook.values_by_endpoint',
           VALUES_BY_ENDPOINT_MOCK)
    def test_invalid_endpoint_raises_error(self):
        with self.assertRaises(NotImplementedError):
            mph = MixpanelHook(endpoint='no_such_endpoint')
