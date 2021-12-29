from datetime import datetime, timedelta
import unittest

from airflow.models import DAG, TaskInstance
from airflow.operators.dummy_operator import DummyOperator
from pyjsparser import PyJsParser

import mixpanel.dags.templates as mdt


USER_DEFINED_FILTERS = {
    'start': lambda dt: 1,  # takes datetime, returns int
    'end': lambda dt: 2 # takes datetime, returns int
}

# '2018-12-10 00:00:28.123456'
EXECUTION_DATE = datetime(2018, 12, 10, 0, 0, 28, 123456)


TEMPLATE_SEARCH_DIR = mdt.__path__


class TestMixpanelTemplates(unittest.TestCase):

    def _get_rendered_template(self, template_file):
        rendered_template = self.d_op.render_template(
            'test_attr',  # class attribute, can be arbitrary in this context
            template_file,
            self.ti.get_template_context()
        )
        return rendered_template

    def setUp(self):
        """
        Instantiate timestamp, DAG, Dummy task, and task instance
        """

        self.p = PyJsParser()

        # get an absolute path to template file directory, following
        # links (if any)

        self.yesterday = datetime.today() - timedelta(days=1)
        self.dag = DAG(
            dag_id='testing_dag',
            start_date=self.yesterday,
            user_defined_filters=USER_DEFINED_FILTERS,
            template_searchpath=TEMPLATE_SEARCH_DIR
        )
        self.d_op = DummyOperator(
            task_id='dummy_task',
            dag=self.dag
        )
        # Necessary to load .js files into Jinja Env
        # instance loads extensions from class definition
        DummyOperator.template_ext = ['js', 'json']

        self.ti = TaskInstance(
            task=self.d_op,
            execution_date=EXECUTION_DATE
        )

    def test_parse_event_interval_query(self):
        rendered_jql = self._get_rendered_template('event_query_template.js')
        self.p.parse(rendered_jql)

    def test_exists_event_interval_query(self):
        self.assertIsNotNone(
            self._get_rendered_template('event_query_template.js'),
            "Event interval query not provided."
        )

    def test_non_empty_string_event_query(self):
        self.assertIsNot(
            self._get_rendered_template('event_query_template.js'),
            '',
            'Event interval query is empty!'
        )

    def test_event_interval_query_runnable(self):
        rendered_jql = self._get_rendered_template('event_query_template.js')
        parsed_jql = self.p.parse(rendered_jql)
        self.assertEqual(
            parsed_jql.get('type', '"type" key not found'),
            'Program'  # provided by pyjsparser
        )

    def test_parse_people_interval_query(self):
        rendered_jql = self._get_rendered_template('people_query_template.js')
        self.p.parse(rendered_jql)

    def test_exists_people_interval_query(self):
        self.assertIsNotNone(
            self._get_rendered_template('people_query_template.js'),
            "People interval query not provided."
        )

    def test_non_empty_string_people_query(self):
        self.assertIsNot(
            self._get_rendered_template('people_query_template.js'),
            '',
            'People interval query is empty!'
        )

    def test_people_interval_query_runnable(self):
        rendered_jql = self._get_rendered_template('people_query_template.js')
        parsed_jql = self.p.parse(rendered_jql)
        self.assertEqual(
            parsed_jql.get('type', '"type" key not found'),
            'Program'  # provided by pyjsparser
        )
