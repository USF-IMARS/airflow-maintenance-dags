from unittest import TestCase
from unittest.mock import patch
from unittest.mock import MagicMock
import importlib

# tested module(s):
from airflow_maintenance_dags.db_cleanup.airflow_db_cleanup import \
    _get_entries_to_delete


class Test_get_entries_to_delete(TestCase):

    # tests:
    #########################
    def test_calls_my_method(self):
        """ ExampleClass module calls my_method twice """
        # new return value for each call w/ side_effect:
        query = MagicMock()

        _get_entries_to_delete(
            query, "airflow_db_model", False, "age_check_column",
            "dag_id", "2019-07-29", ['dag_ignore_list']
        )
