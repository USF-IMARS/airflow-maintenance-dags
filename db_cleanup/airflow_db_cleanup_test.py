from unittest import TestCase
from unittest import mock

from sqlalchemy.orm import load_only
from alchemy_mock.mocking import UnifiedAlchemyMagicMock
from airflow.models import DagRun, DAG


class Test_get_entries_to_delete(TestCase):

    # tests:
    #########################
    def test_calls_my_method(self):
        """ ExampleClass module calls my_method twice """
        # with \
        #         patch('sqlalchemy.or_') as m_or_,\
        #         patch('sqlalchemy.and_') as m_and_,\
        #         patch('sqlalchemy.func') as m_func,\
        #         patch('sqlalchemy.not_') as m_not_:
        from airflow_maintenance_dags.db_cleanup.airflow_db_cleanup \
            import _get_entries_to_delete

        # mock the data:
        # TODO: this is non-functional example copied from
        # https://pypi.org/project/alchemy-mock/
        session = UnifiedAlchemyMagicMock(data=[
            (
                [
                    mock.call.query(DagRun),
                    mock.call.filter(DagRun.foo == 5, DagRun.bar > 10)
                ],
                [DagRun(foo=5, bar=11)]
            ),
            (
                [
                    mock.call.query(DagRun),
                    mock.call.filter(DagRun.note == 'hello world')
                ],
                [DagRun(note='hello world')]
            ),
            (
                [
                    mock.call.query(DAG),
                    mock.call.filter(DagRun.foo == 5, DagRun.bar > 10)
                ],
                [DAG(foo=5, bar=17)]
            ),
        ])
        query = session.query(DagRun).options(
            load_only(DagRun.execution_date)
        )

        result = _get_entries_to_delete(
            query, DagRun, False, DagRun.execution_date,
            DagRun.dag_id, "2019-07-29", ['dag_ignore_list']
        )
        import pdb; pdb.set_trace()
        self.assertEqual(query, False)
