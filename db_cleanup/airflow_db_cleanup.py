"""
A maintenance workflow that you can deploy into Airflow to periodically clean
out the DagRun, TaskInstance, Log, XCom, Job DB and SlaMiss entries to avoid
having too much data in your Airflow MetaStore.

examples:
----------
airflow trigger_dag --conf '{"maxDBEntryAgeInDays":30,"dagIgnoreList":""}' \
    airflow-db-cleanup
airflow trigger_dag \
    --conf '{"dagIgnoreList":"my_special_dag,my_other_special_dag"}'
    airflow-db-cleanup
airflow trigger_dag --conf '{"maxDBEntryAgeInDays":7}' airflow-db-cleanup

--conf options:
---------------
    maxDBEntryAgeInDays:<INT> - Optional
    dagIgnoreList:<String> - Optional

"""
from airflow.models import DAG, DagRun, TaskInstance, Log, XCom, SlaMiss, \
    DagModel, Variable
from airflow.jobs import BaseJob
try:  # airflow ~1.10.4+
    from airflow import settings
except ImportError:  # earlier airflow versions
    from airflow.models import settings
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import logging

import dateutil.parser
from sqlalchemy import func, and_, not_, or_
from sqlalchemy.orm import load_only

try:
    # airflow.utils.timezone is available from v1.10 onwards
    from airflow.utils import timezone
    now = timezone.utcnow
except ImportError:
    now = datetime.utcnow

# DAG_ID should evaluate to `airflow-db-cleanup`
DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
START_DATE = datetime(year=1970, month=1, day=1)

# How often to Run. @daily - Once a day at Midnight (UTC)
SCHEDULE_INTERVAL = "@daily"
# Who is listed as the owner of this DAG in the Airflow Web Server
DAG_OWNER_NAME = "operations"
# List of email address to send email alerts to if this job fails
ALERT_EMAIL_ADDRESSES = []
# Length to retain the log files if not already provided in the conf.
# If this is set to 30, the job will remove those files that are 30 days
# old or older.
DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS = int(Variable.get(
    "max_db_entry_age_in_days", 30)
)
# comma-separated list of dag_id SQL LIKE matches to *not* clear
DEFAULT_DAG_IGNORE_LIST = str(Variable.get("dag_ignore_list", ""))
# Whether the job should delete the db entries or not.
# Included if you want to temporarily avoid deleting the db entries.
ENABLE_DELETE = True
# List of all the objects that will be deleted.
# Comment out any DB objects you want to skip.
DATABASE_OBJECTS = [
    {
        "airflow_db_model": DagRun,
        "age_check_column": DagRun.execution_date,
        "dag_id": DagRun.dag_id,
        "keep_last_run": True
     },
    {
        "airflow_db_model": TaskInstance,
        "age_check_column": TaskInstance.execution_date,
        "dag_id": TaskInstance.dag_id,
        "keep_last_run": False
     },
    {
        "airflow_db_model": Log,
        "age_check_column": Log.dttm,
        "dag_id": Log.dag_id,
        "keep_last_run": False
    },
    {
        "airflow_db_model": XCom,
        "age_check_column": XCom.execution_date,
        "dag_id": XCom.dag_id,
        "keep_last_run": False
    },
    {
        "airflow_db_model": BaseJob,
        "age_check_column": BaseJob.latest_heartbeat,
        "dag_id": BaseJob.dag_id,
        "keep_last_run": False
    },
    {
        "airflow_db_model": SlaMiss,
        "age_check_column": SlaMiss.execution_date,
        "dag_id": SlaMiss.dag_id,
        "keep_last_run": False
    },
    {
        "airflow_db_model": DagModel,
        "age_check_column": DagModel.last_scheduler_run,
        "dag_id": DagModel.dag_id,
        "keep_last_run": False
    },
]

session = settings.Session()

default_args = {
    'owner': DAG_OWNER_NAME,
    'email': ALERT_EMAIL_ADDRESSES,
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': START_DATE,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=START_DATE,
    catchup=False
)
dag.doc_md = __doc__


def print_configuration_function(**context):
    logging.info("Loading Configurations...")
    dag_run_conf = context.get("dag_run").conf
    logging.info("dag_run.conf: " + str(dag_run_conf))
    max_db_entry_age_in_days = None
    dag_ignore_list = None
    if dag_run_conf:
        max_db_entry_age_in_days = dag_run_conf.get(
            "maxDBEntryAgeInDays",
            None
        )
        dag_ignore_list = dag_run_conf.get("dagIgnoreList", None)
    logging.info("using dag_run.conf: " + str(dag_run_conf))
    if max_db_entry_age_in_days is None:
        logging.info(
            "maxDBEntryAgeInDays conf variable isn't included. " +
            "Using Default '" +
            str(DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS) + "'"
        )
        max_db_entry_age_in_days = DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS
    if dag_ignore_list is None:
        logging.info(
            "dagIgnoreList conf variable isn't included. Using Default '" +
            str(DEFAULT_DAG_IGNORE_LIST) + "'"
        )
        dag_ignore_list = DEFAULT_DAG_IGNORE_LIST

    max_date = now() + timedelta(-max_db_entry_age_in_days)
    logging.info("Finished Loading Configurations")
    logging.info("")

    logging.info("Configurations:")
    logging.info("max_db_entry_age_in_days: " + str(max_db_entry_age_in_days))
    logging.info("max_date:                 " + str(max_date))
    logging.info("enable_delete:            " + str(ENABLE_DELETE))
    logging.info("session:                  " + str(session))
    logging.info("dag_ignore_list:          " + str(dag_ignore_list))
    logging.info("")

    logging.info(
        "Setting max_execution_date and dag_ignore_list to XCom for "
        "Downstream Processes"
    )
    context["ti"].xcom_push(key="max_date", value=max_date.isoformat())
    context["ti"].xcom_push(key="dag_ignore_list", value=dag_ignore_list)

print_configuration = PythonOperator(
    task_id='print_configuration',
    python_callable=print_configuration_function,
    provide_context=True,
    dag=dag)


def _get_entries_to_delete(
    query, airflow_db_model, keep_last_run, age_check_column, dag_id,
    max_date, dag_ignore_list
):
    if keep_last_run:
        # workaround for MySQL "table specified twice" issue
        # https://github.com/teamclairvoyant/airflow-maintenance-dags/issues/41
        sub_query = session.query(
            func.max(age_check_column)
        ).group_by(dag_id).from_self()
        query = query.filter(
            age_check_column.notin_(sub_query),
            and_(age_check_column <= max_date),
            and_(not_(or_(*[dag_id.like(d) for d in dag_ignore_list]))),
        )
    else:
        query = query.filter(
            age_check_column <= max_date,
            and_(not_(or_(*[dag_id.like(d) for d in dag_ignore_list]))),
        )
    return query.all()


def cleanup_function(**context):

    logging.info("Retrieving max_execution_date from XCom")
    max_date = context["ti"].xcom_pull(
        task_ids=print_configuration.task_id, key="max_date"
    )
    max_date = dateutil.parser.parse(max_date)  # stored as iso8601 str in xcom

    logging.info("Retrieving dag_ignore_list from XCom")
    dag_ignore_list = context["ti"].xcom_pull(
        task_ids=print_configuration.task_id, key="dag_ignore_list"
    )
    # transform string from xcom to list
    dag_ignore_list = [s.strip() for s in dag_ignore_list.split(',')]

    airflow_db_model = context["params"].get("airflow_db_model")
    age_check_column = context["params"].get("age_check_column")
    keep_last_run = context["params"].get("keep_last_run")
    dag_id = context["params"].get("dag_id")

    logging.info("Configurations:")
    logging.info("max_date:                 " + str(max_date))
    logging.info("enable_delete:            " + str(ENABLE_DELETE))
    logging.info("session:                  " + str(session))
    logging.info("airflow_db_model:         " + str(airflow_db_model))
    logging.info("age_check_column:         " + str(age_check_column))
    logging.info("keep_last_run:            " + str(keep_last_run))
    logging.info("dag_id:                   " + str(dag_id))
    logging.info("dag_ignore_list:          " + str(dag_ignore_list))
    logging.info("")

    logging.info("Running Cleanup Process...")
    query = session.query(airflow_db_model).options(
        load_only(age_check_column)
    )
    entries_to_delete = _get_entries_to_delete(
        query, airflow_db_model, keep_last_run, age_check_column, dag_id,
        max_date, dag_ignore_list
    )

    logging.info("Query : " + str(query))
    logging.info(
        "Process will be Deleting the following " +
        str(airflow_db_model.__name__) + "(s):"
    )
    for entry in entries_to_delete:
        logging.info(
            "\tEntry: " + str(entry) +
            ", Date: " + str(
                entry.__dict__[str(age_check_column).split(".")[1]]
            )
        )

    logging.info(
        "Process will be Deleting " + str(len(entries_to_delete)) + " " +
        str(airflow_db_model.__name__) + "(s)"
    )

    if ENABLE_DELETE:
        logging.info("Performing Delete...")
        # using bulk delete
        query.delete(synchronize_session=False)
        session.commit()
        logging.info("Finished Performing Delete")
    else:
        logging.warn("You're opted to skip deleting the db entries!!!")

    logging.info("Finished Running Cleanup Process")

for db_object in DATABASE_OBJECTS:

    cleanup_op = PythonOperator(
        task_id='cleanup_' + str(db_object["airflow_db_model"].__name__),
        python_callable=cleanup_function,
        params=db_object,
        provide_context=True,
        dag=dag
    )

    print_configuration.set_downstream(cleanup_op)
