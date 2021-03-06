# airflow-maintenance-dags
A series of DAGs/Workflows to help maintain the operation of Airflow

## DAGs/Workflows

* clear-missing-dags
    * A maintenance workflow that you can deploy into Airflow to periodically clean out entries in the DAG table of which there is no longer a corresponding Python File for it. This ensures that the DAG table doesn't have needless items in it and that the Airflow Web Server displays only those available DAGs.  
* db-cleanup
    * A maintenance workflow that you can deploy into Airflow to periodically clean out the DagRun, TaskInstance, Log, XCom, Job DB and SlaMiss entries to avoid having too much data in your Airflow MetaStore.
* kill-halted-tasks
    * A maintenance workflow that you can deploy into Airflow to periodically kill off tasks that are running in the background that don't correspond to a running task in the DB.
    * This is useful because when you kill off a DAG Run or Task through the Airflow Web Server, the task still runs in the background on one of the executors until the task is complete.
* log-cleanup
    * A maintenance workflow that you can deploy into Airflow to periodically clean out the task logs to avoid those getting too big.
* delete-broken-dags
    * A maintenance workflow that you can deploy into Airflow to periodically delete DAG files and clean out entries in the ImportError table for DAGs which Airflow cannot parse or import properly. This ensures that the ImportError table is cleaned every day.


## Configuration
Configuration options can be passed into these dags if triggered using the airflow CLI.
Examples:

```bash
airflow trigger_dag --conf '{"maxDBEntryAgeInDays":30,"dagIgnoreList":""}' airflow-db-cleanup
```

Alternatively, these dags can be configured by setting variables within your airflow environment.
Please see docstrings in the dags for detailed variable information.
Some examples of setting variables using the airflow CLI:
```bash
airflow variables --set dag_ignore_list "my_special_dag,my_other_dag_to_ignore"
airflow variables --set max_db_entry_age_in_days 365
```

## testing
```bash
git clone https://github.com/airflow-maintenance-dags
# rename so python imports can work (replace dashes w/ underscores)
mv airflow-maintenance-dags airflow_maintenance_dags
pip install -r requirements.txt
airflow initdb  # sets up local sqllite db for testing
airflow version  # check that this runs w/o errors
python3 -m pytest ./airflow_maintenance_dags
```
