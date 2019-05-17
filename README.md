# airflow-maintenance-dags
A series of DAGs/Workflows to help maintain the operation of Airflow

## DAGs/Workflows

* db-cleanup
    * A maintenance workflow that you can deploy into Airflow to periodically clean out the DagRun, TaskInstance, Log, XCom, Job DB and SlaMiss entries to avoid having too much data in your Airflow MetaStore.
* kill-halted-tasks
    * A maintenance workflow that you can deploy into Airflow to periodically kill off tasks that are running in the background that don't correspond to a running task in the DB.
    * This is useful because when you kill off a DAG Run or Task through the Airflow Web Server, the task still runs in the background on one of the executors until the task is complete.
* log-cleanup
    * A maintenance workflow that you can deploy into Airflow to periodically clean out the task logs to avoid those getting too big.


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
