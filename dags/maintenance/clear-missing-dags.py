"""
Maintenance workflow that periodically cleans entries in the DAG table 
if there is no longer a corresponding Python files.
This ensures that the DAG table is clean with only active DAGs.
"""
from datetime import timedelta
import logging
import os

from airflow import settings
import airflow
from airflow.models import DAG, DagModel
from airflow.operators.python_operator import PythonOperator

# ----------------------------
# Dag properties
# ----------------------------
properties = {
    "owner": "devops",
    "depends_on_past": False,
    "email": ["jpsantos93@hotmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "start_date": airflow.utils.dates.days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

doc_md = """
# Maintenance - clean old dags

This DAG constitutes of a maintenance workflow that is deployed into Airflow 
to periodically clean entries in the DAG table when there is no longer a corresponding python
file for it. 

This ensures that the DAG table is clean and contains only active DAGs.

## Deployment 

This maintenance is automatically deployed via the git-sync sidecar application. 
However, there might be a case when you would need to deploy it to do special debugging, and for that, you can follow the tutorials below.

### Manually in local machine
To deploy the dag, you need to:

- Get pods (`kubectl get pods`);
- Copy webserver pod id;
- Execute into the webserver (`kubectl exec -it <pod id> -c server -- bash`);
- Navigate to the dags folder' (`cd dags`);
- Create new folder and file (`mkdir maintenance && touch maintenance/clear-missing-dags.py`);
- Run `wget https://raw.githubusercontent.com/joao-fm-santos/airflow-maintenance/main/dags/maintenance/clear-missing-dags.py`;
- Update properties if desired and save;
- Exit pod;
- Port-forward the UI (`kubectl port-forward svc/airflow-webserver 8080:8080`);
- Open new browser tab to `localhost:8080` and login to airflow;
- Enable the DAG

### Manually in remote machine
To deploy the dag, you need to:

- Start the VM if not already;
- Login via bastion or ssh;
- Get pods (`kubectl get pods`);
- Copy webserver pod id;
- Execute into the webserver (`kubectl exec -it <pod id> -c server -- bash`);
- Navigate to the dags folder' (`cd dags`);
- Create new folder and file (`mkdir maintenance && touch maintenance/clear-missing-dags.py`);
- Run `wget https://raw.githubusercontent.com/joao-fm-santos/airflow-maintenance/main/dags/maintenance/clear-missing-dags.py`;
- Update properties if desired and save;
- Exit pod;
- Open new browser tab to `<ingress url>` and login to airflow;
- Enable the DAG
"""

# ----------------------------
# Dag builder 
# ----------------------------
dag = DAG(
    "airflow-maintenance-clean-missing-dags",
    default_args=properties,
    schedule_interval="@daily",
    start_date=airflow.utils.dates.days_ago(1),
    tags=["maintenance", "devops"],
    doc_md=doc_md,
    catchup=False
)

# ----------------------------
# Logic builder 
# ----------------------------
def clean_missing_dags() -> None:
    """
    Clears the missing dags from DAG table.
    """
    log_id = '[MAINTENANCE]|[CLEAN_MISSING_DAGS]|' 
    logging.info(f'{log_id}Process started...')

    session = settings.Session()
    dags = session.query(DagModel).all()
    to_delete = []

    # First check the file path that needs to be imported to load 
    # any DAG or subdag, aka`dag.fileloc`
    # According to the docs, this may not be an actual file on disk
    # when it is loaded from a ZIP file. but just a path
    # If this happens, we take the ZIP file as its path without the extension
    for dag in dags:
        if dag.fileloc is not None and '.zip/' in dag.fileloc:
            index = dag.fileloc.rfind('.zip/') + len('.zip')
            fileloc = dag.fileloc[0:index]
        else:
            fileloc = dag.fileloc

        # Now we mark any files with unknown file paths for deletion
        empty_file_location = fileloc is None
        file_location_does_not_exist = not os.path.exists(fileloc)

        if empty_file_location:
            message = f"{log_id}DAG '{str(dag)}' has a empty file path. Marked for DELETION."
            logging.info(message)
            to_delete.append(dag)

        elif file_location_does_not_exist:
            message = f"{log_id}DAG '{str(dag)}' references an unknown path that does not exist. Marked for DELETION."
            logging.info(message)
            to_delete.append(dag)

    logging.info(f'{log_id}Deleting marked DAGS...')
    for entry in to_delete:
        session.delete(entry)
    session.commit()
    logging.info(f'{log_id}Process completed.')


# ----------------------------
# Runner 
# ----------------------------
clear_missing_dags = PythonOperator(
    task_id='clean_missing_dags',
    python_callable=clean_missing_dags,
    dag=dag
) 
