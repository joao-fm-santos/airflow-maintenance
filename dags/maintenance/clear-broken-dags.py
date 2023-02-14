"""
Maintenance workflow that periodically deletes DAG files and clean entries in the ImportError table 
for DAGS that Airflow cannot parse or import properly.
This ensures that the ImportError table is cleaned every day.
"""
from datetime import timedelta
import logging
import os

from airflow import settings
import airflow
from airflow.models import DAG, ImportError 
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
# Maintenance - clean broken dags

This DAG constitutes of a maintenance workflow that is deployed into Airflow 
to periodically delete DAG files and clean entries in the ImportError table for DAGs that Airflow cannot
parse or import properly.

This ensures that the ImportError table is cleaned every day.

## Deployment 

This maintenance DAG is automatically deployed via the git-sync sidecar application. 
However, there might be a case when you would need to deploy it to do special debugging, and for that, you can follow the tutorials below.

### Manually in local machine
To deploy the dag, you need to:

- Get pods (`kubectl get pods`);
- Copy webserver pod id;
- Execute into the webserver (`kubectl exec -it <pod id> -c server -- bash`);
- Navigate to the dags folder' (`cd dags`);
- Create new folder and file (`mkdir maintenance && touch maintenance/clear-missing-dags.py`);
- Run `wget https://raw.githubusercontent.com/joao-fm-santos/airflow-maintenance/main/dags/maintenance/clear-broken-dags.py`;
- Update properties if desired and save;
- Exit pod;
- Port-forward the UI (`kubectl port-forward svc/airflow-webserver 8080:8080`);
- Open new browser tab to `localhost:8080` and login to airflow;
- Enable the DAG.

### Manually in remote machine
To deploy the dag, you need to:

- Start the VM if not already;
- Login via bastion or ssh;
- Get pods (`kubectl get pods`);
- Copy webserver pod id;
- Execute into the webserver (`kubectl exec -it <pod id> -c server -- bash`);
- Navigate to the dags folder' (`cd dags`);
- Create new folder and file (`mkdir maintenance && touch maintenance/clear-broken-dags.py`);
- Run `wget https://raw.githubusercontent.com/joao-fm-santos/airflow-maintenance/main/dags/maintenance/clear-broken-dags.py`;
- Update properties if desired and save;
- Exit pod;
- Open new browser tab to `<ingress url>` and login to airflow;
- Enable the DAG.
"""

# ----------------------------
# Dag builder 
# ----------------------------
dag = DAG(
    "airflow-maintenance-clear-broken-dags",
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
def clear_broken_dags() -> None:
    """
    Clears the broken dags from ImportError table.
    """
    log_id = '[MAINTENANCE]|[CLEAR_BROKEN_DAGS]|' 
    logging.info(f'{log_id}Process started...')

    session = settings.Session()
    errors = session.query(ImportError).all()

    for error in errors:
        if os.path.exists(error.filename):
            logging.info(f'{log_id}Deleting broken DAG \'{error.filename}\'...')
            os.remove(error.filename)
        session.delete(error)
    logging.info(f'{log_id}Process completed.')

# ----------------------------
# Runner 
# ----------------------------
clear_broken_dags = PythonOperator(
    task_id='clear_broken_dags',
    python_callable=clear_broken_dags,
    dag=dag
) 

