from airflow import DAG
from airflow.sdk.bases.sensor import BaseSensorOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.bases.hook import BaseHook
from airflow.sdk import Variable
from datetime import datetime
import requests
import os
import glob
import shutil

# ============================================================
# CONFIG
# ============================================================

N8N_BASE_URL     = Variable.get("N8N_BASE_URL")
N8N_CONNECTIONS  = BaseHook.get_connection("n8n_api_GES_FL002")
N8N_API_KEY      = N8N_CONNECTIONS.password
WEBHOOK_URL      = N8N_CONNECTIONS.host

WATCH_FOLDER     = Variable.get("GES_FL002_WATCH_FOLDER")
ERROR_FOLDER     = Variable.get("GES_FL002_ERROR_FOLDER")

# ============================================================
# CALLBACK ERREUR
# ============================================================

def on_failure_move_to_error(context):
    file_path = context['ti'].xcom_pull(task_ids='Watch_folder', key='detected_file_path')
    if file_path and os.path.exists(file_path):
        os.makedirs(ERROR_FOLDER, exist_ok=True)
        filename = os.path.basename(file_path)
        dest = os.path.join(ERROR_FOLDER, filename)
        shutil.move(file_path, dest)
        print(f"[LOG] ❌ Echec sur la tâche : {context['task_instance'].task_id}")
        print(f"[LOG] ❌ Fichier déplacé en erreur : {dest}")
    else:
        print(f"[LOG] ❌ Echec sur la tâche : {context['task_instance'].task_id} — fichier introuvable ou déjà supprimé")

# ============================================================
# SENSOR DOSSIER
# ============================================================

class FolderFileSensor(BaseSensorOperator):
    """Surveille un dossier et retourne le premier fichier MP4 trouvé"""

    def __init__(self, folder, **kwargs):
        super().__init__(**kwargs)
        self.folder = folder

    def poke(self, context):
        files = glob.glob(os.path.join(self.folder, "*.mp4"))

        if not files:
            self.log.info(f"[LOG] Aucun fichier MP4 dans {self.folder}, on attend...")
            return False

        # Prend le fichier le plus ancien
        oldest = min(files, key=os.path.getctime)
        filename = os.path.basename(oldest)

        self.log.info(f"[LOG] Fichier détecté : {filename}")
        context['ti'].xcom_push(key='detected_file', value=filename)
        context['ti'].xcom_push(key='detected_file_path', value=oldest)
        return True


# ============================================================
# SENSOR N8N
# ============================================================

class N8nExecutionSensor(BaseSensorOperator):

    def __init__(self, base_url, api_key, trigger_task_id, **kwargs):
        super().__init__(**kwargs)
        self.base_url        = base_url
        self.api_key         = api_key
        self.trigger_task_id = trigger_task_id

    def poke(self, context):
        headers = {"X-N8N-API-KEY": self.api_key}

        execution_ids = context['ti'].xcom_pull(task_ids=self.trigger_task_id, key='execution_ids')
        if not execution_ids:
            self.log.info("[LOG] Execution IDs pas encore disponibles, on attend...")
            return False

        self.log.info(f"[LOG] Poll execution IDs : {execution_ids}")
        self.log.info("========================================")

        all_success = True

        for execution_id in execution_ids:
            result = requests.get(
                f"{self.base_url}/api/v1/executions/{execution_id}",
                headers=headers,
                params={"includeData": "true"}
            )
            result.raise_for_status()
            execution = result.json()

            status        = execution.get('status')
            workflow_name = execution.get('workflowData', {}).get('name', execution_id)

            self.log.info(f"[LOG] Workflow : {workflow_name}")
            self.log.info(f"[LOG] Statut   : {status}")
            self.log.info(f"[LOG] Démarré  : {execution.get('startedAt', '')}")
            self.log.info(f"[LOG] Terminé  : {execution.get('stoppedAt', '')}")

            run_data = execution.get('data', {}).get('resultData', {}).get('runData', {})
            for node_name, node_data in run_data.items():
                self.log.info(f"[LOG]   Noeud : {node_name} → {node_data[0].get('executionStatus', '')}")

            if status in ('error', 'crashed'):
                error = execution.get('data', {}).get('resultData', {}).get('error', {})
                self.log.error(f"[LOG] ❌ n8n KO : {error}")
                raise Exception(f"n8n KO execution {execution_id} : {error}")

            if status != 'success':
                self.log.info(f"[LOG] Execution {execution_id} encore en cours...")
                all_success = False

            self.log.info("========================================")

        if all_success:
            self.log.info("[LOG] Tous les workflows n8n terminés avec succès ✅")
            return True

        return False


# ============================================================
# FONCTION DE TRIGGER N8N
# ============================================================

def GES_FL002_0000_Transcribe_trigger(**context):
    filename = context['ti'].xcom_pull(task_ids='Watch_folder', key='detected_file')

    payload = {
        "filename":     filename,
        "logical_date": str(context['logical_date']),
        "dag_id":       context['dag'].dag_id,
        "run_id":       context['run_id']
    }

    print("========================================")
    print(f"[LOG] Trigger n8n démarré  : {datetime.now()}")
    print(f"[LOG] DAG ID               : {context['dag'].dag_id}")
    print(f"[LOG] RUN ID               : {context['run_id']}")
    print(f"[LOG] Fichier détecté      : {filename}")
    print(f"[LOG] Logical date         : {context['logical_date']}")
    print(f"[LOG] URL webhook          : {WEBHOOK_URL}")
    print(f"[LOG] Payload              : {payload}")
    print("========================================")

    response = requests.post(WEBHOOK_URL, json=payload)
    response.raise_for_status()

    data = response.json()
    print(f"[LOG] Réponse complète : {data}")

    if isinstance(data, list):
        execution_ids = list(set([item.get('log_execution_id') for item in data if item.get('log_execution_id')]))
    else:
        execution_ids = [data.get('log_execution_id')]

    print("========================================")
    print(f"[LOG] Réponse HTTP      : {response.status_code}")
    print(f"[LOG] Execution IDs n8n : {execution_ids}")
    print(f"[LOG] Webhook déclenché : {datetime.now()}")
    print("========================================")

    context['ti'].xcom_push(key='execution_ids', value=execution_ids)
    return execution_ids


# ============================================================
# DÉFINITION DU DAG
# ============================================================

with DAG(
    dag_id="GES_FL002_0000_Transcribe_main",
    start_date=datetime(2026, 1, 1),
    schedule="*/1 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["n8n", "ges_fl002", "transcription"]
) as dag:

    watch = FolderFileSensor(
        task_id="Watch_folder",
        folder=WATCH_FOLDER,
        poke_interval=30,
        timeout=3600,
        mode="poke",
    )

    trigger = PythonOperator(
        task_id="Trigger_n8n_GES_FL002_0000_Transcribe",
        python_callable=GES_FL002_0000_Transcribe_trigger,
        on_failure_callback=on_failure_move_to_error,
    )

    sensor = N8nExecutionSensor(
        task_id="Wait_n8n_GES_FL002_0000_Transcribe",
        base_url=N8N_BASE_URL,
        api_key=N8N_API_KEY,
        trigger_task_id="Trigger_n8n_GES_FL002_0000_Transcribe",
        poke_interval=10,
        timeout=7200,
        mode="poke",
        on_failure_callback=on_failure_move_to_error,
    )

    watch >> trigger >> sensor
