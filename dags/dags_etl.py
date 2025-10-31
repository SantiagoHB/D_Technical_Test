from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

import sys
import os

# Agrega la carpeta 'src' al PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))


# Asegúrate de que estos módulos estén en /opt/airflow/src y PYTHONPATH lo incluya
from extraction import extract
from validation import validate
from write import write

logger = logging.getLogger("dag_etl_ani")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1
}

with DAG(
    dag_id="dag_etl_ani",
    schedule_interval=None,  # Ejecución manual
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["ANI", "ETL"]
) as dag:

    # === 1️⃣ Extracción ===
    def task_extract(**ctx):
        """
        Llama al módulo extraction.py para obtener las regulaciones y sus componentes.
        """
        logger.info("Iniciando extracción de datos de la ANI...")
        data = extract(num_pages=3)

        # Verifica que extract() devuelva dict con ambas llaves
        if not isinstance(data, dict) or "regulations" not in data:
            raise ValueError("extract() debe retornar un dict con 'regulations' y 'components'.")

        regulations = data.get("regulations", [])
        components = data.get("components", [])

        # Enviar a XCom
        ctx["ti"].xcom_push(key="regulations", value=regulations)
        ctx["ti"].xcom_push(key="components", value=components)

        logger.info(
            f"Extracción completada: {len(regulations)} regulaciones y {len(components)} componentes obtenidos."
        )

    # === 2️⃣ Validación ===
    def task_validate(**ctx):
        """
        Valida los datos extraídos antes de cargarlos en la base de datos.
        """
        logger.info("Iniciando validación de datos...")

        regulations = ctx["ti"].xcom_pull(key="regulations", task_ids="extract_task")
        components = ctx["ti"].xcom_pull(key="components", task_ids="extract_task")

        valid_regs, valid_comps = validate(regulations, components)

        ctx["ti"].xcom_push(key="validated_regs", value=valid_regs)
        ctx["ti"].xcom_push(key="validated_comps", value=valid_comps)

        logger.info(
            f"Validación completada: {len(valid_regs)} regulaciones válidas y {len(valid_comps)} componentes válidos."
        )

    # === 3️⃣ Carga (Write) ===
    def task_write(**ctx):
        """
        Inserta las regulaciones y sus componentes en la base de datos.
        """
        logger.info("Iniciando escritura en la base de datos...")

        regs = ctx["ti"].xcom_pull(key="validated_regs", task_ids="validate_task") or []
        comps = ctx["ti"].xcom_pull(key="validated_comps", task_ids="validate_task") or []

        count_regs, count_comps = write(regs, comps)

        logger.info(
            f"Escritura completada: {count_regs} regulaciones insertadas y {count_comps} componentes insertados."
        )

    # === Definición de Tareas ===
    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=task_extract
    )

    validate_task = PythonOperator(
        task_id="validate_task",
        python_callable=task_validate
    )

    write_task = PythonOperator(
        task_id="write_task",
        python_callable=task_write
    )

    # === Dependencias ===
    extract_task >> validate_task >> write_task
