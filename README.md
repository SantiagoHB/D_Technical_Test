# Prueba Técnica - Refactor de Pipeline ETL (ANI)

Este repositorio contiene la solución a una prueba técnica que consiste en refactorizar un script monolítico de Python (diseñado para AWS Lambda) a un pipeline ETL modular (Extracción, Validación, Escritura) orquestado con Apache Airflow.

El pipeline extrae datos de normativas de la ANI, los valida contra un set de reglas configurables y los carga en una base de datos PostgreSQL, asegurando la idempotencia (no se insertan duplicados).

## 🚀 Tecnologías Utilizadas

* **Orquestación:** Apache Airflow
* **Contenedores:** Docker y Docker Compose
* **Base de Datos:** PostgreSQL 15
* **Core:** Python 3.9
* **Librerías:** Pandas, BeautifulSoup (Scraping), Psycopg2 (DB)

## 📁 Estructura del Repositorio

-   `/configs/validation_rules.json`: Archivo JSON con las reglas de validación (regex, tipo, etc.).
-   `/dags/dags_etl.py`: Definición del DAG principal de Airflow (`dag_etl_ani`).
-   `/src/extraction.py`: Módulo de extracción (scraping).
-   `/src/validation.py`: Módulo de validación de datos.
-   `/src/write.py`: Módulo de escritura (persistencia) que contiene la lógica de idempotencia.
-   `DDL.sql`: Script DDL para crear las tablas `regulations` y `regulations_component`.
-   `docker-compose.yml`: Define los servicios de Airflow (webserver, scheduler) y Postgres.
-   `Dockerfile`: Define la imagen de Airflow con las dependencias de Python.

---

## 🏃‍♂️ Cómo Ejecutar el Proyecto

### 1. Levantar el Entorno

Inicia todos los servicios de Airflow y la base de datos de Postgres.

```bash
docker-compose up -d --build

### 2. Crear el Esquema de la Base de Datos

Espera un minuto a que el contenedor de Postgres inicie. Luego, ejecuta el siguiente comando en tu terminal para crear las tablas (asegúrate de que DDL.sql esté en tu carpeta).

# (En Windows/PowerShell)
cat DDL_corregido.sql | docker exec -i dapper_technical_test-postgres-1 psql -U airflow -d airflow

###3. Acceder a Airflow
Abre tu navegador y ve a: URL: http://localhost:8080 Usuario: admin Clave: admin

###4. Ejecutar el Pipeline
En la interfaz de Airflow, busca el DAG llamado dag_etl_ani.

Actívalo (con el interruptor a la izquierda).

Haz clic en el nombre del DAG y presiona el botón "Play" (▶️) en la esquina superior derecha para ejecutarlo.

###5. Verificar la Idempotencia
Primera Ejecución: Revisa los logs de la tarea write_task. Deberías ver un mensaje como New inserted: 29.

Segunda Ejecución: Ejecuta el DAG una segunda vez. Revisa los logs de write_task de esta nueva ejecución. Deberías ver New inserted: 0 y No new records found.... Esto confirma que la lógica de idempotencia funciona.