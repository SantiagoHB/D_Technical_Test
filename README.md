Proyecto de ETL y Scraping de Normativas

Descripcion del proyecto
Este proyecto es una solucion tecnica que permite scrapear normativas oficiales, procesarlas y almacenarlas en una base de datos PostgreSQL para su analisis y consumo posterior.

El proyecto incluye:
- Scraping de normativas desde paginas oficiales.
- Limpieza y normalizacion de los datos extraidos.
- Clasificacion automatica de las normativas segun palabras clave.
- Insercion eficiente en la base de datos PostgreSQL evitando duplicados.
- Registro de componentes asociados a cada normativa.
- Automatizacion de tareas mediante Apache Airflow.

Estructura del proyecto
configs/            # Configuraciones del proyecto
dags/               # DAGs de Airflow para automatizacion de ETL
logs/               # Logs generados por Airflow
plugins/            # Plugins adicionales de Airflow
src/                # Codigo fuente del proyecto
  lambda.py         # Script principal de scraping y procesamiento
  write.py          # Modulo para insercion de datos en PostgreSQL
.env                # Variables de entorno (no incluidas en el repo)
docker-compose.yml  # Configuracion de contenedores para Airflow y PostgreSQL
Dockerfile          # Imagen Docker para el proyecto
requirements.txt    # Dependencias de Python
README.md           # Este archivo

Tecnologias utilizadas
- Python 3.9+
- BeautifulSoup para scraping
- pandas para manipulacion de datos
- psycopg2 para conexion a PostgreSQL
- boto3 para interaccion con AWS (Secrets Manager)
- Apache Airflow para orquestacion de tareas
- Docker y Docker Compose para contenerizacion del proyecto
- PostgreSQL 15 como base de datos

Funcionalidades principales
1. Scraping de normativas
   - Extrae titulo, enlace, fecha de creacion y resumen de normativas.
   - Normaliza y limpia los datos eliminando caracteres especiales.

2. Clasificacion de documentos
   - Asigna automaticamente un rtype_id segun palabras clave en el titulo.
   - Todos los documentos se etiquetan con una clasificacion fija (classification_id).

3. Insercion segura en base de datos
   - Evita duplicados comparando title, created_at y external_link.
   - Inserta componentes asociados a cada normativa.

4. Automatizacion con Airflow
   - DAG principal (dag_etl_ani) que ejecuta tareas de scraping y escritura.
   - Logging detallado para monitoreo de procesos.

5. Contenerizacion y despliegue
   - Configuracion de Docker Compose para levantar Airflow y PostgreSQL.
   - Scripts y dependencias gestionadas en Python para reproducibilidad.

Como ejecutar el proyecto
1. Clonar el repositorio:
   git clone <repo-url>
   cd Dapper_Technical_Test

2. Configurar variables de entorno en .env (credenciales de PostgreSQL y AWS).

3. Levantar contenedores:
   docker-compose up -d

4. Ejecutar DAG de Airflow desde la interfaz web (localhost:8080) o pruebas locales desde lambda.py:
   python src/lambda.py

Consideraciones
- Los archivos sensibles como .env y configuraciones privadas no estan incluidos en el repositorio.
- La base de datos debe inicializarse con las tablas regulations y regulations_component antes de ejecutar el ETL.

Autor
- Santiago Hernandez
