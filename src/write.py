import os
import psycopg2
import logging
import pandas as pd
from typing import List, Dict, Tuple, Any
from datetime import datetime

logger = logging.getLogger("write")

# Constante de la Lambda original
ENTITY_VALUE = 'Agencia Nacional de Infraestructura'

# --- CLASE DATABASEMANAGER (Refactorizada) ---
# Esta clase está basada en la de lambda.py, pero modificada
# para usar variables de entorno en lugar de AWS Secrets Manager.
class DatabaseManager:
    def __init__(self):
        self.connection = None
        self.cursor = None
        # Lee las credenciales desde las variables de entorno
        self.db_name = os.getenv("POSTGRES_DB", "airflow")
        self.user = os.getenv("POSTGRES_USER", "airflow")
        self.password = os.getenv("POSTGRES_PASSWORD", "airflow")
        self.host = os.getenv("POSTGRES_HOST", "postgres")
        self.port = os.getenv("POSTGRES_PORT", "5432")

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                dbname=self.db_name,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            self.cursor = self.connection.cursor()
            logger.info("Conexión a BD (Postgres) exitosa.")
            return True
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            return False

    def close(self):
        # Lógica de lambda.py
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def execute_query(self, query, params=None):
        # Lógica de lambda.py
        if not self.cursor:
            raise Exception("Database not connected")
        self.cursor.execute(query, params)
        return self.cursor.fetchall()

    def bulk_insert(self, df, table_name):
        # Lógica de lambda.py
        if not self.connection or not self.cursor:
            raise Exception("Database not connected")
        
        try:
            df = df.astype(object).where(pd.notnull(df), None)
            columns_for_sql = ", ".join([f'"{col}"' for col in df.columns])
            placeholders = ", ".join(["%s"] * len(df.columns))
            
            insert_query = f"INSERT INTO {table_name} ({columns_for_sql}) VALUES ({placeholders})"
            records_to_insert = [tuple(x) for x in df.values]
            
            self.cursor.executemany(insert_query, records_to_insert)
            self.connection.commit()
            return len(df)
        except Exception as e:
            self.connection.rollback()
            raise Exception(f"Error inserting into {table_name}: {str(e)}")

# --- LÓGICA DE IDEMPOTENCIA (Copiada de lambda.py) ---
# Estas son las funciones originales que cumplen el requisito R8.

def insert_regulations_component(db_manager, new_ids):
    """
    Inserta los componentes de las regulaciones.
    (Copiada de lambda.py)
    """
    if not new_ids:
        return 0, "No new regulation IDs provided"

    try:
        # La lógica original asume un 'components_id' fijo de 7
        id_rows = pd.DataFrame(new_ids, columns=['regulations_id'])
        id_rows['components_id'] = 7
        
        inserted_count = db_manager.bulk_insert(id_rows, 'regulations_component')
        return inserted_count, f"Successfully inserted {inserted_count} regulation components"
        
    except Exception as e:
        return 0, f"Error inserting regulation components: {str(e)}"

def insert_new_records(db_manager, df, entity):
    """
    Inserta nuevos registros en la base de datos evitando duplicados.
    (Copiada de lambda.py)
    """
    regulations_table_name = 'regulations'
    
    try:
        # 1. OBTENER REGISTROS EXISTENTES
        query = """
            SELECT title, created_at, entity, COALESCE(external_link, '') as external_link 
            FROM {} 
            WHERE entity = %s
        """.format(regulations_table_name)
        
        existing_records = db_manager.execute_query(query, (entity,))
        
        if not existing_records:
            db_df = pd.DataFrame(columns=['title', 'created_at', 'entity', 'external_link'])
        else:
            db_df = pd.DataFrame(existing_records, columns=['title', 'created_at', 'entity', 'external_link'])
        
        logger.info(f"Registros existentes en BD para {entity}: {len(db_df)}")
        
        # 2. PREPARAR DATAFRAME DE LA ENTIDAD
        entity_df = df[df['entity'] == entity].copy()
        
        if entity_df.empty:
            return 0, f"No records found for entity {entity}"
        
        logger.info(f"Registros a procesar para {entity}: {len(entity_df)}")
        
        # 3. NORMALIZAR DATOS PARA COMPARACIÓN
        if not db_df.empty:
            db_df['created_at'] = db_df['created_at'].astype(str)
            db_df['external_link'] = db_df['external_link'].fillna('').astype(str)
            db_df['title'] = db_df['title'].astype(str).str.strip()
        
        entity_df['created_at'] = entity_df['created_at'].astype(str)
        entity_df['external_link'] = entity_df['external_link'].fillna('').astype(str)
        entity_df['title'] = entity_df['title'].astype(str).str.strip()
        
        # 4. IDENTIFICAR DUPLICADOS (Lógica de lambda.py)
        if db_df.empty:
            new_records = entity_df.copy()
            duplicates_found = 0
        else:
            entity_df['unique_key'] = (
                entity_df['title'] + '|' + 
                entity_df['created_at'] + '|' + 
                entity_df['external_link']
            )
            db_df['unique_key'] = (
                db_df['title'] + '|' + 
                db_df['created_at'] + '|' + 
                db_df['external_link']
            )
            
            existing_keys = set(db_df['unique_key'])
            entity_df['is_duplicate'] = entity_df['unique_key'].isin(existing_keys)
            
            new_records = entity_df[~entity_df['is_duplicate']].copy()
            duplicates_found = len(entity_df) - len(new_records)
        
        # 5. REMOVER DUPLICADOS INTERNOS DEL DATAFRAME
        new_records = new_records.drop_duplicates(
            subset=['title', 'created_at', 'external_link'], 
            keep='first'
        )
        internal_duplicates = len(entity_df) - duplicates_found - len(new_records)
        total_duplicates = duplicates_found + internal_duplicates
        
        if new_records.empty:
            logger.info(f"No new records found for entity {entity} after duplicate validation")
            return 0, f"No new records found for entity {entity} after duplicate validation"
        
        # 6. LIMPIAR DATAFRAME ANTES DE INSERTAR
        columns_to_drop = ['unique_key', 'is_duplicate']
        for col in columns_to_drop:
            if col in new_records.columns:
                new_records = new_records.drop(columns=[col])
        
        # 7. INSERTAR NUEVOS REGISTROS
        total_rows_processed = 0
        try:
            total_rows_processed = db_manager.bulk_insert(new_records, regulations_table_name)
            if total_rows_processed == 0:
                return 0, f"No records were actually inserted for entity {entity}"
        except Exception as insert_error:
            logger.error(f"Error en inserción: {insert_error}")
            if "duplicate" in str(insert_error).lower() or "unique" in str(insert_error).lower():
                return 0, f"Some records for entity {entity} were duplicates and skipped"
            else:
                raise insert_error
        
        # 8. OBTENER IDS DE REGISTROS INSERTADOS
        new_ids_query = f"""
            SELECT id FROM {regulations_table_name}
            WHERE entity = %s 
            ORDER BY id DESC
            LIMIT %s
        """
        new_ids_result = db_manager.execute_query(new_ids_query, (entity, total_rows_processed))
        new_ids = [row[0] for row in new_ids_result]
        
        # 9. INSERTAR COMPONENTES DE REGULACIÓN
        inserted_count_comp, component_message = insert_regulations_component(db_manager, new_ids)
        
        # 10. MENSAJE FINAL
        stats = (
            f"Processed: {len(entity_df)} | "
            f"Duplicates skipped: {total_duplicates} | "
            f"New inserted: {total_rows_processed}"
        )
        message = f"Entity {entity}: {stats}. {component_message}"
        logger.info(message)
        
        return total_rows_processed, message
        
    except Exception as e:
        if hasattr(db_manager, 'connection') and db_manager.connection:
            db_manager.connection.rollback()
        error_msg = f"Error processing entity {entity}: {str(e)}"
        logger.error(f"ERROR CRÍTICO: {error_msg}")
        return 0, error_msg

# --- FUNCIÓN PRINCIPAL (Llamada por el DAG) ---

def write(regulations: List[Dict], components: List[Dict]) -> Tuple[int, int]:
    """
    Punto de entrada para la tarea de escritura del DAG.
    Usa la lógica de idempotencia original de lambda.py.
    """
    if not regulations:
        logger.info("No hay regulaciones validadas para escribir.")
        return 0, 0

    # Convertir la lista de dicts (de XCom) a DataFrame (requerido por insert_new_records)
    df_normas = pd.DataFrame(regulations)
    
    # La lógica original (insert_new_records) no usa la lista 'components'.
    # Genera los componentes internamente usando un ID fijo (7).
    # Por lo tanto, ignoramos el argumento 'components' para ser fieles al requisito.

    db_manager = DatabaseManager()
    if not db_manager.connect():
        raise Exception("Fallo al conectar con la base de datos")
    
    try:
        inserted_count, status_message = insert_new_records(
            db_manager, df_normas, ENTITY_VALUE
        )
        
        # La lógica original 'insert_regulations_component' se llama *dentro* de 'insert_new_records'
        # por lo que no necesitamos un conteo separado aquí.
        
        logger.info(status_message)
        # Devolvemos el conteo de regulaciones y 0 para componentes (ya que está incluido)
        return inserted_count, 0 

    except Exception as e:
        logger.error(f"Error en la tarea de escritura: {e}")
        raise e
    finally:
        db_manager.close()