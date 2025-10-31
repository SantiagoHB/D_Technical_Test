import os
import psycopg2
import logging
from typing import List, Dict, Tuple

logger = logging.getLogger("write")

# === Configuración de conexión ===
def get_connection():
    """Obtiene la conexión a la base de datos PostgreSQL."""
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB", "airflow"),
        user=os.getenv("POSTGRES_USER", "airflow"),
        password=os.getenv("POSTGRES_PASSWORD", "airflow"),
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432")
    )


# === Inserción principal ===
def write(regulations: List[Dict], components: List[Dict]) -> Tuple[int, int]:
    """
    Inserta regulaciones y sus componentes asociados.
    Retorna una tupla: (regulations_inserted, components_inserted)
    """
    if not regulations:
        logger.info("No hay regulaciones para insertar.")
        return 0, 0

    conn = get_connection()
    inserted_regs = 0
    inserted_comps = 0

    try:
        with conn:
            with conn.cursor() as cur:
                logger.info(f"Insertando {len(regulations)} regulaciones...")

                for row in regulations:
                    # === Inserción / Upsert de regulations ===
                    cur.execute("""
                        INSERT INTO regulations (
                            title, created_at, entity, summary,
                            external_link, classification_id,
                            rtype_id, gtype, is_active, update_at
                        )
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,now())
                        ON CONFLICT (title, created_at, entity)
                        DO UPDATE SET
                            summary = EXCLUDED.summary,
                            external_link = EXCLUDED.external_link,
                            update_at = now()
                        RETURNING id;
                    """, (
                        row.get("title"),
                        row.get("created_at"),
                        row.get("entity"),
                        row.get("summary"),
                        row.get("external_link"),
                        row.get("classification_id"),
                        row.get("rtype_id"),
                        row.get("gtype"),
                        row.get("is_active", True)
                    ))

                    regulation_id = cur.fetchone()[0]
                    inserted_regs += 1

                    # === Inserción del componente asociado ===
                    # Usa el mismo índice del loop si la lista components está alineada
                    comp = components[min(inserted_regs - 1, len(components) - 1)]
                    components_id = comp.get("components_id", 7)

                    cur.execute("""
                        INSERT INTO regulations_component (regulations_id, components_id)
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING;
                    """, (regulation_id, components_id))

                    inserted_comps += 1

        conn.commit()
        logger.info(
            f"Filas procesadas: {inserted_regs} regulaciones y {inserted_comps} componentes insertados."
        )
        return inserted_regs, inserted_comps

    except Exception as e:
        conn.rollback()
        logger.error(f"Error durante la escritura: {e}")
        return 0, 0

    finally:
        conn.close()
