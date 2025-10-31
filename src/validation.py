import json
import logging
import os
import re
from typing import List, Dict, Tuple

logger = logging.getLogger("validation")

# Ruta al archivo de reglas
RULES_PATH = os.getenv("VALIDATION_RULES_FILE", "/opt/airflow/configs/validation_rules.json")


def load_rules():
    """Carga las reglas de validación desde el archivo JSON."""
    try:
        with open(RULES_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error cargando reglas de validación: {e}")
        raise


def validate_field(value, rules):
    """Valida un campo individual según las reglas."""
    if value is None:
        return not rules.get("required", False)

    # Validación de tipo entero
    if rules.get("type") == "int":
        if not str(value).isdigit():
            return False

    # Validación por expresión regular
    if rules.get("regex"):
        if not re.match(rules["regex"], str(value)):
            return False

    return True


def validate_regulations(data: List[Dict]) -> List[Dict]:
    """Valida las regulaciones según las reglas definidas."""
    rules = load_rules()
    valid_rows = []
    discarded = 0

    for row in data:
        valid = True
        for field, cfg in rules.get("fields", {}).items():
            val = row.get(field)
            if not validate_field(val, cfg):
                if cfg.get("required", False):
                    valid = False
                    break
                row[field] = None
        if valid:
            valid_rows.append(row)
        else:
            discarded += 1

    logger.info(f"Validación completada: {len(valid_rows)} válidas, {discarded} descartadas.")
    return valid_rows


def validate(regulations: List[Dict], components: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
    """
    Valida las regulaciones y retorna ambas listas (regulations y components)
    sin alterar los componentes, ya que son datos fijos.
    """
    valid_regs = validate_regulations(regulations)

    # Los componentes no necesitan validación, pero se mantiene la relación 1 a 1
    if len(components) != len(regulations):
        logger.warning("Número de componentes no coincide con las regulaciones. Se ajustará automáticamente.")
        components = components[:len(valid_regs)]

    return valid_regs, components
