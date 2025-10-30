import json, re, pandas as pd

def load_rules(path="src/validation/rules.json"):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

import re
import json

import json
import re
import pandas as pd
from pathlib import Path

def _is_empty(value) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False

def _type_ok(value, expected_type: str) -> bool:
    """
    Valida tipo de forma básica y predecible sin castear:
      - "string": debe ser str
      - "int": debe ser int (no float con .0)
      - "float": debe ser float o int (permitimos int como float válido)
      - "boolean": debe ser bool
      - "date": lo validará el regex (aquí solo se permite str o datetime formateado a str previamente)
    """
    if _is_empty(value):
        return True  # vacío se evalúa en otra regla (required)
    if expected_type == "string":
        return isinstance(value, str)
    if expected_type == "int":
        return isinstance(value, int) and not isinstance(value, bool)  # bool es subclass de int
    if expected_type == "float":
        return isinstance(value, (float, int)) and not isinstance(value, bool)
    if expected_type == "boolean":
        return isinstance(value, bool)
    if expected_type == "date":
        # El chequeo real vendrá por regex (YYYY-MM-DD). Aquí permitimos str.
        return isinstance(value, str)
    # Si no se reconoce el tipo, no lo invalidamos por tipo.
    return True

def load_rules(path: str = None) -> dict:
    if path is None:
        path = Path(__file__).parent / "rules.json"
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def validate_dataframe(df: pd.DataFrame, rules_path: str = None):
    """
    Aplica reglas de validación por campo (tipo, regex, obligatoriedad).

    Reglas:
      - Si un campo no cumple tipo o regex → ese campo se borra (None/NULL).
      - Si un campo obligatorio falta o no cumple → descartar fila completa.

    Retorna: (valid_df, invalid_df)
    """
    rules = load_rules(rules_path)

    print("Cargando reglas de validación:")
    for col, r in rules.items():
        print(f"  - {col}: {r}")

    valid_rows = []
    invalid_rows = []

    for idx, row in df.iterrows():
        row_dict = row.to_dict()
        row_invalid = False
        reasons = []

        for field, rule in rules.items():
            value = row_dict.get(field, None)

            # 1) Campo requerido: si está vacío → fila inválida
            if rule.get("required", False) and _is_empty(value):
                reasons.append(f"{field}: requerido pero vacío")
                row_invalid = True
                # No hace falta seguir validando este campo; pero seguimos con otros para log completo
                continue

            # 2) Tipo: si no cumple y NO está vacío → campo a None; si era requerido → fila inválida
            expected_type = rule.get("type", None)
            if expected_type and not _is_empty(value) and not _type_ok(value, expected_type):
                if rule.get("required", False):
                    reasons.append(f"{field}: no cumple tipo ({expected_type})")
                    row_invalid = True
                else:
                    reasons.append(f"{field}: opcional, no cumple tipo → limpiado")
                    row_dict[field] = None  # borrar campo
                # seguimos para evaluar regex (aunque ya queda None si opcional)

            # 3) Regex: si no cumple y NO está vacío → campo a None; si era requerido → fila inválida
            pattern = rule.get("regex", None)
            if pattern and not _is_empty(row_dict.get(field)):
                val = row_dict.get(field)
                if isinstance(val, str):
                    if not re.match(pattern, val):
                        if rule.get("required", False):
                            reasons.append(f"{field}: no cumple regex {pattern}")
                            row_invalid = True
                        else:
                            reasons.append(f"{field}: opcional, no cumple regex → limpiado")
                            row_dict[field] = None
                else:
                    # Si hay regex pero el valor no es string, ya falló tipo antes;
                    # si es requerido, ya marcamos fila inválida; si opcional, lo dejamos None.
                    pass

        if row_invalid:
            invalid_rows.append(row_dict)
            print(f"Fila {idx} rechazada → {', '.join(reasons)}")
        else:
            valid_rows.append(row_dict)

    valid_df = pd.DataFrame(valid_rows)
    invalid_df = pd.DataFrame(invalid_rows)

    print(f"Validados {len(valid_df)} registros; descartados {len(invalid_df)}.")
    return valid_df, invalid_df
