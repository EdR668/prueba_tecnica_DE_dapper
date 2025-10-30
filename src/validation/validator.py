import json, re, pandas as pd

def load_rules(path="src/validation/rules.json"):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

import re
import json

def validate_dataframe(df):
    """
    Valida el dataframe según las reglas en rules.json.
    Retorna dos dataframes: (válidos, inválidos)
    """
    import pandas as pd
    from pathlib import Path

    rules_path = Path(__file__).parent / "rules.json"
    with open(rules_path, "r") as f:
        rules = json.load(f)

    print("🧾 Cargando reglas de validación:")
    for col, r in rules.items():
        print(f"  - {col}: {r}")

    valid_rows = []
    invalid_rows = []

    for idx, row in df.iterrows():
        row_valid = True
        reasons = []  # para guardar por qué se rechaza

        for field, rule in rules.items():
            value = row.get(field)

            # Si es obligatorio pero no tiene valor
            if rule.get("required") and (pd.isna(value) or value == ""):
                reasons.append(f"{field}: requerido pero vacío")
                row_valid = False
                continue

            # Si hay un tipo esperado
            expected_type = rule.get("type")
            if expected_type and not pd.isna(value):
                if expected_type == "string" and not isinstance(value, str):
                    reasons.append(f"{field}: esperaba string, obtuvo {type(value).__name__}")
                    row_valid = False
                elif expected_type == "int" and not isinstance(value, int):
                    reasons.append(f"{field}: esperaba int, obtuvo {type(value).__name__}")

            # Si hay un patrón regex
            pattern = rule.get("regex")
            if pattern and isinstance(value, str):
                if not re.match(pattern, value):
                    reasons.append(f"{field}: no cumple regex {pattern}")
                    row_valid = False

        if row_valid:
            valid_rows.append(row)
        else:
            invalid_rows.append(row)
            print(f"❌ Fila {idx} rechazada → {', '.join(reasons)}")

    valid_df = pd.DataFrame(valid_rows)
    invalid_df = pd.DataFrame(invalid_rows)

    print(f"✅ Validados {len(valid_df)} registros; ❌ descartados {len(invalid_df)}.")
    return valid_df, invalid_df
