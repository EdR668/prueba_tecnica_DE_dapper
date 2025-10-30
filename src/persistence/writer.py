
from utils.db import DatabaseManager
import pandas as pd
from airflow.exceptions import AirflowException
def insert_regulations_component(db_manager, new_ids):
    """
    Inserta los componentes de las regulaciones.
    """
    if not new_ids:
        return 0, "No new regulation IDs provided"

    try:
        id_rows = pd.DataFrame(new_ids, columns=['regulations_id'])
        id_rows['components_id'] = 7
        
        inserted_count = db_manager.bulk_insert(id_rows, 'regulations_component')
        return inserted_count, f"Successfully inserted {inserted_count} regulation components"
        
    except Exception as e:
        return 0, f"Error inserting regulation components: {str(e)}"



def insert_new_records(db_manager, df, entity):
    """
    Inserta nuevos registros en la base de datos evitando duplicados.
    Conserva la lógica de idempotencia (title|created_at|external_link),
    no muta tipos reales del DF que va a BD y sanea 'NaT'/'NaN'/'' -> None
    antes de insertar para evitar errores de tipos en Postgres.
    """
    regulations_table_name = 'regulations'

    try:
        # 1) Cargar existentes para el entity (incluyendo external_link)
        query = f"""
            SELECT title, created_at, entity, COALESCE(external_link, '') AS external_link
            FROM {regulations_table_name}
            WHERE entity = %s
        """
        existing_records = db_manager.execute_query(query, (entity,))
        if not existing_records:
            db_df = pd.DataFrame(columns=['title', 'created_at', 'entity', 'external_link'])
        else:
            db_df = pd.DataFrame(existing_records, columns=['title', 'created_at', 'entity', 'external_link'])

        print(f"Registros existentes en BD para {entity}: {len(db_df)}")

        # 2) Filtrar el DF por la entidad
        entity_df = df[df['entity'] == entity].copy()
        if entity_df.empty:
            return 0, f"No records found for entity {entity}"

        print(f"Registros a procesar para {entity}: {len(entity_df)}")

        # 3) Normalización para comparación (NO mutar el DF real que irá a BD)
        cmp_entity_df = entity_df.copy()
        cmp_db_df = db_df.copy()

        if not cmp_db_df.empty:
            cmp_db_df['created_at'] = cmp_db_df['created_at'].astype(str)
            cmp_db_df['external_link'] = cmp_db_df['external_link'].fillna('').astype(str)
            cmp_db_df['title'] = cmp_db_df['title'].astype(str).str.strip()

        cmp_entity_df['created_at'] = cmp_entity_df['created_at'].astype(str)
        cmp_entity_df['external_link'] = cmp_entity_df['external_link'].fillna('').astype(str)
        cmp_entity_df['title'] = cmp_entity_df['title'].astype(str).str.strip()

        # 4) Idempotencia con clave única sobre copias de comparación
        if cmp_db_df.empty:
            new_records = entity_df.copy()  # ¡Original, sin cambios de tipo!
            duplicates_found = 0
            print("No hay registros existentes, todos son nuevos")
        else:
            cmp_entity_df['unique_key'] = (
                cmp_entity_df['title'] + '|' +
                cmp_entity_df['created_at'] + '|' +
                cmp_entity_df['external_link']
            )
            cmp_db_df['unique_key'] = (
                cmp_db_df['title'] + '|' +
                cmp_db_df['created_at'] + '|' +
                cmp_db_df['external_link']
            )

            existing_keys = set(cmp_db_df['unique_key'])
            cmp_entity_df['is_duplicate'] = cmp_entity_df['unique_key'].isin(existing_keys)

            keep_idx = cmp_entity_df.index[~cmp_entity_df['is_duplicate']]
            new_records = entity_df.loc[keep_idx].copy()  # Original, sin astype(str)
            duplicates_found = len(entity_df) - len(new_records)

            if duplicates_found > 0:
                print(f"Duplicados encontrados: {duplicates_found}")
                duplicate_records = cmp_entity_df[cmp_entity_df['is_duplicate']]
                print("Ejemplos de duplicados:")
                for _, row in duplicate_records.head(3).iterrows():
                    print(f"  - {row['title'][:50]}... | {row['created_at']}")

        # 5) Duplicados internos (sobre original)
        print(f"Antes de remover duplicados internos: {len(new_records)}")
        new_records = new_records.drop_duplicates(
            subset=['title', 'created_at', 'external_link'],
            keep='first'
        )
        internal_duplicates = len(entity_df) - duplicates_found - len(new_records)
        if internal_duplicates > 0:
            print(f"Duplicados internos removidos: {internal_duplicates}")

        print(f"Después de remover duplicados internos: {len(new_records)}")
        print(f"=== DUPLICADOS IDENTIFICADOS: {duplicates_found + internal_duplicates} ===")

        if new_records.empty:
            return 0, f"No new records found for entity {entity} after duplicate validation"

        # 6) Limpieza estricta de valores antes de insertar (NaT/NaN/'' -> None)
        def _clean_value(v):
            import pandas as pd
            if v is None:
                return None
            # NaN/NaT (pandas)
            if pd.isna(v):
                return None
            if isinstance(v, str) and v.strip() in ("", "NaT", "NaN", "nan"):
                return None
            return v

        new_records = new_records.applymap(_clean_value)

        # created_at compatible con DATE: Timestamp -> date, '' -> None
        if 'created_at' in new_records.columns:
            new_records['created_at'] = new_records['created_at'].apply(
                lambda x: (x.date() if hasattr(x, 'date') else (None if isinstance(x, str) and x.strip() == "" else x))
            )

        print(f"Registros finales a insertar: {len(new_records)}")

        # 7) Insertar nuevos registros
        try:
            print(f"=== INSERTANDO {len(new_records)} REGISTROS ===")
            total_rows_processed = db_manager.bulk_insert(new_records, regulations_table_name)

            if total_rows_processed == 0:
                return 0, f"No records were actually inserted for entity {entity}"

            print(f"Registros insertados exitosamente: {total_rows_processed}")

        except Exception as insert_error:
            print(f"Error en inserción: {insert_error}")
            # Si es error por duplicados, reportar sin romper
            lower_msg = str(insert_error).lower()
            if "duplicate" in lower_msg or "unique" in lower_msg:
                print("Error de duplicados detectado - algunos registros ya existían")
                return 0, f"Some records for entity {entity} were duplicates and skipped"
            else:
                raise insert_error

        # 8) Obtener IDs de recién insertados (método simple)
        print("=== OBTENIENDO IDS DE REGISTROS INSERTADOS ===")
        new_ids_query = f"""
            SELECT id FROM {regulations_table_name}
            WHERE entity = %s
            ORDER BY id DESC
            LIMIT %s
        """
        new_ids_result = db_manager.execute_query(new_ids_query, (entity, total_rows_processed))
        new_ids = [row[0] for row in new_ids_result]
        print(f"IDs obtenidos: {len(new_ids)}")

        # 9) Insertar componentes (si aplica)
        inserted_count_comp = 0
        component_message = ""
        try:
            if new_ids:
                inserted_count_comp, component_message = insert_regulations_component(db_manager, new_ids)
                print(f"Componentes: {component_message}")
        except Exception as comp_error:
            print(f"Error insertando componentes: {comp_error}")
            component_message = f"Error inserting components: {str(comp_error)}"

        # 10) Mensaje final
        total_duplicates = duplicates_found + internal_duplicates
        stats = (
            f"Processed: {len(entity_df)} | "
            f"Existing: {len(db_df)} | "
            f"Duplicates skipped: {total_duplicates} | "
            f"New inserted: {total_rows_processed}"
        )
        message = f"Entity {entity}: {stats}. {component_message}"
        print("=== RESULTADO FINAL ===")
        print(message)
        print("=" * 50)

        return total_rows_processed, message

    except Exception as e:
        if hasattr(db_manager, 'connection') and db_manager.connection:
            db_manager.connection.rollback()
        error_msg = f"Error processing entity {entity}: {str(e)}"
        print(f"ERROR CRÍTICO: {error_msg}")
        import traceback
        print(traceback.format_exc())
        return 0, error_msg





def write_to_db(df: pd.DataFrame):
    """
    Inserta los datos validados en la base de datos usando DatabaseManager e insert_new_records.
    Si ocurre un error, se lanza una excepción para que Airflow marque el task como FAILED.
    """
    from persistence.writer import insert_new_records  # evita import circular
    from utils.db import DatabaseManager

    ENTITY_VALUE = 'Agencia Nacional de Infraestructura'  # puedes parametrizarlo si luego manejas varias entidades

    if df.empty:
        msg = "⚠️ No hay datos válidos para insertar en la base de datos."
        print(msg)
        raise AirflowException(msg)

    db_manager = DatabaseManager()
    inserted_count = 0
    message = ""

    try:
        # 🔌 Conexión
        if not db_manager.connect():
            raise AirflowException("❌ No se pudo conectar a la base de datos.")

        # 💾 Inserción de datos
        inserted_count, message = insert_new_records(db_manager, df, ENTITY_VALUE)

        if inserted_count == 0:
            raise AirflowException(f"❌ No se insertaron registros. Detalle: {message}")

        print(f"✅ Inserción completada: {inserted_count} registros insertados.")
        print(f"🪶 Detalle: {message}")
        return inserted_count

    except Exception as e:
        error_msg = f"💥 Error durante la escritura en DB: {str(e)}"
        print(error_msg)
        raise AirflowException(error_msg)

    finally:
        db_manager.close()

    """
    Wrapper que inserta los datos validados en la base de datos.
    Utiliza DatabaseManager e insert_new_records.
    """
    from persistence.writer import insert_new_records  # evita import circular

    ENTITY_VALUE = 'Agencia Nacional de Infraestructura'  # o el valor que uses en el scraper

    if df.empty:
        print("⚠️ No hay datos válidos para insertar en la base de datos.")
        return

    db_manager = DatabaseManager()
    try:
        if not db_manager.connect():
            print("❌ No se pudo conectar a la base de datos.")
            return
        
        inserted_count, message = insert_new_records(db_manager, df, ENTITY_VALUE)
        print(f"✅ Inserción completada: {inserted_count} registros insertados.")
        print(f"🪶 Detalle: {message}")

    except Exception as e:
        print(f"❌ Error durante la escritura en DB: {str(e)}")
    finally:
        db_manager.close()
