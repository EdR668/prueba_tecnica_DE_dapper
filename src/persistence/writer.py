
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
    Optimizada para velocidad y precisión.
    """
    regulations_table_name = 'regulations'
    
    try:
        # 1. OBTENER REGISTROS EXISTENTES INCLUYENDO EXTERNAL_LINK
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
        
        print(f"Registros existentes en BD para {entity}: {len(db_df)}")
        
        # 2. PREPARAR DATAFRAME DE LA ENTIDAD
        entity_df = df[df['entity'] == entity].copy()
        
        if entity_df.empty:
            return 0, f"No records found for entity {entity}"
        
        print(f"Registros a procesar para {entity}: {len(entity_df)}")
        
        # 3. NORMALIZAR DATOS PARA COMPARACIÓN CONSISTENTE
        # Normalizar created_at a string
        if not db_df.empty:
            db_df['created_at'] = db_df['created_at'].astype(str)
            db_df['external_link'] = db_df['external_link'].fillna('').astype(str)
            db_df['title'] = db_df['title'].astype(str).str.strip()
        
        entity_df['created_at'] = entity_df['created_at'].astype(str)
        entity_df['external_link'] = entity_df['external_link'].fillna('').astype(str)
        entity_df['title'] = entity_df['title'].astype(str).str.strip()
        
        # 4. IDENTIFICAR DUPLICADOS DE MANERA OPTIMIZADA
        print("=== INICIANDO VALIDACIÓN DE DUPLICADOS OPTIMIZADA ===")
        
        if db_df.empty:
            # Si no hay registros existentes, todos son nuevos
            new_records = entity_df.copy()
            duplicates_found = 0
            print("No hay registros existentes, todos son nuevos")
        else:
            # Crear claves únicas para comparación super rápida
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
            
            # Usar set para comparación O(1) - súper rápido
            existing_keys = set(db_df['unique_key'])
            entity_df['is_duplicate'] = entity_df['unique_key'].isin(existing_keys)
            
            new_records = entity_df[~entity_df['is_duplicate']].copy()
            duplicates_found = len(entity_df) - len(new_records)
            
            # Log para debugging
            if duplicates_found > 0:
                print(f"Duplicados encontrados: {duplicates_found}")
                duplicate_records = entity_df[entity_df['is_duplicate']]
                print("Ejemplos de duplicados:")
                for idx, row in duplicate_records.head(3).iterrows():
                    print(f"  - {row['title'][:50]}... | {row['created_at']}")
        
        # 5. REMOVER DUPLICADOS INTERNOS DEL DATAFRAME
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
        
        # 6. LIMPIAR DATAFRAME ANTES DE INSERTAR
        # Remover columnas auxiliares
        columns_to_drop = ['unique_key', 'is_duplicate']
        for col in columns_to_drop:
            if col in new_records.columns:
                new_records = new_records.drop(columns=[col])
        
        print(f"Registros finales a insertar: {len(new_records)}")
        
        # 7. INSERTAR NUEVOS REGISTROS
        try:
            print(f"=== INSERTANDO {len(new_records)} REGISTROS ===")
            
            total_rows_processed = db_manager.bulk_insert(new_records, regulations_table_name)
            
            if total_rows_processed == 0:
                return 0, f"No records were actually inserted for entity {entity}"
            
            print(f"Registros insertados exitosamente: {total_rows_processed}")
            
        except Exception as insert_error:
            print(f"Error en inserción: {insert_error}")
            # Si es error de duplicados, algunos se escaparon
            if "duplicate" in str(insert_error).lower() or "unique" in str(insert_error).lower():
                print("Error de duplicados detectado - algunos registros ya existían")
                return 0, f"Some records for entity {entity} were duplicates and skipped"
            else:
                raise insert_error
        
        # 8. OBTENER IDS DE REGISTROS INSERTADOS - MÉTODO OPTIMIZADO
        print("=== OBTENIENDO IDS DE REGISTROS INSERTADOS ===")
        
        # Método simple y eficiente - obtener los últimos N IDs
        new_ids_query = f"""
            SELECT id FROM {regulations_table_name}
            WHERE entity = %s 
            ORDER BY id DESC
            LIMIT %s
        """
        
        new_ids_result = db_manager.execute_query(
            new_ids_query, 
            (entity, total_rows_processed)
        )
        new_ids = [row[0] for row in new_ids_result]
        
        print(f"IDs obtenidos: {len(new_ids)}")
        
        # 9. INSERTAR COMPONENTES DE REGULACIÓN
        inserted_count_comp = 0
        component_message = ""
        
        if new_ids:
            try:
                inserted_count_comp, component_message = insert_regulations_component(db_manager, new_ids)
                print(f"Componentes: {component_message}")
            except Exception as comp_error:
                print(f"Error insertando componentes: {comp_error}")
                component_message = f"Error inserting components: {str(comp_error)}"
        
        # 10. MENSAJE FINAL CON ESTADÍSTICAS DETALLADAS
        total_duplicates = duplicates_found + internal_duplicates
        stats = (
            f"Processed: {len(entity_df)} | "
            f"Existing: {len(db_df)} | "
            f"Duplicates skipped: {total_duplicates} | "
            f"New inserted: {total_rows_processed}"
        )
        
        message = f"Entity {entity}: {stats}. {component_message}"
        print(f"=== RESULTADO FINAL ===")
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
