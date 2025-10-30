# Prueba Técnica – Dapper ETL con Airflow

## Descripción general

Este proyecto refactoriza una Lambda Function de scraping para ejecutar un flujo ETL orquestado con Apache Airflow, siguiendo la secuencia:

Extracción → Validación → Escritura

El objetivo es mantener intacta la lógica original del scraping, incorporar validación configurable por reglas y escribir los datos validados en la base de datos Postgres incluida en el entorno de Airflow.

---

## Estructura del proyecto

```
├── dags/
│   └── dapper_pipeline_dag.py        # DAG principal con las tres tareas (extract, validate, write)
│
├── src/
│   ├── extraction/
│   │   └── scraper.py                # Lógica de scraping (sin modificaciones en la lógica original)
│   ├── validation/
│   │   ├── validator.py              # Validación de datos según reglas configurables
│   │   └── rules.json                # Reglas (regex, tipos, obligatoriedad)
│   ├── persistence/
│   │   └── writer.py                 # Inserción en la base de datos + idempotencia
│   └── utils/
│       └── db.py                     # Clase DatabaseManager (manejo de conexión PostgreSQL)
│
├── schema.sql                        # Tablas destino (DDL)
├── docker-compose.yml                # Entorno de Airflow con Postgres
├── Makefile                          # Comandos para inicializar y ejecutar Airflow
├── .env                              # Variables de entorno (PYTHONPATH, etc.)
└── README.md                         # Este documento
```

---

## Requisitos previos

- Docker y Docker Compose instalados.
- Make disponible en tu sistema (usado para automatizar comandos).
- Puerto 8080 libre (interfaz web de Airflow).
- Sistema compatible con UNIX/macOS/Linux o WSL en Windows.

---

## Inicialización del entorno

Todo el ciclo de configuración se maneja con el Makefile.

### Paso único: levantar Airflow desde cero
```bash
make start
```

Esto ejecuta en orden:
1. make down-airflow → detiene y limpia contenedores previos.  
2. make reset-airflow → limpia logs, dags y plugins.
3. make init-airflow → inicializa la base de datos y crea el usuario admin.
4. make up-airflow → levanta los servicios (Postgres, Scheduler y Webserver).

---

## Acceso a la interfaz de Airflow

Una vez levantado el entorno:
- Accede a http://localhost:8080
- Usuario: admin
- Contraseña: admin

---

## Estructura del DAG

DAG ID: dapper_pipeline

| Tarea       | Descripción |
|--------------|-------------|
| extract    | Ejecuta scrape_page() — obtiene las normas desde la web y genera un DataFrame. |
| validate   | Ejecuta validate_dataframe() — aplica las reglas del archivo rules.json. |
| write      | Ejecuta write_to_db() — inserta registros en Postgres, evitando duplicados. |

Flujo:

extract → validate → write

---

## Base de datos

### Esquema (schema.sql)

```sql
CREATE TABLE IF NOT EXISTS regulations (
    id SERIAL PRIMARY KEY,
    title TEXT,
    summary TEXT,
    created_at DATE,
    entity TEXT,
    external_link TEXT,
    classification_id INT,
    rtype_id INT,
    is_active BOOLEAN,
    update_at TIMESTAMP,
    gtype TEXT
);

CREATE TABLE IF NOT EXISTS regulations_component (
    id SERIAL PRIMARY KEY,
    regulations_id INT NOT NULL REFERENCES regulations(id) ON DELETE CASCADE,
    components_id INT NOT NULL
);
```

Estas tablas se crean automáticamente al iniciar el contenedor Postgres si schema.sql está montado.

---

## Validación de datos

Las reglas se definen en src/validation/rules.json:

```json
{
  "title": {
    "type": "string",
    "regex": "^[A-ZÁÉÍÓÚÑ0-9\s\-_,.]+$",
    "required": true
  },
  "created_at": {
    "type": "date",
    "regex": "^\d{4}-\d{2}-\d{2}$",
    "required": true
  },
  "external_link": {
    "type": "string",
    "regex": "^https?://",
    "required": true
  },
  "summary": {
    "type": "string",
    "regex": ".*",
    "required": false
  }
}
```

### Reglas:
- Campos no válidos se dejan vacíos.
- Campos obligatorios inválidos descartan toda la fila.
- El título (title) solo acepta mayúsculas y caracteres válidos según regex.

---

## Idempotencia

Se conserva la lógica original de la Lambda:  
antes de insertar, se consultan los registros existentes (title, created_at, external_link) para evitar duplicados.

---

## Logs esperados

Ejemplo de salida en Airflow:

```
Extracción completa: 30 registros.
Validación completa: 25 válidos, 5 descartados.
Escritura completada: 25 registros insertados.
Detalle: Processed: 30 | Existing: 100 | Duplicates skipped: 5 | New inserted: 25
```

---

## Comandos útiles

| Comando | Descripción |
|----------|-------------|
| make start | Limpia, inicializa y levanta Airflow completo |
| make up-airflow | Levanta los contenedores ya inicializados |
| make down-airflow | Detiene y elimina contenedores |
| make reset-airflow | Limpia logs y dags locales |
| make init-airflow | Re-inicializa la base de datos y crea el usuario admin |

---

## Variables de entorno

.env
```
PYTHONPATH=/opt/airflow/src
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
```

---

## Criterios cumplidos

| Criterio | Implementación |
|-----------|----------------|
| Correctitud | Lógica original del scraper intacta. Validación regex + tipos configurables. |
| Diseño modular | Módulos: extraction, validation, persistence, utils. |
| Operabilidad | Makefile, .env, docker-compose.yml y README autoexplicativos. |
| Calidad | Manejo robusto de errores y logs detallados por etapa. |
| Idempotencia | Inserciones únicas basadas en clave compuesta (title + created_at + link). |

---

## Autor

Desarrollado por: Eder José Hernández Buelvas
Contacto: ederhernandez667@gmail.com
Fecha: Octubre 2025  
Organización: Dapper – Prueba Técnica
