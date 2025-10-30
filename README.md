# Dapper ETL con Airflow

## Descripción
Proyecto que refactoriza una Lambda de scraping para ejecutarse como flujo ETL en Airflow:  
Extracción → Validación → Escritura.  
El flujo se ejecuta con tres tareas en un DAG, usando la base de datos Postgres incluida en el entorno.

---

## Requisitos
- Docker y Docker Compose instalados  
- Make instalado en el sistema  
- Puerto 8080 libre

---

## Levantar el entorno

Ejecuta todo el entorno con:

make start

Este comando:
1. Limpia contenedores y logs previos  
2. Inicializa Airflow y crea el usuario admin  
3. Levanta los servicios (Postgres, Scheduler y Webserver)

---

## Acceso a Airflow

Interfaz: http://localhost:8080  
Usuario: admin  
Contraseña: admin

---

## Ejecutar el DAG

1. Entra a la interfaz web de Airflow  
2. Activa el DAG dapper_pipeline  
3. Ejecuta manualmente el flujo o espera la ejecución diaria (@daily)

---

## Flujo del DAG
- extract: realiza el scraping (sin cambios a la lógica original)  
- validate: aplica las reglas del archivo rules.json  
- write: inserta registros validados en la tabla regulations (evita duplicados)

---

## Tablas
Definidas en schema.sql.  
La base se inicializa automáticamente al levantar el contenedor de Postgres.

---

## Estructura
src/
 ├─ extraction/      # Scraper
 ├─ validation/      # Validación con reglas
 ├─ persistence/     # Escritura e idempotencia
 └─ utils/           # Manejo de conexión DB
dags/
 └─ dapper_pipeline_dag.py

---

## Variables de entorno
Archivo .env en la raíz del proyecto:
PYTHONPATH=/opt/airflow/src
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow

---

## Comandos útiles
make up-airflow       # Levanta contenedores ya configurados
make down-airflow     # Detiene y borra contenedores
make reset-airflow    # Limpia logs y dags
make init-airflow     # Inicializa la DB y usuario admin
