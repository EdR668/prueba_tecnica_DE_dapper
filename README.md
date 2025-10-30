# Prueba Técnica – ETL con Airflow

## Descripción
Este proyecto refactoriza una Lambda en un pipeline ETL con tres etapas orquestadas en **Apache Airflow**:
1. **Extracción:** obtiene datos mediante scraping.
2. **Validación:** limpia y valida los datos según reglas configurables.
3. **Escritura:** inserta los datos en una base de datos PostgreSQL.

El objetivo es mantener la lógica original del scraping, agregar validación configurable e implementar idempotencia básica (evitar duplicados).

---


## Levantar el entorno

### Requisitos
- Docker y Docker Compose instalados.
- GNU Make instalado (`choco install make` en Windows o `brew install make` en macOS).

---

### 🧩 Archivo `.env`

Crea un archivo llamado `.env` en la **raíz del proyecto** con las variables necesarias para ejecutar el proyecto.

Ejemplo:

```bash
# Variables de entorno
PYTHONPATH=/opt/airflow/src
```

### Instalación completa
```bash
make install
```
Esto:
1. Detiene cualquier contenedor anterior.
2. Reconstruye las imágenes.
3. Inicializa Airflow y crea el usuario `admin / admin`.


---

### Acceso
- Interfaz de Airflow: [http://localhost:8080](http://localhost:8080)  
  Usuario: `admin`  
  Contraseña: `admin`

---

### Ejecución del DAG
1. En la interfaz de Airflow, activa el DAG `dapper_pipeline`.
2. Haz clic en **Trigger DAG** para ejecutarlo.
3. Observa los logs de cada tarea (extract, validate, write).

---


## Logs y resultados
Los logs se almacenan en `./logs/` e incluyen:
- Total de registros extraídos.
- Filas descartadas por validación.
- Filas insertadas en base de datos.

---

## Reglas de validación (`rules.json`)
El archivo `src/validation/rules.json` define las reglas que se aplican a cada campo del DataFrame antes de insertarlo.

Ejemplo:
```json
{
  "title": {
    "type": "string",
    "regex": "^[A-ZÁÉÍÓÚÑ0-9\\s\\-_,.]+$",
    "required": true
  },
  "created_at": {
    "type": "date",
    "regex": "^\\d{4}-\\d{2}-\\d{2}$",
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

### Cómo funciona
- Si un campo **obligatorio (`required: true`)** no cumple su tipo o regex, la fila se **descarta**.
- Si un campo **no obligatorio** no cumple, el valor se reemplaza por `NULL`.
- Las reglas pueden modificarse sin cambiar el código: basta con editar `rules.json`.

---


### Deshabilitar el sistema:
```bash
make down-airflow
```
---
Nota: La proxima vez que se vaya a utilizar el sistema, es recomendable usar el siguiente comando:
### Levantar sin reconstruir
```bash
make up-airflow
```

## Estructura principal
```
/dags
 └── dapper_pipeline_dag.py     # DAG principal de Airflow
/src
 ├── extraction/scraper.py      # Módulo de extracción
 ├── validation/validator.py    # Módulo de validación
 ├── validation/rules.json      # Reglas configurables de validación
 └── persistence/writer.py      # Módulo de escritura
/configs
 └── schema.sql                 # Esquema base de la tabla destino
```

---

## Comandos Make disponibles
| Comando | Descripción |
|----------|--------------|
| `make install` | Instala, construye e inicializa Airflow. |
| `make up-airflow` | Levanta el entorno sin reconstruir. |
| `make init-airflow` | Inicializa Airflow y crea usuario admin. |
| `make down-airflow` | Detiene y elimina los contenedores. |
| `make build-airflow` | Reconstruye las imágenes y levanta los servicios. |

---

## Contacto
Proyecto desarrollado como parte de una prueba técnica para Dapper.

Eder Hernández Buelvas
ederhernandez667@gmail.com
