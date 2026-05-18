# 🛠️ Retos Técnicos y Decisiones de Arquitectura (`CHALLENGES.md`)

Durante el desarrollo de este Lakehouse end-to-end, se presentaron múltiples desafíos de infraestructura, calidad de datos crudos y compatibilidad de versiones. A continuación, se detalla la bitácora de problemas encontrados y las soluciones de ingeniería adoptadas para resolverlos.

---

### 0. Limitaciones del Entorno de Desarrollo (Migración Forzada a WSL2)
* **Desafío:** El diseño original del stack tecnológico contemplaba la ejecución nativa en Windows 11. Sin embargo, durante la fase de inicialización surgieron errores críticos sistémicos:
  1. **Apache Spark:** Conflictos insalvables con los binarios de Hadoop (`winutils.exe`), permisos de ejecución de Java y variables de entorno del sistema operativo.
  2. **Apache Airflow:** Incompatibilidad nativa absoluta con sistemas Windows debido a la fuerte dependencia del motor con las librerías del kernel de Linux para la gestión de procesos hijos y bifurcaciones (*forking*).
* **Solución:** Se tomó la decisión estratégica de **emigrar todo el entorno de desarrollo a WSL2 (Ubuntu)**. Esto permitió emular un entorno de producción real basado en Linux, garantizando el aislamiento de dependencias mediante un entorno virtual (`venv`) y asegurando la compatibilidad nativa de Airflow, PySpark y dbt sin necesidad de parches inestables en el sistema anfitrión.

---

### 1. Datos Corruptos y Anomalías en el Mundo Físico
* **Desafío:** Tras consolidar la capa **Bronze**, la analítica inicial reveló incoherencias físicas severas en los registros de los taxímetros (viajes con distancias de 0 millas pero tarifas de $100, o duraciones de viaje negativas debido a desajustes en los relojes de los vehículos).
* **Impacto:** Incluir estos registros sesgaba drásticamente la tarifa promedio y la velocidad media de la flota, arruinando los reportes de negocio en la capa Gold.
* **Solución:** En la capa **Silver** (`silver_transformations.py`), se diseñó un filtro de calidad distribuido con PySpark. En lugar de borrar los datos de forma destructiva, se optó por una estrategia de negocio: añadir la columna booleana `is_valid_trip`. Esto permitió aislar un **3.4% de registros anómalos (319,037 filas)**, manteniéndolos disponibles para auditoría técnica pero protegiendo las métricas financieras de la capa Gold.

---

### 2. El Bug del "Viaje en el Tiempo" (Contaminación de Particiones)
* **Desafío:** Al generar el primer reporte diario en la capa Gold, aparecieron viajes con fechas de recogida en los años **2001, 2005 y 2008**, a pesar de estar procesando exclusivamente el dataset oficial del primer trimestre de 2023.
* **Causa Raíz:** Los dispositivos de telemetría de algunos taxis sufrieron reinicios de fábrica (Epoch reset), guardando datos reales de 2023 con marcas de tiempo erróneas en el pasado. Al particionar los datos en PySpark por la carpeta del año de ingesta, la basura quedó oculta dentro de los archivos de 2023.
* **Solución:** Se migró el filtrado temporal estricto a la capa **Gold analítica en dbt** (`gold_daily_revenue.sql`). En lugar de confiar ciegamente en la estructura de directorios de la partición, se aplicó un filtro SQL nativo con DuckDB: `EXTRACT(YEAR FROM pickup_date) = 2023`. Esto limpió los gráficos de tendencias temporales por completo en el dashboard final.

---

### 3. Fricción por Cambios Rompedores en Versiones "Bleeding-Edge" (Airflow 3.x)
* **Desafío:** Al instalar Apache Airflow de forma nativa mediante `pip`, el gestor instaló la versión mayor más reciente (**Airflow 3.x**). Al lanzar el DAG inicial, el planificador falló inmediatamente con un error de tipo `TypeError: DAG.__init__() got an unexpected keyword argument 'schedule_interval'`.
* **Causa Raíz:** La nueva versión de Airflow eliminó por completo parámetros clásicos de la versión 2.x (como `schedule_interval` y el binario tradicional `webserver`) a favor de una arquitectura más limpia orientada a APIs (`api-server`) y el parámetro simplificado `schedule`.
* **Solución:** Se adaptó el código del DAG a los estándares de la nueva versión reemplazando la directiva a `schedule='@monthly'`. Asimismo, para simplificar el despliegue local en WSL2, se utilizó el comando unificado `airflow standalone`, el cual automatiza las migraciones de la base de datos interna y gestiona los demonios de control de forma segura.

---

### 4. Errores de Entorno de Ejecución y Caracteres Invisibles (Exit Code 2 en Bash)
* **Desafío:** La tarea de Airflow `transform_to_silver` fallaba sistemáticamente devolviendo un código de salida `exit code 2` y un mensaje de error indicando que el archivo no existía, a pesar de que el script se ejecutaba correctamente al lanzarlo de forma manual.
* **Causa Raíz:** Una discrepancia en la nomenclatura (singular vs. plural) entre la llamada del `BashOperator` y el sistema de archivos, sumado a la creación accidental de un archivo duplicado con un punto al final (`silver_transformations.py.`) debido a un error de tipeo en la consola, provocaba que el sistema operativo bloqueara la ruta de ejecución.
* **Solución:** Se implementaron comandos de auditoría en la terminal de Linux (`ls` y `find`) para inspeccionar el directorio físico. Se purgó el archivo corrupto con `rm` y se alineó la configuración de las tareas del DAG en Airflow garantizando coincidencia exacta de rutas absolutas mediante el parámetro `cwd` (Current Working Directory).