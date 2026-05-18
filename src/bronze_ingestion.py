from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, input_file_name, regexp_extract
from pyspark.sql.types import IntegerType, LongType, FloatType # <-- NUEVAS IMPORTACIONES
from pathlib import Path
import uuid
import glob
import shutil

# --- RUTAS ---
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
LANDING_DIR = PROJECT_ROOT / "data" / "landing"
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze" / "yellow_taxi_trips"

def get_spark_session():
    return (SparkSession.builder
            .appName("NYC_Mobility_Bronze_Ingestion")
            .config("spark.driver.memory", "4g")
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.parquet.enableVectorizedReader", "false")
            .getOrCreate())

def main():
    print("🚀 Iniciando proceso de ingesta iterativa a la capa Bronze...")
    
    if BRONZE_DIR.exists():
        print("🧹 Limpiando directorio Bronze previo...")
        shutil.rmtree(BRONZE_DIR)
        
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    archivos = glob.glob(str(LANDING_DIR / "yellow_tripdata_2023-*.parquet"))
    
    if not archivos:
        print("❌ No se encontraron archivos en la Landing Zone.")
        return

    filas_totales = 0
    
    try:
        for archivo in archivos:
            print(f"\n📖 Leyendo archivo: {Path(archivo).name}")
            df_raw = spark.read.parquet(archivo)
            
            # --- EL PARCHE SENIOR V2: BLINDAJE CONTRA PANDAS ---
            # Si vemos cualquier tipo de dato numérico fluctuante, lo forzamos a Double
            for field in df_raw.schema.fields:
                if isinstance(field.dataType, (IntegerType, LongType, FloatType)):
                    df_raw = df_raw.withColumn(field.name, col(field.name).cast("double"))
            # ---------------------------------------------------
            
            batch_id = str(uuid.uuid4())
            
            df_bronze = (df_raw
                         .withColumn("source_file", input_file_name())
                         .withColumn("ingestion_timestamp", current_timestamp())
                         .withColumn("service_type", lit("yellow"))
                         .withColumn("year", regexp_extract(col("source_file"), r"_(\d{4})-(\d{2})\.parquet", 1))
                         .withColumn("month", regexp_extract(col("source_file"), r"_(\d{4})-(\d{2})\.parquet", 2))
                         .withColumn("batch_id", lit(batch_id))
                         )
            
            conteo = df_bronze.count()
            filas_totales += conteo
            print(f"⚙️ Procesando {conteo:,} filas...")
            
            (df_bronze.write
             .format("delta")
             .mode("append")
             .option("mergeSchema", "true") 
             .save(str(BRONZE_DIR)))
            
            print(f"✅ Archivo ingerido correctamente en Bronze.")
            
        print(f"\n🎉 Ingesta TOTAL completada. Filas totales procesadas: {filas_totales:,}")
        
    except Exception as e:
        print(f"❌ Error crítico durante la ingesta: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()