from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, unix_timestamp, round as spark_round, when
from pathlib import Path
import shutil

# --- RUTAS ---
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze" / "yellow_taxi_trips"
SILVER_DIR = PROJECT_ROOT / "data" / "silver" / "yellow_taxi_trips"

def get_spark_session():
    return (SparkSession.builder
            .appName("NYC_Mobility_Silver_Transform")
            .config("spark.driver.memory", "4g")
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())

def main():
    print("🚀 Iniciando transformaciones de la capa Silver...")
    
    if SILVER_DIR.exists():
        print("🧹 Limpiando directorio Silver previo...")
        shutil.rmtree(SILVER_DIR)
        
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        print("📖 Leyendo tabla Delta desde la capa Bronze...")
        df_bronze = spark.read.format("delta").load(str(BRONZE_DIR))
        
        print("⚙️ Aplicando transformaciones y limpieza...")
        
        # 1. Estandarización de nombres
        df_silver = (
            df_bronze
            .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
            .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
            .withColumnRenamed("PULocationID", "pickup_location_id")
            .withColumnRenamed("DOLocationID", "dropoff_location_id")
        )
        
        # 2. Creación de columnas derivadas
        df_silver = (
            df_silver
            .withColumn("pickup_date", to_date(col("pickup_datetime")))
            .withColumn(
                "trip_duration_minutes",
                spark_round(
                    (unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))) / 60,
                    2
                )
            )
        )
        
        # 3. Reglas de validación
        df_silver = df_silver.withColumn(
            "is_valid_trip",
            when(
                (col("pickup_datetime").isNotNull()) &
                (col("dropoff_datetime").isNotNull()) &
                (col("dropoff_datetime") > col("pickup_datetime")) &
                (col("trip_distance") >= 0) &
                (col("fare_amount") >= 0) &
                (col("total_amount") >= 0) &
                (col("passenger_count") >= 0) &
                (col("pickup_location_id").isNotNull()) &
                (col("dropoff_location_id").isNotNull()),
                True
            ).otherwise(False)
        )
        
        # 4. Reporte rápido de calidad
        total_rows = df_silver.count()
        valid_rows = df_silver.filter(col("is_valid_trip") == True).count()
        invalid_rows = total_rows - valid_rows

        invalid_rate = round((invalid_rows / total_rows) * 100, 2) if total_rows > 0 else 0
        
        print("📊 Reporte rápido de calidad:")
        print(f"   - Filas totales: {total_rows:,}")
        print(f"   - Viajes válidos: {valid_rows:,}")
        print(f"   - Viajes anómalos: {invalid_rows:,} ({invalid_rate}%)")
        
        print(f"💾 Escribiendo en formato Delta en: {SILVER_DIR}")
        
        (
            df_silver.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("year", "month")
            .save(str(SILVER_DIR))
        )
        
        print("✅ Transformación a Silver completada con éxito.")
        
    except Exception as e:
        print(f"❌ Error durante la transformación Silver: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()