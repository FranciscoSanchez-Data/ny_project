from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as _sum, avg, round as spark_round, when
)
from pathlib import Path
import shutil

# --- RUTAS ---
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
SILVER_DIR = PROJECT_ROOT / "data" / "silver" / "yellow_taxi_trips"
GOLD_DIR = PROJECT_ROOT / "data" / "gold" / "gold_daily_revenue"

def get_spark_session():
    return (SparkSession.builder
            .appName("NYC_Mobility_Gold_Marts")
            .config("spark.driver.memory", "4g")
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())

def main():
    print("🚀 Iniciando construcción de Data Marts (Capa Gold)...")
    
    if GOLD_DIR.exists():
        print("🧹 Limpiando directorio Gold previo...")
        shutil.rmtree(GOLD_DIR)
        
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        print("📖 Leyendo tabla Delta desde la capa Silver...")
        df_silver = spark.read.format("delta").load(str(SILVER_DIR))
        
        # 1. REGLA DE ORO: Para métricas de negocio, usamos SOLO viajes válidos
        print("⚙️ Filtrando anomalías y agregando métricas de negocio...")
        df_valid = df_silver.filter(col("is_valid_trip") == True)
        
        # 2. Agrupación y cálculo de KPIs (Data Mart: gold_daily_revenue)
        # Diccionario oficial TLC: payment_type 1 = Tarjeta, 2 = Efectivo
        df_gold_daily = df_valid.groupBy("pickup_date").agg(
            count("*").alias("total_trips"),
            spark_round(_sum("total_amount"), 2).alias("total_revenue"),
            spark_round(avg("fare_amount"), 2).alias("avg_fare"),
            spark_round(avg("tip_amount"), 2).alias("avg_tip"),
            spark_round(avg("trip_distance"), 2).alias("avg_trip_distance"),
            spark_round(avg("trip_duration_minutes"), 2).alias("avg_trip_duration_minutes"),
            
            # % Pagos con Tarjeta
            spark_round(
                (_sum(when(col("payment_type") == 1, 1).otherwise(0)) / count("*")) * 100, 2
            ).alias("card_payment_share_pct"),
            
            # % Pagos en Efectivo
            spark_round(
                (_sum(when(col("payment_type") == 2, 1).otherwise(0)) / count("*")) * 100, 2
            ).alias("cash_payment_share_pct")
        ).orderBy("pickup_date") # Ordenamos cronológicamente para que sea bonito de leer
        
        print(f"💾 Escribiendo Data Mart en formato Delta en: {GOLD_DIR}")
        (df_gold_daily.write
         .format("delta")
         .mode("overwrite")
         .save(str(GOLD_DIR)))
        
        # 3. Mostrar resultados en consola para validar
        print("\n📈 PREVIEW DEL DATA MART (gold_daily_revenue):")
        df_gold_daily.show(10, truncate=False)
        
        print("✅ Capa Gold construida con éxito. ¡MVP técnico completado!")
        
    except Exception as e:
        print(f"❌ Error durante la construcción Gold: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()