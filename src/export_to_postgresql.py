import duckdb
import pandas as pd
from sqlalchemy import create_engine
import subprocess
from pathlib import Path

# 1. Configuración de rutas
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "dbt" / "local_lakehouse.duckdb"

def get_windows_ip():
    """Obtiene dinámicamente la IP del host de Windows desde WSL2"""
    try:
        # Ejecutamos un comando de Linux para ver la IP de la puerta de enlace (Windows)
        cmd = "ip route show | grep default | awk '{print $3}'"
        ip = subprocess.check_output(cmd, shell=True).decode('utf-8').strip()
        return ip
    except Exception as e:
        print(f"⚠️ No se pudo detectar la IP de Windows, usando localhost: {e}")
        return "127.0.0.1"

def export_gold_to_postgresql():
    win_ip = get_windows_ip()
    print(f"🔌 Conectando a PostgreSQL en Windows a través de la IP: {win_ip}...")
    
    # =========================================================================
    # 📝 CONFIGURACIÓN DE CREDENCIALES
    # =========================================================================
    USER = "postgres"
    PASSWORD = "root"  # 👈 ¡Pon aquí la contraseña que elegiste en la instalación!
    DB_NAME = "postgres"            # Usamos la base de datos por defecto que crea Postgres
    PORT = "5432"
    # =========================================================================
    
    # 2. Leer los datos desde nuestro DuckDB local
    if not DB_PATH.exists():
        raise FileNotFoundError(f"❌ No se encuentra la base de datos DuckDB en {DB_PATH}")
        
    print("📖 Leyendo la capa Gold desde DuckDB...")
    duck_conn = duckdb.connect(str(DB_PATH), read_only=True)
    df = duck_conn.execute("SELECT * FROM gold_daily_revenue").df()
    duck_conn.close()
    
    # 3. Conectar y escribir en PostgreSQL en Windows usando SQLAlchemy y Psycopg2
    connection_string = f"postgresql+psycopg2://{USER}:{PASSWORD}@{win_ip}:{PORT}/{DB_NAME}"
    engine = create_engine(connection_string)
    
    print(f"🚀 Insertando {len(df)} filas en la tabla 'gold_daily_revenue' de PostgreSQL...")
    # if_exists='replace' recrea la tabla si ya existe para actualizar los datos limpios
    df.to_sql(name='gold_daily_revenue', con=engine, if_exists='replace', index=False)
    
    print("✅ ¡Exportación a la capa de servicio de PostgreSQL completada con éxito!")

if __name__ == "__main__":
    export_gold_to_postgresql()