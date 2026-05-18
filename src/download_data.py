import requests
from pathlib import Path

# --- RUTAS ABSOLUTAS DINÁMICAS ---
# __file__ apunta a download_data.py. Hacemos .parent para subir a 'src' y otro .parent para la raíz del proyecto.
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DOWNLOAD_DIR = PROJECT_ROOT / "data" / "landing"

# --- CONFIGURACIÓN ---
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
YEAR = "2023"
MONTHS = ["01", "02", "03"]
TAXI_TYPE = "yellow"

def download_parquet(taxi_type: str, year: str, month: str, output_dir: Path):
    file_name = f"{taxi_type}_tripdata_{year}-{month}.parquet"
    url = f"{BASE_URL}/{file_name}"
    output_path = output_dir / file_name

    # Idempotencia mejorada: verificamos que exista Y que no esté vacío
    if output_path.exists() and output_path.stat().st_size > 1000:
        print(f"✅ El archivo {file_name} ya existe y parece válido en {output_dir}. Saltando.")
        return
    elif output_path.exists():
        print(f"⚠️ El archivo {file_name} existe pero parece corrupto/vacío. Se volverá a descargar.")

    print(f"⬇️ Descargando {file_name} desde {url}...")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status() 
        
        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"🚀 Descarga completada: {output_path}")
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Error al descargar {file_name}: {e}")

def main():
    print(f"🔍 Ruta del proyecto detectada: {PROJECT_ROOT}")
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    
    print("--- Iniciando descarga de datos (MVP - Phase 1) ---")
    for month in MONTHS:
        download_parquet(TAXI_TYPE, YEAR, month, DOWNLOAD_DIR)
    print("--- Proceso finalizado ---")

if __name__ == "__main__":
    main()