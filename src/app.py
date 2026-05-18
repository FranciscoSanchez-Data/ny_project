import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
from pathlib import Path

# 1. Configuración de la página (Modo Ancho y Oscuro por defecto)
st.set_page_config(
    page_title="NYC Taxi Analytics",
    page_icon="🚕",
    layout="wide"
)

st.title("🚕 NYC Yellow Taxi - Dashboard de Operaciones 2023")
st.markdown("Analizando la eficiencia operativa e ingresos de la capa **Gold** del Lakehouse.")
st.markdown("---")

# 2. Ruta absoluta al archivo DuckDB generado por dbt
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "dbt" / "local_lakehouse.duckdb"

# 3. Función optimizada con caché para cargar los datos de la capa Gold
@st.cache_data
def load_gold_data():
    if not DB_PATH.exists():
        st.error(f"❌ No se encuentra la base de datos en: {DB_PATH}. ¿Ejecutaste 'dbt run'?")
        return pd.DataFrame()
    
    # Conectamos a DuckDB en modo lectura
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    df = conn.execute("SELECT * FROM gold_daily_revenue").df()
    conn.close()
    
    # Convertir la fecha a tipo datetime para los gráficos
    df['pickup_date'] = pd.to_datetime(df['pickup_date'])
    return df

# Carga de datos
df = load_gold_data()

if not df.empty:
    # 4. KPIs Principales (Tarjetas Superiores)
    total_revenue = df['total_revenue'].sum()
    total_trips = df['total_trips'].sum()
    avg_fare = df['avg_fare'].mean()
    avg_distance = df['avg_trip_distance'].mean()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("💰 Ingresos Totales (Q1)", f"${total_revenue:,.2f}")
    col2.metric("🚖 Viajes Totales", f"{total_trips:,}")
    col3.metric("💵 Tarifa Promedio", f"${avg_fare:.2f}")
    col4.metric("📍 Distancia Promedio", f"{avg_distance:.2f} millas")

    st.markdown("---")

    # 5. Gráficos Interactivos (Distribución en Columnas)
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("📈 Evolución de Ingresos Diarios")
        fig_revenue = px.line(
            df, 
            x='pickup_date', 
            y='total_revenue',
            labels={'pickup_date': 'Fecha', 'total_revenue': 'Ingresos ($)'},
            line_shape='spline',
            render_mode='svg'
        )
        fig_revenue.update_traces(line_color='#F1C40F', line_width=2.5)
        st.plotly_chart(fig_revenue, use_container_width=True)

    with col_right:
        st.subheader("💳 Cuota de Mercado: Tarjeta vs Efectivo")
        # Transformamos las columnas de pagos para poder graficarlas juntas
        df_melted = df.melt(
            id_vars=['pickup_date'], 
            value_vars=['card_payment_share_pct', 'cash_payment_share_pct'],
            var_name='Metodo_Pago', 
            value_name='Porcentaje'
        )
        df_melted['Metodo_Pago'] = df_melted['Metodo_Pago'].map({
            'card_payment_share_pct': 'Tarjeta',
            'cash_payment_share_pct': 'Efectivo'
        })
        
        fig_payment = px.line(
            df_melted, 
            x='pickup_date', 
            y='Porcentaje', 
            color='Metodo_Pago',
            labels={'pickup_date': 'Fecha', 'Porcentaje': '% del Total'},
            color_discrete_map={'Tarjeta': '#2980B9', 'Efectivo': '#27AE60'}
        )
        st.plotly_chart(fig_payment, use_container_width=True)

    st.markdown("---")

    # 6. Tabla Detallada
    st.subheader("📊 Tabla de Datos de Negocio (Marts)")
    st.dataframe(
        df.sort_values(by='pickup_date', ascending=False), 
        use_container_width=True
    )