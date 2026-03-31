#!/usr/bin/env python3
"""
Dashboard Streamlit — Prévisions IMD par station.
Embeddable dans React via <iframe src="http://localhost:8501" />

Lancer : streamlit run scripts/forecast_dashboard.py
"""
import json
from pathlib import Path

import plotly.graph_objects as go
import streamlit as st

FORECAST_PATH = Path("data/processed/forecast.jsonl")
STATIONS_GEOJSON = Path("data/raw/postes_synop.geojson")

st.set_page_config(
    page_title="Prévisions IMD",
    page_icon="🌤️",
    layout="wide",
)


@st.cache_data
def load_station_names(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    return {
        str(f["properties"]["Id"]).strip(): str(f["properties"]["Nom"]).strip()
        for f in data.get("features", [])
        if f.get("properties", {}).get("Id") and f.get("properties", {}).get("Nom")
    }


@st.cache_data(ttl=60)
def load_forecast(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def imd_color(value: float) -> str:
    if value < 25:
        return "#2ecc71"
    if value < 50:
        return "#f1c40f"
    if value < 75:
        return "#e67e22"
    return "#e74c3c"


rows = load_forecast(FORECAST_PATH)
station_names = load_station_names(STATIONS_GEOJSON)

if not rows:
    st.error(f"Aucune donnée de prévision trouvée dans `{FORECAST_PATH}`.")
    st.info("Lancez d'abord : `bash scripts/run_step6_forecast.sh`")
    st.stop()

# --- Sidebar controls ---
stations = sorted({r["stationId"] for r in rows if r.get("stationId")})
models = sorted({r["model"] for r in rows if r.get("model")})

def station_label(sid: str) -> str:
    name = station_names.get(sid, "")
    return f"{sid} — {name}" if name else sid

st.sidebar.title("Filtres")
selected_station = st.sidebar.selectbox(
    "Station",
    stations,
    format_func=station_label,
)
selected_model = st.sidebar.radio(
    "Modèle",
    models,
    format_func=lambda m: "Court terme 24h (AR)" if m == "AR_global" else "Tendance 7 jours",
)

# --- Filter data ---
filtered = [
    r for r in rows
    if r.get("stationId") == selected_station and r.get("model") == selected_model
]
filtered.sort(key=lambda r: r.get("timestamp", ""))

# --- Header ---
st.title("🌤️ Prévisions — Indice Météo Défavorable (IMD)")
station_display = station_label(selected_station)
st.caption(
    f"Station **{station_display}** · "
    f"{'Court terme 24h' if selected_model == 'AR_global' else 'Tendance 7 jours'} · "
    f"{len(filtered)} points"
)

if not filtered:
    st.warning("Aucune prévision disponible pour cette sélection.")
    st.stop()

# --- Metrics ---
imd_values = [r["predicted"]["IMD"] for r in filtered]
col1, col2, col3 = st.columns(3)
col1.metric("IMD min", f"{min(imd_values):.1f}")
col2.metric("IMD max", f"{max(imd_values):.1f}")
col3.metric(
    "R² modèle",
    f"{filtered[0]['modelStats']['r2']:.4f}",
    help="Coefficient de détermination — plus proche de 1 = meilleur ajustement",
)

# --- Chart ---
timestamps = [r["timestamp"] for r in filtered]
horizon_label = "forecastHorizonH" if selected_model == "AR_global" else "forecastHorizonDays"

fig = go.Figure()

fig.add_trace(go.Scatter(
    x=timestamps,
    y=imd_values,
    mode="lines+markers",
    name="IMD prédit",
    line=dict(color="#3498db", width=2),
    marker=dict(size=6),
    hovertemplate="<b>%{x}</b><br>IMD : %{y:.2f}<extra></extra>",
))

# Coloured background zones
fig.add_hrect(y0=0,  y1=25,  fillcolor="#2ecc71", opacity=0.07, line_width=0, annotation_text="Favorable",   annotation_position="left")
fig.add_hrect(y0=25, y1=50,  fillcolor="#f1c40f", opacity=0.07, line_width=0, annotation_text="Modéré",      annotation_position="left")
fig.add_hrect(y0=50, y1=75,  fillcolor="#e67e22", opacity=0.07, line_width=0, annotation_text="Dégradé",     annotation_position="left")
fig.add_hrect(y0=75, y1=100, fillcolor="#e74c3c", opacity=0.07, line_width=0, annotation_text="Très mauvais",annotation_position="left")

fig.update_layout(
    xaxis_title="Horodatage",
    yaxis_title="IMD (0–100)",
    yaxis=dict(range=[0, 100]),
    height=420,
    margin=dict(l=60, r=20, t=20, b=60),
    hovermode="x unified",
    plot_bgcolor="white",
    paper_bgcolor="white",
)
fig.update_xaxes(showgrid=True, gridcolor="#f0f0f0")
fig.update_yaxes(showgrid=True, gridcolor="#f0f0f0")

st.plotly_chart(fig, use_container_width=True)

# Trend info
if selected_model == "trend_linear":
    slope = filtered[0]["modelStats"].get("slope", 0)
    direction = "↑ hausse" if slope > 0 else "↓ baisse"
    st.info(
        f"Tendance : **{direction}** de {abs(slope):.4f} points IMD / jour "
        f"(pente = {slope:+.6f})"
    )

# --- Raw data table ---
with st.expander("Données brutes"):
    st.dataframe(
        [
            {
                "timestamp": r["timestamp"],
                "IMD prédit": r["predicted"]["IMD"],
                horizon_label: r.get(horizon_label),
                "modèle": r["model"],
                "R²": r["modelStats"]["r2"],
            }
            for r in filtered
        ],
        use_container_width=True,
    )

# --- Iframe embed hint ---
st.sidebar.markdown("---")
st.sidebar.markdown(
    "**Intégration React :**\n```html\n<iframe\n  src=\"http://localhost:8501\"\n  width=\"100%\"\n  height=\"600\"\n  frameborder=\"0\"\n/>\n```"
)
