import os
import sqlite3
import pandas as pd
import streamlit as st
import plotly.express as px

st.set_page_config(
    page_title="Paris Housing Dashboard",
    page_icon="🗼",
    layout="wide",
    initial_sidebar_state="expanded",
)

DWH_PATH = os.path.join(os.path.dirname(__file__), "../data/warehouse.db")

@st.cache_data
def load_table(table: str) -> pd.DataFrame:
    con = sqlite3.connect(DWH_PATH)
    df = pd.read_sql(f"SELECT * FROM {table}", con)
    con.close()
    return df

def fmt_eur(v: float) -> str:
    if v >= 1_000_000:
        return f"€{v/1_000_000:.2f}M"
    return f"€{v/1_000:.0f}k"

# Load data
try:
    fact = load_table("fact_properties")
    agg_arr = load_table("agg_by_arrondissement")
    agg_type = load_table("agg_by_type")
    agg_cond = load_table("agg_by_condition")
    agg_decade = load_table("agg_by_decade")
    agg_dist = load_table("agg_price_distribution")
except Exception as e:
    st.error(f"Cannot load warehouse. Run `python pipeline/orchestrate.py` first.\n\n{e}")
    st.stop()

# Sidebar filters
st.sidebar.title("🗼 Paris Housing")
st.sidebar.markdown("---")
prop_types = ["All"] + sorted(fact["Property_Type"].unique())
sel_type = st.sidebar.selectbox("Property type", prop_types)
conditions = ["All"] + sorted(fact["Condition"].unique())
sel_cond = st.sidebar.selectbox("Condition", conditions)
price_range = st.sidebar.slider(
    "Price range (€)",
    min_value=int(fact["Price_EUR"].min()),
    max_value=int(fact["Price_EUR"].max()),
    value=(int(fact["Price_EUR"].min()), int(fact["Price_EUR"].max())),
    step=50_000,
    format="€%d",
)
arr_options = ["All"] + sorted(fact["Arrondissement"].unique().tolist())
sel_arr = st.sidebar.selectbox("Arrondissement", arr_options)
st.sidebar.markdown("---")
st.sidebar.caption("Data: Paris Housing Prices Dataset (1 200 properties)")

# Filter
filt = fact.copy()
if sel_type != "All": filt = filt[filt["Property_Type"] == sel_type]
if sel_cond != "All": filt = filt[filt["Condition"] == sel_cond]
if sel_arr != "All": filt = filt[filt["Arrondissement"] == sel_arr]
filt = filt[filt["Price_EUR"].between(*price_range)]

# Header
st.title("🗼 Paris Housing Market Dashboard")
st.markdown(f"Showing **{len(filt):,}** of {len(fact):,} properties")
st.markdown("---")

# KPI
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Properties", f"{len(filt):,}")
k2.metric("Avg price", fmt_eur(filt["Price_EUR"].mean()) if len(filt) else "—")
k3.metric("Median price", fmt_eur(filt["Price_EUR"].median()) if len(filt) else "—")
k4.metric("Avg size", f"{filt['Size_sqm'].mean():.0f} m²" if len(filt) else "—")
k5.metric("Avg €/m²", f"€{filt['Price_per_sqm'].mean():.0f}" if len(filt) else "—")
st.markdown("---")

# TILE 1 — Categorical
st.subheader("📍 Tile 1 — Average price by arrondissement")
arr_filt = (
    filt.groupby("Arrondissement")
    .agg(avg_price=("Price_EUR","mean"), count=("Property_ID","count"))
    .reset_index()
    .sort_values("Arrondissement")
)
arr_filt["label"] = arr_filt["Arrondissement"].astype(str) + "e"
fig_arr = px.bar(
    arr_filt, x="label", y="avg_price",
    text=arr_filt["avg_price"].apply(lambda v: f"€{v/1e6:.2f}M"),
    color="avg_price",
    color_continuous_scale="Blues",
    labels={"label": "Arrondissement", "avg_price": "Average price (€)"},
    title="Average property price by arrondissement",
    hover_data={"count": True},
)
fig_arr.update_traces(textposition="outside")
fig_arr.update_layout(coloraxis_showscale=False, yaxis_tickformat="€,.0f", showlegend=False)
st.plotly_chart(fig_arr, use_container_width=True)

# TILE 2 — Temporal
st.subheader("📅 Tile 2 — Average price by decade built")
dec_filt = (
    filt.groupby("Decade_Built")
    .agg(avg_price=("Price_EUR","mean"), count=("Property_ID","count"))
    .reset_index()
    .sort_values("Decade_Built")
)
fig_dec = px.line(
    dec_filt, x="Decade_Built", y="avg_price",
    markers=True,
    labels={"Decade_Built": "Decade built", "avg_price": "Average price (€)"},
    title="Average price evolution by decade of construction",
)
fig_dec.update_traces(line_color="#1B6EC2", marker_size=8)
fig_dec.update_layout(yaxis_tickformat="€,.0f")
st.plotly_chart(fig_dec, use_container_width=True)

st.markdown("---")
# Дополнительные графики (по желанию)
col1, col2 = st.columns(2)
with col1:
    st.subheader("🏠 Distribution by property type")
    type_filt = filt["Property_Type"].value_counts().reset_index()
    type_filt.columns = ["Type", "Count"]
    fig_t = px.pie(type_filt, names="Type", values="Count", hole=0.4)
    st.plotly_chart(fig_t, use_container_width=True)
with col2:
    st.subheader("🔧 Average price by condition")
    cond_filt = filt.groupby("Condition")["Price_EUR"].mean().sort_values().reset_index()
    fig_c = px.bar(cond_filt, x="Price_EUR", y="Condition", orientation="h",
                   text=cond_filt["Price_EUR"].apply(lambda v: f"€{v/1e6:.2f}M"))
    fig_c.update_traces(textposition="outside")
    st.plotly_chart(fig_c, use_container_width=True)

# Scatter
st.subheader("📐 Size vs price")
fig_s = px.scatter(
    filt.sample(min(500, len(filt)), random_state=42) if len(filt) > 500 else filt,
    x="Size_sqm", y="Price_EUR",
    color="Property_Type",
    hover_data=["Arrondissement_str", "Condition"],
    title="Property size vs price",
)
st.plotly_chart(fig_s, use_container_width=True)

# Raw data
with st.expander("🔍 View raw data"):
    st.dataframe(filt.reset_index(drop=True), use_container_width=True)