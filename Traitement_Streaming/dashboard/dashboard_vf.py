import streamlit as st
import os
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from streamlit_autorefresh import st_autorefresh
import numpy as np
from PIL import Image

# ================= CONFIG =================
DB_USER = st.secrets["DB_USER"]
DB_PASSWORD = st.secrets["DB_PASSWORD"]
DB_HOST = st.secrets["DB_HOST"]
DB_PORT = st.secrets["DB_PORT"]
DB_NAME = st.secrets["DB_NAME"]

DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DB_URL)

st.set_page_config(
    page_title="📊 Crypto Dashboard",
    page_icon="💹",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ================= STYLE DARK =================
st.markdown("""
<style>
body { background-color: #0b0e11; color: white; }
.stButton>button { background-color: #1f2937; color: white; }
h3 { color: #f1f5f9; }
</style>
""", unsafe_allow_html=True)

# ==================== AUTO REFRESH ====================
REFRESH_INTERVAL = 5  # secondes
st_autorefresh(interval=REFRESH_INTERVAL * 1000, key="crypto_refresh")

# ==================== LOAD DATA ====================
@st.cache_data(ttl=2, show_spinner=False)
def load_data(pairs, limit=2000):
    query = f"""
    SELECT timestamp_ts, symbol, open_price, high_price, low_price, close_price, volume
    FROM projet_bdcc
    WHERE symbol = ANY(%(pairs)s)
    ORDER BY timestamp_ts DESC
    LIMIT {limit}
    """
    df = pd.read_sql(query, engine, params={"pairs": pairs})
    if df.empty:
        return df
    df["timestamp_ts"] = pd.to_datetime(df["timestamp_ts"])
    df["pct_change"] = ((df["close_price"] - df["open_price"]) / df["open_price"] * 100).round(4)
    df["volatility"] = ((df["high_price"] - df["low_price"]) / df["close_price"]).round(6)
    df["spread"] = (df["high_price"] - df["low_price"]).round(8)
    return df.sort_values("timestamp_ts").reset_index(drop=True)

# ================= SIDEBAR : CONFIGURATION =================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
IMG_PATH = os.path.join(BASE_DIR, "img", "binance-1.png")

if os.path.exists(IMG_PATH):
    logo = Image.open(IMG_PATH)
    st.sidebar.image(logo, width=100)  # largeur réduite
else:
    st.sidebar.error(f"❌ Image introuvable : {IMG_PATH}")

# Récupérer toutes les cryptos disponibles dans la base
@st.cache_data(ttl=300, show_spinner=False)
def get_all_symbols():
    query = "SELECT DISTINCT symbol FROM projet_bdcc ORDER BY symbol"
    df_symbols = pd.read_sql(query, engine)
    return df_symbols['symbol'].tolist()

all_symbols = get_all_symbols()

pairs = st.sidebar.multiselect(
    "Choisissez les cryptos",
    all_symbols,
    default=["BTCUSDT", "ETHUSDT"] if "BTCUSDT" in all_symbols and "ETHUSDT" in all_symbols else all_symbols[:2]
)
time_filter = st.sidebar.selectbox(
    "Sélectionnez la période",
    ["1H", "24H", "7J", "30J", "Tout"],
    index=1
)

if not pairs:
    st.warning("Veuillez sélectionner au moins une crypto !")
    st.stop()

# Charger les données
df = load_data(pairs)

# ================= Filtrage temps =================
if not df.empty and time_filter != "Tout":
    now = df["timestamp_ts"].max()
    if time_filter == "1H": start_time = now - timedelta(hours=1)
    elif time_filter == "24H": start_time = now - timedelta(hours=24)
    elif time_filter == "7J": start_time = now - timedelta(days=7)
    elif time_filter == "30J": start_time = now - timedelta(days=30)
    df = df[df["timestamp_ts"] >= start_time]

# ================= ONGLETS PRINCIPAUX =================
tabs = st.tabs(["Overview", "Graphiques", "Top Cryptos", "Anomalies", "Détails Crypto"])

# ================= TAB 1: Overview =================
with tabs[0]:
    st.markdown("### 📈 Aperçu du marché")

    cols = st.columns(len(pairs))
    for i, sym in enumerate(pairs):
        d = df[df["symbol"] == sym].copy() if not df.empty else None
        if d is not None and not d.empty:
            latest = d.iloc[-1]
            change_start = d["close_price"].iloc[0]
            change_24h = latest["close_price"] - change_start
            change_pct = (change_24h / change_start) * 100

            avg_price = d["close_price"].mean()
            max_price = d["high_price"].max()
            min_price = d["low_price"].min()
            avg_volatility = d["volatility"].mean()
            total_volume = d["volume"].sum()

            cols[i].metric(
                label=f"{sym}",
                value=f"${latest['close_price']:.2f}",
                delta=f"{change_24h:.2f} ({change_pct:.2f}%)"
            )

            st.markdown(f"""
            <div style="background-color:#1f2937; padding:10px; border-radius:10px; margin-bottom:10px;">
                <p style="color:#f1f5f9; margin:0;"><b>📊 Statistiques Globales - {sym}</b></p>
                <p style="margin:0;">Prix Moyen : ${avg_price:.2f}</p>
                <p style="margin:0;">Prix Max : ${max_price:.2f} | Prix Min : ${min_price:.2f}</p>
                <p style="margin:0;">Volatilité Moyenne : {avg_volatility:.6f}</p>
                <p style="margin:0;">Volume Total : {total_volume:,.0f}</p>
            </div>
            """, unsafe_allow_html=True)

# ================= TAB 2: Graphiques chandeliers =================
with tabs[1]:
    st.markdown("### Graphiques en Chandeliers avec Volume et MA")
    for sym in pairs:
        d = df[df["symbol"] == sym].copy()
        if not d.empty:
            d["MA7"] = d["close_price"].rolling(7).mean()
            d["MA25"] = d["close_price"].rolling(25).mean()
            d["color"] = np.where(d["close_price"]>=d["open_price"], "green", "red")

            fig = go.Figure()
            # Candlestick
            fig.add_trace(go.Candlestick(
                x=d['timestamp_ts'], open=d['open_price'], high=d['high_price'],
                low=d['low_price'], close=d['close_price'], name=sym,
                increasing_line_color='green', decreasing_line_color='red',
                hovertext=[f"Open: {o}<br>High: {h}<br>Low: {l}<br>Close: {c}<br>Volume: {v}"
                            for o,h,l,c,v in zip(d['open_price'], d['high_price'], d['low_price'], d['close_price'], d['volume'])],
                hoverinfo="text"
            ))
            # Moyennes mobiles
            fig.add_trace(go.Scatter(x=d['timestamp_ts'], y=d['MA7'], name=f"MA7", line=dict(color='orange', width=2)))
            fig.add_trace(go.Scatter(x=d['timestamp_ts'], y=d['MA25'], name=f"MA25", line=dict(color='blue', width=2)))
            # Volume
            fig.add_trace(go.Bar(x=d['timestamp_ts'], y=d['volume'], name='Volume', marker_color=d['color'],
                                 yaxis='y2', opacity=0.3))
            # Layout
            fig.update_layout(
                template="plotly_dark", height=400,
                xaxis_title="Time", yaxis_title="Price",
                yaxis2=dict(title='Volume', overlaying='y', side='right', showgrid=False, range=[0, d['volume'].max()*4]),
                xaxis_rangeslider_visible=False,
                hovermode="x unified",
                title=f"Candlestick & Volume - {sym}"
            )
            st.plotly_chart(fig, use_container_width=True)

# ================= TAB 3: Top Cryptos =================
with tabs[2]:
    if not df.empty:
        latest = df.groupby('symbol').last().reset_index()
        
        # =========================
        # Top 5 par volume (INCHANGÉ)
        # =========================
        top5_vol = latest.nlargest(5, 'volume')[['symbol','close_price','volume','pct_change','volatility']].copy()
        top5_vol.columns = ['Symbole','Prix Clôture','Volume','Variation (%)','Volatilité']
        st.markdown("### 🔝 Top 5 Cryptos par Volume")
        st.dataframe(top5_vol.set_index('Symbole'), use_container_width=True)

        # =========================
        # Top Gainers / Top Losers (INCHANGÉ)
        # =========================
        top_gainers = latest.nlargest(5, 'pct_change')[['symbol','close_price','pct_change']].copy()
        top_gainers.columns = ['Symbole','Prix Clôture','Variation (%)']

        top_losers = latest.nsmallest(5, 'pct_change')[['symbol','close_price','pct_change']].copy()
        top_losers.columns = ['Symbole','Prix Clôture','Variation (%)']

        col1, col2 = st.columns(2)

        # =====================================================
        # 📈 TOP GAINERS – ÉVOLUTION PRIX (BASE 100)
        # =====================================================
        with col1:
            st.markdown("#### 🔺 Top Gainers")
            st.dataframe(top_gainers.set_index('Symbole'), use_container_width=True)

            fig_gain = go.Figure()

            for sym in top_gainers['Symbole']:
                df_sym = (
                    df[df['symbol'] == sym]
                    .sort_values('timestamp_ts')
                    .copy()
                )

                # Normalisation Base 100
                df_sym['price_index'] = (
                    df_sym['close_price'] /
                    df_sym['close_price'].iloc[0] * 100
                )

                fig_gain.add_trace(go.Scatter(
                    x=df_sym['timestamp_ts'],
                    y=df_sym['price_index'],
                    mode='lines',
                    name=sym
                ))

            fig_gain.update_layout(
                template='plotly_dark',
                title='📈 Évolution des Top Gainers',
                xaxis_title='Time',
                yaxis_title='Indice de prix',
                hovermode='x unified',
                height=450
            )

            st.plotly_chart(fig_gain, use_container_width=True)

        # =====================================================
        # 📊 TOP LOSERS – BARRES DE VARIATION (%)
        # =====================================================
        with col2:
            st.markdown("#### 🔻 Top Losers")
            st.dataframe(top_losers.set_index('Symbole'), use_container_width=True)

            fig_loss = px.bar(
                top_losers.sort_values('Variation (%)'),
                x='Symbole',
                y='Variation (%)',
                color='Variation (%)',
                text='Variation (%)',
                color_continuous_scale='Reds_r',
                template='plotly_dark',
                title='📉 Top Losers – Variation (%)'
            )


# ================= TAB 4: Anomalies =================
with tabs[3]:
    st.markdown("### ⚠️ Anomalies et Alertes")
    if not df.empty:
        latest = df.groupby('symbol').last().reset_index()

        # ---------------- High Variation ----------------
        high_var = latest[abs(latest['pct_change']) > 5]
        if not high_var.empty:
            st.warning(f"🔺 {len(high_var)} crypto(s) avec variation > 5%")
            df_var_display = high_var[['symbol','pct_change','volume','volatility']].copy()
            df_var_display.columns = ['Symbole','Variation (%)','Volume','Volatilité']
            # Tableau stylé
            st.dataframe(df_var_display.style.format({'Variation (%)':'{:.2f}%', 'Volume':'{:,}', 'Volatilité':'{:.6f}'}).background_gradient(subset=['Variation (%)'], cmap='RdYlGn', axis=0), use_container_width=True)

            # Graphique barres pour variation
            fig_var = px.bar(
                df_var_display, x='Symbole', y='Variation (%)', 
                color='Variation (%)', color_continuous_scale='RdYlGn',
                text='Variation (%)', title="Variation des prix (>5%)"
            )
            fig_var.update_layout(template='plotly_dark', yaxis_title='% Change', height=350)
            st.plotly_chart(fig_var, use_container_width=True)

        # ---------------- High Volatility ----------------
        high_vol = latest[latest['volatility'] > latest['volatility'].quantile(0.75)]
        if not high_vol.empty:
            st.warning(f"⚡ {len(high_vol)} crypto(s) avec volatilité élevée")
            df_vol_display = high_vol[['symbol','volatility','pct_change','volume']].copy()
            df_vol_display.columns = ['Symbole','Volatilité','Variation (%)','Volume']
            # Tableau stylé
            st.dataframe(df_vol_display.style.format({'Volatilité':'{:.6f}', 'Variation (%)':'{:.2f}%', 'Volume':'{:,}'}).background_gradient(subset=['Volatilité'], cmap='RdYlGn_r', axis=0), use_container_width=True)

            # Graphique barres pour volatilité
            fig_vol = px.bar(
                df_vol_display, x='Symbole', y='Volatilité',
                color='Volatilité', color_continuous_scale='Viridis_r',
                text='Volatilité', title="Volatilité élevée (top 25%)"
            )
            fig_vol.update_layout(template='plotly_dark', yaxis_title='Volatilité', height=350)
            st.plotly_chart(fig_vol, use_container_width=True)

# ================= TAB 5: Détails Crypto =================
with tabs[4]:
    selected_symbol = st.selectbox("Sélectionnez une crypto pour les détails", pairs)
    df_sym = df[df["symbol"] == selected_symbol].sort_values("timestamp_ts", ascending=False)
    
    if not df_sym.empty:
        latest = df_sym.iloc[0]

        # ---------------- Statistiques principales ----------------
        st.markdown(f"### 📊 Détails pour {selected_symbol}")
        summary_data = {
            "Prix Ouverture": [latest['open_price']],
            "Prix Clôture": [latest['close_price']],
            "Prix Haut": [latest['high_price']],
            "Prix Bas": [latest['low_price']],
            "Spread": [latest['spread']],
            "Variation (%)": [latest['pct_change']],
            "Volatilité": [latest['volatility']],
            "Volume": [latest['volume']]
        }
        df_summary = pd.DataFrame(summary_data).T
        df_summary.columns = ["Valeur"]
        df_summary = df_summary.style.format({
            "Valeur": "{:,.4f}"
        }).background_gradient(subset=["Valeur"], cmap="RdYlGn", axis=0)
        st.table(df_summary)

        # ---------------- Graphiques High/Low et Volume ----------------
        df_sym["MA7"] = df_sym["close_price"].rolling(7).mean()
        df_sym["MA25"] = df_sym["close_price"].rolling(25).mean()

        fig_price = go.Figure()
        fig_price.add_trace(go.Scatter(x=df_sym['timestamp_ts'], y=df_sym['high_price'],
                                       name='High Price', mode='lines', line=dict(color='green')))
        fig_price.add_trace(go.Scatter(x=df_sym['timestamp_ts'], y=df_sym['low_price'],
                                       name='Low Price', mode='lines', line=dict(color='red')))
        fig_price.add_trace(go.Scatter(x=df_sym['timestamp_ts'], y=df_sym['MA7'], 
                                       name='MA7', line=dict(color='orange', dash='dot')))
        fig_price.add_trace(go.Scatter(x=df_sym['timestamp_ts'], y=df_sym['MA25'], 
                                       name='MA25', line=dict(color='blue', dash='dot')))
        fig_price.update_layout(template="plotly_dark", height=350, title=f"High/Low & MA - {selected_symbol}")
        st.plotly_chart(fig_price, use_container_width=True)

        fig_vol = px.bar(df_sym, x='timestamp_ts', y='volume', title=f"Volume - {selected_symbol}",
                         labels={'timestamp_ts':'Time','volume':'Volume'})
        fig_vol.update_layout(template="plotly_dark", height=300)
        st.plotly_chart(fig_vol, use_container_width=True)

        # ---------------- Historique complet ----------------
        st.markdown("#### Historique complet")
        df_hist = df_sym[['timestamp_ts','open_price','high_price','low_price','close_price',
                          'volume','pct_change','volatility','spread']].rename(
            columns={'timestamp_ts':'Timestamp','open_price':'Open','high_price':'High','low_price':'Low',
                     'close_price':'Close','volume':'Volume','pct_change':'% Change','volatility':'Volatility',
                     'spread':'Spread'}
        )
        df_hist = df_hist.style.format({
            'Open':'{:.4f}', 'High':'{:.4f}', 'Low':'{:.4f}', 'Close':'{:.4f}',
            'Volume':'{:,}', '% Change':'{:.2f}%', 'Volatility':'{:.6f}', 'Spread':'{:.8f}'
        }).background_gradient(subset=['% Change','Volatility'], cmap='RdYlGn', axis=0)
        st.dataframe(df_hist, use_container_width=True)

