# -*- coding: utf-8 -*-
"""
Created on Sat Dec 13 13:23:19 2025

@author: HP
"""
# -*- coding: utf-8 -*-
"""
Analyse batch Spark des données Binance + génération d'un rapport HTML.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import to_date
import requests
import API_Postgres

from config_binance import (
    PG_HOST,
    PG_PORT,
    PG_TARGET_DB,
    PG_USER,
    PG_PWD,
    EMAIL_USER,
    EMAIL_PWD,
    BATCH_INTERVAL_SECONDS,
    SPARK_MASTER,
    destinataires
)


import pandas as pd
import matplotlib.pyplot as plt

from datetime import datetime
from pathlib import Path
import os, base64, io
import time

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders


# -------------------------------------------------------------------
# 0) Dossier de base du projet
# -------------------------------------------------------------------
try:
    BASE_DIR = Path(__file__).resolve().parent
    os.chdir(BASE_DIR)
except NameError:
    # __file__ n'existe pas si exécution interactive (normalement pas ton cas)
    BASE_DIR = Path.cwd()

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_TARGET_DB}"
POSTGRES_JAR = BASE_DIR / "jars" / "postgresql-42.7.8.jar"


# -------------------------------------------------------------------
# 1) Job Spark : lecture, agrégats, conversion en pandas
# -------------------------------------------------------------------
def job_spark_batch():
    print("[Spark] Lecture de la table binance_tickers...")

    print("Création de la SparkSession...")
    spark = (
        SparkSession.builder
        .appName("Analyse_Binance_Spark")
        .master(SPARK_MASTER)
        .config("spark.jars", str(POSTGRES_JAR))    # 🔑 jar PostgreSQL
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    df = (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "binance_tickers")
        .option("user", PG_USER)
        .option("password", PG_PWD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    print("[Spark] Recherche du dernier run_ts...")
    last_run = df.agg(F.max("run_ts").alias("last_run")).collect()[0]["last_run"]
    print(" dernier run_ts :", last_run)

    df_last = df.filter(F.col("run_ts") == last_run)
    print(" lignes dans le dernier batch :", df_last.count())

    # Ajout de colonnes price spread / mid_price / date / event_time
    df_last = df_last.withColumn(
    "spread",
    F.col("high_price") - F.col("low_price")
    ).withColumn(
    "mid_price",
    (F.col("high_price") + F.col("low_price")) / 2
    )

    # date "propre" (AAAA-MM-JJ) pour agréger / regrouper
    df_last = df_last.withColumn("date", to_date(F.col("run_ts")))

    # secondes depuis 1970-01-01 à partir du TIMESTAMP
    df_last = df_last.withColumn(
    "event_time",
    F.col("run_ts").cast("long")   # ICI on cast le TIMESTAMP, pas le DATE
    )


    df = df_last

    # Volume en millions
    df = df.withColumn(
        "quote_volume_million",
        (F.col("quote_volume") / 1_000_000).cast("double")
    )

    # Indicateurs par ligne
    # Indicateurs clés

    # petite fonction utilitaire pour éviter les divisions par zéro

    def safe_div(num_col, denom_col):
       return F.when(
           (denom_col.isNull()) | (denom_col == 0),
           F.lit(None)
       ).otherwise(num_col / denom_col)

    df = (
       df
       .withColumn("volatility_abs", F.col("high_price") - F.col("low_price"))
       .withColumn(
           "return_pct",
           safe_div(
               F.col("close_price") - F.col("open_price"),
               F.col("open_price")
           )
       )
       .withColumn(
           "volatility_pct",
           safe_div(
               F.col("volatility_abs"),
               F.col("mid_price")
           )
       )
   )

    # KPIs globaux
    g = (
        df.agg(
            F.countDistinct("symbol").alias("nb_symbols"),
            F.count("*").alias("nb_rows"),
            F.sum("quote_volume_million").alias("total_quote_volume_million"),
            F.avg("return_pct").alias("avg_market_return_pct"),
            F.avg("volatility_pct").alias("avg_market_volatility_pct"),
        )
        .collect()[0]
    )

    global_stats = {
        "nb_symbols": int(g["nb_symbols"] or 0),
        "nb_rows": int(g["nb_rows"] or 0),
        "total_quote_volume_million": float(g["total_quote_volume_million"] or 0.0),
        "avg_market_return_pct": float(g["avg_market_return_pct"] or 0.0),
        "avg_market_volatility_pct": float(g["avg_market_volatility_pct"] or 0.0),
    }

    # Agrégats par crypto
    symbol_stats = (
        df.groupBy("symbol")
        .agg(
            F.avg("return_pct").alias("avg_return_pct"),
            F.avg("volatility_pct").alias("avg_volatility_pct"),
            F.sum("quote_volume_million").alias("total_quote_volume_million"),
        )
    )

    symbol_pd = symbol_stats.toPandas()

    TOP_N = 5
    top_winners  = symbol_pd.sort_values("avg_return_pct", ascending=False).head(TOP_N).copy()
    top_losers   = symbol_pd.sort_values("avg_return_pct", ascending=True).head(TOP_N).copy()
    top_volume   = symbol_pd.sort_values("total_quote_volume_million", ascending=False).head(TOP_N).copy()
    top_volatile = symbol_pd.sort_values("avg_volatility_pct", ascending=False).head(TOP_N).copy()

    print("[Spark] Agrégats calculés, conversion en pandas terminée.")

    # On peut arrêter Spark proprement
    spark.stop()

    return global_stats, top_winners, top_losers, top_volume, top_volatile


# -------------------------------------------------------------------
# 2) Fonctions pour graphiques / tableaux HTML
# -------------------------------------------------------------------
BLUE_DARK = "#0B2A6F"
BLUE = "#2563EB"
YELLOW = "#FBBF24"
GRID = "#E5E7EB"


def fig_to_base64_png(fig):
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=170, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


def bar_chart_base64(df, x, y, title, y_label, is_percent=False, sort_desc=True):
    d = df[[x, y]].copy()
    d = d.sort_values(y, ascending=not sort_desc)

    fig = plt.figure(figsize=(6.3, 3.2))
    ax = fig.add_subplot(111)
    values = (d[y] * 100) if is_percent else d[y]

    ax.bar(d[x], values, color=BLUE)
    ax.set_title(title, fontsize=12, color=BLUE_DARK, fontweight="bold")
    ax.set_ylabel(y_label)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    for s in ["top", "right"]:
        ax.spines[s].set_visible(False)

    for i, v in enumerate(values.tolist()):
        ax.text(
            i,
            v,
            f"{v:.2f}" + ("%" if is_percent else ""),
            ha="center",
            va="bottom",
            fontsize=9,
            color=BLUE_DARK,
        )

    return fig_to_base64_png(fig)


def fmt_money(x):
    return f"{x:,.0f}".replace(",", " ")


def chip_pct(x, good_if_positive=True, warn_threshold=None):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return "-"
    pct = x * 100

    if warn_threshold is not None:
        cls = "warn" if x >= warn_threshold else "good"
        return f'<span class="chip {cls}">{pct:.2f}%</span>'

    cls = "good" if x >= 0 else "bad"
    sign = "+" if x > 0 else ""
    return f'<span class="chip {cls}">{sign}{pct:.2f}%</span>'


def table_html(df, cols, rename_map, formatters=None):
    d = df[cols].copy().rename(columns=rename_map)
    if formatters:
        for c, f in formatters.items():
            if c in d.columns:
                d[c] = d[c].map(f)
    return d.to_html(index=False, border=0, classes="tbl", justify="center", escape=False)


# -------------------------------------------------------------------
# 3) Génération du rapport HTML complet
# -------------------------------------------------------------------
def generer_rapport_html(global_stats, top_winners, top_losers, top_volume, top_volatile):
    VOL_WARN = 0.02  # 2%

    # Graphiques
    b64_winners = bar_chart_base64(
        top_winners, "symbol", "avg_return_pct", "Top 5 des marchés gagnants", "Return moyen", is_percent=True
    )
    b64_losers = bar_chart_base64(
        top_losers, "symbol", "avg_return_pct", "Top 5 des marchés perdants", "Return moyen", is_percent=True, sort_desc=False
    )
    b64_volume = bar_chart_base64(
        top_volume, "symbol", "total_quote_volume_million", "Top 5 des volumes (million USDT)", "Volume (USDT)"
    )
    b64_volat = bar_chart_base64(
        top_volatile, "symbol", "avg_volatility_pct", "Top 5 des marchés les plus volatiles", "Volatilité moyenne", is_percent=True
    )

    # Tableaux
    tbl_winners = table_html(
        top_winners,
        ["symbol", "avg_return_pct", "avg_volatility_pct", "total_quote_volume_million"],
        {
            "symbol": "Symbole",
            "avg_return_pct": "Return moyen",
            "avg_volatility_pct": "Volatilité",
            "total_quote_volume_million": "Volume USDT",
        },
        {
            "Return moyen": lambda v: chip_pct(v, good_if_positive=True),
            "Volatilité": lambda v: chip_pct(v, warn_threshold=VOL_WARN),
            "Volume USDT": fmt_money,
        },
    )

    tbl_losers = table_html(
        top_losers,
        ["symbol", "avg_return_pct", "avg_volatility_pct", "total_quote_volume_million"],
        {
            "symbol": "Symbole",
            "avg_return_pct": "Return moyen",
            "avg_volatility_pct": "Volatilité",
            "total_quote_volume_million": "Volume USDT",
        },
        {
            "Return moyen": lambda v: chip_pct(v, good_if_positive=True),
            "Volatilité": lambda v: chip_pct(v, warn_threshold=VOL_WARN),
            "Volume USDT": fmt_money,
        },
    )

    tbl_volume = table_html(
        top_volume,
        ["symbol", "total_quote_volume_million", "avg_return_pct", "avg_volatility_pct"],
        {
            "symbol": "Symbole",
            "total_quote_volume_million": "Volume USDT",
            "avg_return_pct": "Return moyen",
            "avg_volatility_pct": "Volatilité",
        },
        {
            "Return moyen": lambda v: chip_pct(v, good_if_positive=True),
            "Volatilité": lambda v: chip_pct(v, warn_threshold=VOL_WARN),
            "Volume USDT": fmt_money,
        },
    )

    tbl_volat = table_html(
        top_volatile,
        ["symbol", "avg_volatility_pct", "avg_return_pct", "total_quote_volume_million"],
        {
            "symbol": "Symbole",
            "avg_volatility_pct": "Volatilité",
            "avg_return_pct": "Return moyen",
            "total_quote_volume_million": "Volume USDT",
        },
        {
            "Return moyen": lambda v: chip_pct(v, good_if_positive=True),
            "Volatilité": lambda v: chip_pct(v, warn_threshold=VOL_WARN),
            "Volume USDT": fmt_money,
        },
    )

    # KPIs chips
    market_return_chip = chip_pct(global_stats["avg_market_return_pct"])
    market_vol_chip = chip_pct(global_stats["avg_market_volatility_pct"], warn_threshold=VOL_WARN)

    # Meilleurs / pires
    best = top_winners.iloc[0]
    worst = top_losers.iloc[0]
    bigv = top_volume.iloc[0]
    highv = top_volatile.iloc[0]

    summary_text = (
        f"Sur la fenêtre des 10 dernières minutes, {global_stats['nb_symbols']} échanges de cryptomonnaies ont été réalisés dans le marché"
        f"({global_stats['nb_rows']} Observations). Le volume total échangé s’élève à "
        f"{fmt_money(global_stats['total_quote_volume_million'])} M USDT.\n"
        f"Au niveau global, le marché affiche un rendement moyen de "
        f"{global_stats['avg_market_return_pct']*100:.2f}% et une volatilité moyenne de "
        f"{global_stats['avg_market_volatility_pct']*100:.2f}%.\n"
        f"En performance, {best['symbol']} ressort comme le marché d'échange le plus dynamique "
        f"({best['avg_return_pct']*100:.2f}%), tandis que {worst['symbol']} enregistre la baisse la plus marquée "
        f"({worst['avg_return_pct']*100:.2f}%).\n"
        f"En liquidité, {bigv['symbol']} domine avec "
        f"{fmt_money(bigv['total_quote_volume_million'])} M USDT échangés.\n"
        f"Enfin, le marché le plus volatil sur la période est {highv['symbol']} "
        f"({highv['avg_volatility_pct']*100:.2f}%)."
    )

    decision_text = (
        f"• <b style='color:#0B2A6F'> Dynamique positive :</b> {best['symbol']} affiche la meilleure performance en moyenne sur la période analysée, "
        "ce qui traduit une évolution favorable du prix sur la fenêtre récente.\n"
        f"• <b style='color:#0B2A6F'> Signal de prudence :</b> {worst['symbol']} présente un rendement moyen négatif, indiquant une tendance défavorable.\n"
        f"• <b style='color:#0B2A6F'> Profondeur de marché :</b> {bigv['symbol']} se distingue par le volume d’échanges le plus élevé, "
        "ce qui suggère une forte liquidité et une meilleure facilité d’exécution des transactions.\n"
        f"• <b style='color:#0B2A6F'> Niveau de risque :</b> {highv['symbol']} enregistre la volatilité la plus élevée, révélant des fluctuations de prix marquées "
        "et un risque plus important à court terme."
    )

    group_members = [
        "COMPAORE Bassirou",
        "DIAKHATE Khadidiatou",
        "DIALLO Aissatou",
        "FOGWOUNG DJOUFACK Sarah-Laure",
        "FOUMSOU Lawa Prosper",
    ]
    programme = "Élèves Ingénieurs statisticiens économistes – 2ème année"

    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    now_fs = datetime.now().strftime("%Y%m%d_%H%M")

    #  Dossier rapports/
    out_dir = BASE_DIR / "rapports"
    out_dir.mkdir(exist_ok=True)
    html_path = out_dir / f"Rapport_Binance_{now_fs}.html"

    # === HTML final ===
    html = f"""<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="utf-8">
<title>Dashboard Binance – Batch Spark</title>

<style>
  :root {{
    --blue-dark: #0B2A6F;
    --blue: #2563EB;
    --yellow: #FBBF24;

    --good: #16A34A;
    --bad: #DC2626;
    --warn: #F59E0B;

    --text: #0F172A;
    --muted: #64748B;

    --bg: #FFFFFF;
    --soft: #F6F8FC;

    --border: #D6E2FF;
    --border-strong: #AFC7FF;

    --card: #FFFFFF;
    --shadow: 0 10px 25px rgba(2, 6, 23, 0.08);
  }}

  body {{
    margin: 0;
    background: var(--soft);
    font-family: system-ui, -apple-system, "Segoe UI", sans-serif;
    color: var(--text);
  }}

  .page {{
    max-width: 1120px;
    margin: 18px auto;
    background: var(--bg);
    border: 1px solid var(--border-strong);
    border-radius: 16px;
    padding: 18px 20px;
    box-shadow: var(--shadow);
  }}

  .header {{
    display: flex;
    justify-content: space-between;
    gap: 18px;
    align-items: flex-start;
    padding-bottom: 12px;
    border-bottom: 1px solid var(--border);
  }}

  .title {{
    font-size: 1.6rem;
    font-weight: 900;
    color: var(--blue-dark);
    letter-spacing: -0.02em;
  }}

  .subtitle {{
    color: var(--muted);
    font-size: 0.92rem;
    margin-top: 2px;
  }}

  .badge {{
    display: inline-flex;
    gap: 8px;
    align-items: center;
    margin-top: 10px;
    padding: 6px 10px;
    border-radius: 999px;
    background: #FFF7ED;
    border: 1px solid #FED7AA;
    color: #9A3412;
    font-size: 0.78rem;
    font-weight: 700;
  }}

  .group {{ text-align: right; font-size: 0.9rem; }}
  .group b {{ color: var(--blue-dark); }}

  .grid-kpi {{
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    gap: 12px;
    margin-top: 14px;
  }}

  .kpi {{
    border: 1px solid var(--border);
    border-radius: 14px;
    padding: 12px 12px;
    background: var(--card);
    box-shadow: 0 6px 14px rgba(2, 6, 23, 0.05);
    position: relative;
    overflow: hidden;
  }}

  .kpi:before {{
    content: "";
    position: absolute;
    left: 0; top: 0;
    height: 4px; width: 100%;
    background: linear-gradient(90deg, var(--blue), var(--yellow));
    opacity: 0.9;
  }}

  .kpi .k {{ font-size: 0.78rem; color: var(--muted); font-weight: 600; }}
  .kpi .v {{ font-size: 1.25rem; font-weight: 900; color: var(--blue-dark); margin-top: 6px; }}
  .kpi .v span{{
    color: #92400E;
    background: #FFFBEB;
    padding: 2px 8px;
    border-radius: 999px;
    border: 1px solid #FDE68A;
    font-weight: 900;
    margin-left: 6px;
    font-size: 0.9rem;
  }}

  .section {{ margin-top: 16px; }}
  .section h2 {{ margin: 0 0 6px 0; font-size: 1.12rem; color: var(--blue-dark); }}
  .section p  {{ margin: 0 0 10px 0; color: var(--muted); font-size: 0.92rem; }}

  .grid-2 {{
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 12px;
  }}

  .card {{
    border: 1px solid var(--border);
    border-radius: 14px;
    padding: 12px 12px;
    background: var(--card);
    box-shadow: 0 8px 18px rgba(2, 6, 23, 0.06);
  }}

  .card h3 {{
    margin: 0 0 8px 0;
    font-size: 1rem;
    font-weight: 900;
    color: var(--blue-dark);
  }}

  .img {{
    width: 100%;
    border: 1px solid var(--border);
    border-radius: 12px;
    background: #fff;
    padding: 6px;
  }}

  .tbl {{
    width: 100%;
    border-collapse: separate;
    border-spacing: 0;
    margin-top: 10px;
    font-size: 0.84rem;
    border: 1px solid var(--border);
    border-radius: 12px;
    overflow: hidden;
  }}

  .tbl th, .tbl td {{
    padding: 8px 10px;
    text-align: center;
    border-bottom: 1px solid var(--border);
  }}

  .tbl th {{
    background: #EFF6FF;
    color: var(--blue-dark);
    font-weight: 900;
  }}

  .tbl tr:last-child td {{ border-bottom: none; }}

  .chip {{
    display: inline-flex;
    align-items: center;
    justify-content: center;
    padding: 2px 10px;
    border-radius: 999px;
    font-weight: 900;
    font-size: 0.82rem;
    border: 1px solid transparent;
    white-space: nowrap;
  }}
  .chip.good {{ color: #065F46; background: #ECFDF5; border-color: #A7F3D0; }}
  .chip.bad  {{ color: #7F1D1D; background: #FEF2F2; border-color: #FECACA; }}
  .chip.warn {{ color: #7C2D12; background: #FFFBEB; border-color: #FDE68A; }}

  .note {{
    border: 1px solid var(--border);
    border-left: 6px solid var(--blue);
    background: #F8FAFF;
    padding: 12px 12px;
    border-radius: 14px;
    white-space: pre-line;
  }}

  .decision {{
    border: 1px solid #FDE68A;
    border-left: 6px solid var(--yellow);
    background: #FFFBEB;
    padding: 12px 12px;
    border-radius: 14px;
    white-space: pre-line;
  }}

  .footer {{
    margin-top: 12px;
    color: var(--muted);
    font-size: 0.78rem;
    text-align: right;
    border-top: 1px solid var(--border);
    padding-top: 10px;
  }}
</style>

</head>
<body>
<div class="page">

  <div class="header">
    <div>
      <div class="title"> Projet Big Data — Traitement batch Spark et génération automatique de rapports </div>
      <div class="subtitle"> <b> Source </b> : Binance · <b> Généré le </b> {now} · <b> Fenêtre analysée </b> : dernières 10 minutes </div>
      <div class="badge"> Rapport automatisé de suivi de la performance, de la liquidité et du risque des marchés de cryptomonnaies </div>
    </div>
    <div class="group">
      <b> Réalisé par </b><br>
      {"<br>".join(group_members)}<br>
      <span class="subtitle">{programme}</span>
    </div>
  </div>

  <div class="grid-kpi">
    <div class="kpi">
      <div class="k">Nouveaux échanges effectués</div>
      <div class="v">{global_stats["nb_symbols"]}</div>
    </div>
    <div class="kpi">
      <div class="k"> observations</div>
      <div class="v">{global_stats["nb_rows"]}</div>
    </div>
    <div class="kpi">
      <div class="k">Volume total échangé</div>
      <div class="v">{fmt_money(global_stats["total_quote_volume_million"])} <span>million USDT</span></div>
    </div>
    <div class="kpi">
      <div class="k">État global du marché (return / volatilité)</div>
      <div class="v">{market_return_chip} &nbsp; {market_vol_chip}</div>
    </div>
  </div>

  <div class="section">
    <h2>Performance & Liquidité des marchés</h2>
    <p>Top 5 par performance (return), liquidité (volume USDT) et risque (volatilité).</p>

    <div class="grid-2">
      <div class="card">
        <h3>Top 5 gagnants (Return moyen)</h3>
        <img class="img" src="data:image/png;base64,{b64_winners}" />
        {tbl_winners}
      </div>

      <div class="card">
        <h3>Top 5 perdants (Return moyen)</h3>
        <img class="img" src="data:image/png;base64,{b64_losers}" />
        {tbl_losers}
      </div>
    </div>

    <div class="grid-2" style="margin-top:12px;">
      <div class="card">
        <h3>Top 5 volumes (USDT)</h3>
        <img class="img" src="data:image/png;base64,{b64_volume}" />
        {tbl_volume}
      </div>

      <div class="card">
        <h3>Top 5 volatilité</h3>
        <img class="img" src="data:image/png;base64,{b64_volat}" />
        {tbl_volat}
      </div>
    </div>
  </div>

  <div class="section">
    <h2>Synthèse</h2>
    <div class="note">{summary_text}</div>
  </div>

  <div class="section">
    <h2>Aide à la décision (lecture prudente — pas une recommandation d’investissement)</h2>
    <div class="decision">{decision_text}</div>
  </div>

  <div class="footer">
    Rapport généré automatiquement à partir des données Binance via PySpark (traitement batch).<br>
    Objectif : reporting clair et interprétable (top 5, KPIs, résumé).
  </div>

</div>
</body>
</html>
"""

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"HTML généré : {html_path}")
    return html_path



    
def envoyer_rapport_html(destinataires, html_path):
    print("[MAIL] Début de l'envoi du rapport...")
    print("Fichier HTML :", html_path)
    print("Destinataires :", destinataires)

    try:
        # 1) Corps du mail en texte simple
        body = """Bonjour,

Veuillez trouver, en attaché, le rapport Binance (format HTML) du .
Vous pourrez le visualiser dans votre navigateur.


Bonne réception

--
Respectueusement

COMPAORE Bassirou
DIAKHATE Khadidiatou
DIALLO Aissatou
FOGWOUNG DJOUFACK Sarah-Laure
FOUMSOU Lawa Prosper

ISE2
"""

        msg = MIMEMultipart()
        msg["Subject"] = "Rapport Binance – Fichier HTML"
        msg["From"] = EMAIL_USER
        msg["To"] = ", ".join(destinataires)

        msg.attach(MIMEText(body, "plain", _charset="utf-8"))

        # 2) Pièce jointe : le fichier HTML
        html_path = Path(html_path)
        with open(html_path, "rb") as f:
            part = MIMEBase("text", "html")
            part.set_payload(f.read())

        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f'attachment; filename="{html_path.name}"',
        )
        msg.attach(part)

        # 3) Envoi via Gmail
        print("[MAIL] Connexion à smtp.gmail.com...")
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(EMAIL_USER, EMAIL_PWD)
            print("[MAIL] Authentification OK")
            server.sendmail(EMAIL_USER, destinataires, msg.as_string())

        print(f"[MAIL] Rapport envoyé à : {', '.join(destinataires)}")

    except Exception as e:
        print("[MAIL] Erreur pendant l'envoi :")
        print(e)
# -------------------------------------------------------------------
# 4) MAIN – un SEUL batch, pas de boucle infinie
# -------------------------------------------------------------------
if __name__ == "__main__":
    while True:
        print("Nouveau batch COMPLET : ingestion + analyse + rapport...")

        # 1) Ingestion : on récupère de nouvelles données Binance et on les insère dans Postgres
        API_Postgres.job_ingestion()

        # 2) Analyse Spark sur le dernier run_ts
        global_stats, top_winners, top_losers, top_volume, top_volatile = job_spark_batch()

        # 3) Génération du rapport HTML
        html_path = generer_rapport_html(global_stats, top_winners, top_losers, top_volume, top_volatile)

        # 4) Envoi par mail
        
        envoyer_rapport_html(destinataires, str(html_path))

        print("Fin du batch. Rapport disponible dans :", html_path)

        # 5) Pause
        time.sleep(BATCH_INTERVAL_SECONDS)   # ou 6*60*60 pour 6h
