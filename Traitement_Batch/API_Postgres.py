# -*- coding: utf-8 -*-
"""
Created on Sat Dec 13 12:54:55 2025

@author: HP
"""

# API_Postgres.py

import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone



# ==========================
# 1. CONFIG POSTGRES
# ==========================
from config_binance import (
    PG_HOST,
    PG_PORT,
    PG_SUPER_DB,
    PG_USER,
    PG_PWD,
    PG_TARGET_DB,
)


# ==========================
# 2. API BINANCE
# ==========================
def fetch_binance_tickers():
    """
    Appelle l'API REST de Binance et retourne une liste de dictionnaires.
    """
    url = "https://api.binance.com/api/v3/ticker/24hr"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()

    raw_tickers = resp.json()
    tickers = []

    run_ts = datetime.now(timezone.utc)

    for t in raw_tickers:
        tickers.append({
            "run_ts": run_ts,
            "symbol": t["symbol"],
            "close_price": float(t["lastPrice"]),
            "open_price": float(t["openPrice"]),
            "high_price": float(t["highPrice"]),
            "low_price": float(t["lowPrice"]),
            "volume": float(t["volume"]),
            "quote_volume": float(t["quoteVolume"]),
            "close_time": int(t["closeTime"])
        })

    return tickers

# ==========================
# 3. CRÉER LA BASE SI BESOIN
# ==========================
def ensure_database_exists():
    """
    Se connecte à la base 'postgres' et crée la base PG_TARGET_DB si elle n'existe pas.
    """
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_SUPER_DB,
        user=PG_USER,
        password=PG_PWD
    )
    conn.autocommit = True  # nécessaire pour CREATE DATABASE

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (PG_TARGET_DB,))
            exists = cur.fetchone()
            if not exists:
                print(f"Création de la base '{PG_TARGET_DB}'...")
                cur.execute(f"CREATE DATABASE {PG_TARGET_DB};")
            else:
                print(f"Base '{PG_TARGET_DB}' déjà existante.")
    finally:
        conn.close()

# ==========================
# 4. CRÉER LA TABLE SI BESOIN
# ==========================
def create_table_if_not_exists(conn):
    sql = """
    CREATE TABLE IF NOT EXISTS binance_tickers (
        id SERIAL PRIMARY KEY,
        run_ts TIMESTAMPTZ NOT NULL,
        symbol TEXT NOT NULL,
        close_price NUMERIC,
        open_price NUMERIC,
        high_price NUMERIC,
        low_price NUMERIC,
        volume NUMERIC,
        quote_volume NUMERIC,
        close_time BIGINT
    );
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()

# ==========================
# 5. INSÉRER LES DONNÉES
# ==========================
def insert_tickers(conn, tickers):
    if not tickers:
        print("Aucun ticker à insérer.")
        return

    insert_sql = """
    INSERT INTO binance_tickers (
        run_ts, symbol, close_price, open_price, high_price,
        low_price, volume, quote_volume, close_time
    )
    VALUES %s
    """

    values = [
        (
            t["run_ts"],
            t["symbol"],
            t["close_price"],
            t["open_price"],
            t["high_price"],
            t["low_price"],
            t["volume"],
            t["quote_volume"],
            t["close_time"],
        )
        for t in tickers
    ]

    with conn.cursor() as cur:
        execute_values(cur, insert_sql, values)
    conn.commit()
    print(f"{len(tickers)} lignes insérées dans binance_tickers.")

# ==========================
# 6. MAIN
# ==========================
def main():
    print("Vérification / création de la base de données...")
    ensure_database_exists()

    print("Récupération des données Binance...")
    tickers = fetch_binance_tickers()
    print(f" {len(tickers)} tickers récupérés.")

    print("Connexion à la base projet...")
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_TARGET_DB,
        user=PG_USER,
        password=PG_PWD
    )

    try:
        create_table_if_not_exists(conn)
        insert_tickers(conn, tickers)
    finally:
        conn.close()
        print("Connexion PostgreSQL fermée.")



def job_ingestion():
    """Une exécution complète du batch d’ingestion."""
    main()  # ta fonction main déjà définie plus haut
