# -*- coding: utf-8 -*-
"""
Created on Mon Dec 15 12:00:14 2025

@author: HP
"""

"""
Configuration locale pour le projet Binance + Spark + Postgres.

Chaque utilisateur doit ADAPTER ce fichier à sa machine.
"""


# ==========================
# 1. Config PostgreSQL
# ==========================

# Hôte de la base PostgreSQL (en local la plupart du temps)
PG_HOST = "localhost"

# Port par défaut de PostgreSQL
PG_PORT = 5432

# Base "super" pour créer la base projet (en général 'postgres')
PG_SUPER_DB = "postgres"

# Utilisateur PostgreSQL (souvent 'postgres' en local)
PG_USER = "postgres"

#  À CHANGER : mot de passe PostgreSQL de l’utilisateur ci-dessus
PG_PWD = "Khady77@"

# Nom de la base projet utilisée dans ce travail
PG_TARGET_DB = "crypto_db"


# ==========================
# 2. Config email (Gmail)
# ==========================

# À CHANGER : votre adresse Gmail
EMAIL_USER = "khadijaatou180702@gmail.com"

# À CHANGER : mot de passe d’application Gmail
# (pas le vrai mot de passe ! le mot de passe d’appli à 16 caractères)
EMAIL_PWD = "txxdfbyzthuuumcq"

#liste des destinataires
destinataires = [
    "khadijaatou180702@gmail.com",
    "fogwoungsarahlaure@gmail.com",
    "lawafoum@gmail.com",
    "a.segadiallo@gmail.com"
]
# ==========================
# 3. Paramètres du batch
# ==========================

# Intervalle entre deux batchs (en secondes)
    # Pour un vrai batch mettre par exemple 6h : 6 * 60 * 60
BATCH_INTERVAL_SECONDS = 10 * 60 # nous avons mis 10 minutes pour des besoins de test

# Master Spark (laisse "local[*]" pour exécuter sur la machine)
SPARK_MASTER = "local[*]"
