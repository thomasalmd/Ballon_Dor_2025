# Ballon d’Or 2025 — Kaggle → Azure → Snowflake → Streamlit

Ce dépôt implémente une chaîne **data science / data engineering** complète pour explorer les performances des joueurs (saison 2024-25) et calculer un score “Ballon d’Or” paramétrable dans une application **Streamlit**.

## Architecture (vue d’ensemble)

```
Kaggle (FBref CSV)
        │  (kaggle API)
        ▼
ETL Python ──► Azure Blob (parquet archive)
        │
        └──► Snowflake (RAW → ANALYTICS vues)
                          │
                          └──► App Streamlit (lecture Snowflake OU Azure)
                                   │
                                   └──► GitHub (code + CI/Actions + déploiement Streamlit Cloud)
```

* **Kaggle** : source gratuite des stats joueurs (xG, buts, passes, minutes, …).
* **ETL** : script Python `etl/ingest_kaggle_players.py` (télécharge, nettoie, archive Azure, charge Snowflake).
* **Snowflake** : zone **RAW** (table parquetisée) + vues **ANALYTICS** (dédoublonnage, per-90, normalisation ligues).
* **Streamlit** : application interactive (poids par ligue, critères par poste, anti-biais “peu de minutes”, export).
* **Azure-only (fallback)** : l’app peut lire directement un **Parquet** dans Azure si l’accès Snowflake n’est plus disponible.
* **GitHub** : versionnage, PR/branches, Actions (CI et ingestion planifiée), déploiement Streamlit Cloud.

---

# Structure du dépôt

```
.
├─ app.py                       # Application Streamlit (UI, scoring, graphes, exports)
├─ etl/
│   └─ ingest_kaggle_players.py # ETL Kaggle → Azure → Snowflake (write_pandas)
├─ sql/
│   └─ analytics.sql            # Vues ANALYTICS (dédoublonnage, per-90, mapping ligues, agrégations)
├─ .streamlit/
│   └─ config.toml              # Thème Streamlit (facultatif)
├─ requirements.txt             # Dépendances de l’app
├─ requirements-etl.txt         # Dépendances de l’ETL (optionnel si séparées)
├─ .env.example                 # Variables d’environnement (exemple, sans secrets)
└─ .github/workflows/
    ├─ ci.yml                   # CI: build + smoke imports
    └─ ingest.yml               # Ingestion planifiée/à la demande (Kaggle→Azure→Snowflake)
```

---

# Branches (stratégie)

* **`main`** : branche stable, déployée (Streamlit Cloud suit généralement `main`).
* **`ui-refresh`** : branche de refonte UI/UX (thème, explications, graphiques). Travaux fusionnés via PR vers `main`.
* **`feature/*`** : branches de fonctionnalité (ex. `feature/azure-only-mode`, `feature/etl-kaggle`), PR → `main`.
* **`hotfix/*`** : correctifs rapides si nécessaire.

> Flux recommandé :
> `git checkout -b feature/xxx` → commit/push → **Pull Request** vers `main` → **merge** (squash) → suppression de la branche.

---

# Prérequis

* Python **3.11** (recommandé), `pip`, `git`.
* Comptes et accès :

  * **Kaggle** (API token).
  * **Azure Storage** (Blob / container).
  * **Snowflake** (DB `FOOTBALL`, schémas `RAW`, `ANALYTICS`, WH `BALLON_WH`).
  * **GitHub** (repo) et, si besoin, **Streamlit Community Cloud**.

---

# Variables d’environnement

Créer un fichier `.env` (local) à partir de `.env.example` :

```
# Snowflake (local / dev)
SNOWFLAKE_ACCOUNT=xxxxxx-xxxxxx         # ajouter le suffixe région si présent (ex: eu-west-3)
SNOWFLAKE_USER=...
SNOWFLAKE_PASSWORD=...                  # obligatoire en Cloud (pas de SSO)
SNOWFLAKE_ROLE=SYSADMIN                 # ou rôle de travail
SNOWFLAKE_WAREHOUSE=BALLON_WH
SNOWFLAKE_DATABASE=FOOTBALL
SNOWFLAKE_SCHEMA=RAW                    # pour l’ETL ; l’app lit ANALYTICS

# Azure
AZURE_STORAGE_CONNECTION_STRING=...
AZURE_CONTAINER=football-raw

# Kaggle
KAGGLE_USERNAME=...
KAGGLE_KEY=...
```

**Streamlit Cloud (déploiement)** : saisir les **Secrets** dans *Settings → Secrets* (ils ne viennent pas de GitHub). L’app lit `st.secrets` et `os.environ`.

---

# Installation & exécution locale

```bash
# 1) Clonage
git clone <repo>
cd <repo>

# 2) Environnement Python
python -m venv .venv
# Windows:
. .\.venv\Scripts\Activate.ps1
# macOS/Linux:
source .venv/bin/activate

# 3) Dépendances app
pip install -U pip
pip install -r requirements.txt

# 4) Lancer l’app
streamlit run app.py
```

---

# Ingestion des données (Kaggle → Azure → Snowflake)

1. **Configurer Kaggle** (placer `kaggle.json` dans `~/.kaggle/` ou utiliser `KAGGLE_USERNAME`/`KAGGLE_KEY`).
2. **Configurer Snowflake** (DB `FOOTBALL`, schémas `RAW` et `ANALYTICS` existants et accessibles).
3. **Exécuter l’ETL** :

```bash
# dépendances ETL si séparées
pip install -r requirements-etl.txt

python etl/ingest_kaggle_players.py
```

Effets :

* Téléchargement du dataset Kaggle (FBref 24/25), concaténation, nettoyage léger.
* **Archive Azure** (`/kaggle/players_2425.parquet`).
* **Table** `FOOTBALL.RAW.KAGGLE_PLAYERS_2425` (via `write_pandas`).

---

# Modèle de données & vues Snowflake

* **RAW**

  * `KAGGLE_PLAYERS_2425` : table unifiée joueurs avec colonnes clés (`PLAYER`, `POS`, `SQUAD`, `COMP`, `AGE`, `MINUTES`, `NINETIES`, `GLS`, `AST`, `SOT`, `XG`, `NPXG`, `XAG`, …).

* **ANALYTICS**

  * `V_PLAYER_SEASON` : vue **dédoublonnée / par saison et ligue** :

    * Normalisation des ligues (`eng Premier League` → `Premier League`, etc.).
    * **Agrégation** multi-lignes par joueur (transferts) ⇒ minutes, buts, passes, xG cumulés.
    * Sélection **équipe** et **poste** dominants (plus de minutes).
    * Calcul **per-90** : `*_p90 = total / nineties`.
  * (optionnel) `REF_COMPETITIONS` : poids par ligue.
  * (optionnel) `V_PLAYER_AGG` / `V_BALLON_SCORE` : agrégations pondérées, z-scores SQL.

> L’app réalise le **scoring** côté Python (z-scores, shrinkage, mix per-90 vs totaux, facteur minutes) pour permettre les réglages interactifs.

---

# Application Streamlit (fonctionnalités)

* **Filtres** : compétitions, postes, âge (dans la fiche), **minimum de matchs** (x90).
* **Anti-biais petits échantillons** :

  * **Shrinkage** des métriques per-90 vers la moyenne quand `nineties` est faible (`K` réglable).
  * **Facteur minutes** plafonné (atteint 1 vers `minutes_ref`, courbe lissée).
* **Mix qualité/volume** : combinaison réglable des **per-90** et des **totaux** via `λ` (0→volume, 1→qualité).
* **Poids par compétition** : multiplicateurs par ligue (Premier League, Liga, …).
* **Critères par poste** : profils prédéfinis (Équilibré / Attaque / Création / Défense) + sliders détaillés.
* **Sorties** : Top global, Tops par compétition (onglets), graphique barres, fiche joueur, **export CSV/JSON**.

Lancement local :

```bash
streamlit run app.py
```

---

# Mode **Azure-only** (sans Snowflake)

Si l’accès Snowflake n’est plus disponible, l’app peut lire un **Parquet** stocké dans Azure (exporté depuis Snowflake ou produit par l’ETL).

1. Export Snowflake → Azure (depuis Snowsight) :

```sql
USE DATABASE FOOTBALL; USE SCHEMA ANALYTICS; USE WAREHOUSE BALLON_WH;

CREATE OR REPLACE STAGE AZURE_EXPORT
  URL='azure://<account>.blob.core.windows.net/<container>'
  CREDENTIALS=(AZURE_SAS_TOKEN='<SAS_sans_le_?>');

COPY INTO @AZURE_EXPORT/exports/v_player_season.parquet
FROM (SELECT * FROM FOOTBALL.ANALYTICS.V_PLAYER_SEASON)
FILE_FORMAT=(TYPE=PARQUET COMPRESSION=SNAPPY)
OVERWRITE=TRUE
SINGLE=TRUE;
```

2. Secrets Streamlit (ou `.env`) :

```
AZURE_STORAGE_CONNECTION_STRING = "<connection_string>"
AZURE_CONTAINER = "<container>"
AZURE_EXPORT_PATH = "exports/v_player_season.parquet"
```

L’app détecte ces variables et bascule automatiquement sur la lecture **Azure** (sinon elle lit Snowflake).

---

# CI/CD (GitHub Actions & déploiement)

* **CI (`.github/workflows/ci.yml`)** : à chaque push/PR, installation des dépendances et **smoke test d’imports** (sanité).
* **Ingestion planifiée (`ingest.yml`)** :

  * `schedule` (cron) et/ou `workflow_dispatch`.
  * Utilise les **GitHub Secrets** (`SNOWFLAKE_*`, `AZURE_*`, `KAGGLE_*`) pour exécuter `etl/ingest_kaggle_players.py`.
* **Streamlit Community Cloud** :

  * Déploiement auto sur **push** de la branche suivie (ex. `main`).
  * Les **secrets Streamlit** sont saisis dans l’interface Streamlit (indépendants des GitHub Secrets).

---

# Déploiement Streamlit Cloud (résumé)

1. Connecter le repo → **New app** → branch `main` → `app.py`.
2. **Settings → Secrets** : saisir `SNOWFLAKE_*` (avec **password**, pas de SSO) **ou** secrets Azure pour le mode Azure-only.
3. Option “Automatically update on push” : **ON**.

---

# Dépannage (FAQ)

* **`'NoneType' object has no attribute 'find'`** au démarrage :
  `SNOWFLAKE_ACCOUNT` absent → secrets manquants (Streamlit) ou `.env` non chargé (local).

* **PyArrow incompatible / warnings Snowflake** :
  épingler `pyarrow==18.1.0` dans `requirements.txt`.

* **Vue introuvable** (`FOOTBALL.ANALYTICS.V_PLAYER_SEASON does not exist`) :
  exécuter le SQL des vues (fichier `sql/analytics.sql`) avec un rôle ayant les droits, vérifier la DB/SC.

* **SSO en Cloud** :
  non supporté sur Streamlit Cloud → fournir **SNOWFLAKE\_PASSWORD**.

* **Données vides / scores tous nuls** :
  mapping des ligues non concordant → utiliser la normalisation (`eng …` → `Premier League`) et/ou `COALESCE` des poids.

---

# Données & crédits

* **Source** : FBref (via dataset Kaggle “Football Players Stats 2024-2025”).
* **Usage** : à des fins d’analyse/démonstration ; la méthodologie de score est **paramétrable** et non officielle.

---

# Licence

Ce projet est fourni “en l’état”, à des fins d’apprentissage et de démonstration. Adapter la licence selon votre politique interne (MIT, Apache-2.0, etc.).

---

# Commandes utiles (mémo)

```bash
# Environnement
python -m venv .venv
. .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

# App
streamlit run app.py

# ETL
pip install -r requirements-etl.txt
python etl/ingest_kaggle_players.py

# Git (branche UI)
git checkout -b ui-refresh
git add app.py requirements.txt .streamlit/config.toml
git commit -m "UI: thème + presets + explications + charts"
git push -u origin ui-refresh
# ouvrir une Pull Request vers main, puis merger
```