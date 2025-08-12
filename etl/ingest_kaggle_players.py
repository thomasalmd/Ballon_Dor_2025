import os, glob
import pandas as pd
from dotenv import load_dotenv
from kaggle.api.kaggle_api_extended import KaggleApi
from azure.storage.blob import BlobServiceClient
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

# --- Kaggle download ---
DATASET = "hubertsidorowicz/football-players-stats-2024-2025"  # FBref Big-5 24/25
DL_DIR = "data/kaggle"
os.makedirs(DL_DIR, exist_ok=True)

api = KaggleApi()
api.authenticate()
api.dataset_download_files(DATASET, path=DL_DIR, unzip=True)

# Concat de tous les CSV téléchargés
files = glob.glob(os.path.join(DL_DIR, "*.csv"))
dfs = []
for f in files:
    df = pd.read_csv(f)
    df["source_file"] = os.path.basename(f)
    dfs.append(df)
raw = pd.concat(dfs, ignore_index=True)

# Harmonise quelques colonnes (suivant FBref)
# Colonnes typiques: Player, Nation, Pos, Squad, Comp, Age, Born, MP, Starts, Min, 90s, Gls, Ast, PK, PKatt, Sh, SoT, xG, npxG, xAG, npxG+xAG, ...
cols = [c for c in raw.columns]
keep = [x for x in ["Player","Nation","Pos","Squad","Comp","Age","Min","90s","Gls","Ast","Sh","SoT","xG","npxG","xAG","npxG+xAG"] if x in cols]
dfp = raw[keep].copy()

# Nettoyages rapides
for c in ["Age","Min","90s","Gls","Ast","Sh","SoT","xG","npxG","xAG"]:
    if c in dfp.columns:
        dfp[c] = pd.to_numeric(dfp[c], errors="coerce")

dfp.rename(columns={"npxG+xAG":"npxG_xAG"}, inplace=True)

# --- Azure archive (optionnel) ---
AZURE_CS = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZ_CONTAINER = os.getenv("AZURE_CONTAINER", "football-raw")
if AZURE_CS:
    svc = BlobServiceClient.from_connection_string(AZURE_CS)
    cont = svc.get_container_client(AZ_CONTAINER)
    try: cont.create_container()
    except Exception: pass
    # upload parquet léger
    parquet_path = os.path.join(DL_DIR, "players_2425.parquet")
    dfp.to_parquet(parquet_path, index=False)
    with open(parquet_path, "rb") as fh:
        cont.upload_blob(name="kaggle/players_2425.parquet", data=fh, overwrite=True)
    print("[AZURE] players_2425.parquet upload OK")

# --- Snowflake load ---
conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    role=os.getenv("SNOWFLAKE_ROLE"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),  # RAW
)
cur = conn.cursor()

cur.execute("""
CREATE OR REPLACE TABLE FOOTBALL.RAW.KAGGLE_PLAYERS_2425 (
  PLAYER STRING,
  NATION STRING,
  POS STRING,
  SQUAD STRING,
  COMP STRING,
  AGE FLOAT,
  MINUTES NUMBER,
  NINETIES FLOAT,
  GLS NUMBER,
  AST NUMBER,
  SH NUMBER,
  SOT NUMBER,
  XG FLOAT,
  NPXG FLOAT,
  XAG FLOAT,
  NPXG_XAG FLOAT,
  SOURCE_FILE STRING
);
""")

# Aligne DF sur le schéma
for col in ["Player","Nation","Pos","Squad","Comp","Age","Min","90s","Gls","Ast","Sh","SoT","xG","npxG","xAG","npxG_xAG","source_file"]:
    if col not in dfp.columns:
        dfp[col] = None
dfp = dfp[["Player","Nation","Pos","Squad","Comp","Age","Min","90s","Gls","Ast","Sh","SoT","xG","npxG","xAG","npxG_xAG","source_file"]]
dfp.columns = ["PLAYER","NATION","POS","SQUAD","COMP","AGE","MINUTES","NINETIES","GLS","AST","SH","SOT","XG","NPXG","XAG","NPXG_XAG","SOURCE_FILE"]

ok, nchunks, nrows, _ = write_pandas(conn, dfp, table_name="KAGGLE_PLAYERS_2425", database="FOOTBALL", schema="RAW", overwrite=True)
print(f"[SF] LOAD OK → rows={nrows}")

cur.close(); conn.close()
print("[SUCCESS] Kaggle → Azure → Snowflake terminé ✅")
