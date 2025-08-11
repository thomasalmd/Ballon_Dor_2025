import os, json, time, datetime as dt
import requests
import pandas as pd
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
import snowflake.connector

load_dotenv()

API_KEY = os.getenv("APIFOOTBALL_KEY")
if not API_KEY:
    raise RuntimeError("APIFOOTBALL_KEY manquant dans .env")

HEADERS = {"x-apisports-key": API_KEY}
BASE = "https://v3.football.api-sports.io"

LEAGUES = [2,39,140,61,78,135]  # UCL + top5 ligues
SEASON = 2024
ts_now = int(dt.datetime.utcnow().timestamp())

# Azure
AZURE_CS = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_CONTAINER = os.getenv("AZURE_CONTAINER", "football-raw")
if AZURE_CS:
    blob_service = BlobServiceClient.from_connection_string(AZURE_CS)
    container = blob_service.get_container_client(AZURE_CONTAINER)
    try:
        container.create_container()
    except Exception:
        pass
else:
    container = None

def get(endpoint, params):
    r = requests.get(f"{BASE}/{endpoint}", headers=HEADERS, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def upload_blob(path, data):
    if container:
        container.upload_blob(name=path, data=data, overwrite=True)

# Snowflake
conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    role=os.getenv("SNOWFLAKE_ROLE"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
)
cur = conn.cursor()

def insert_json(table, league, payload):
    cur.execute(
        f"INSERT INTO {table} (ingest_ts, league, payload) SELECT CURRENT_TIMESTAMP(), %s, PARSE_JSON(%s)",
        (league, json.dumps(payload))
    )

def ingest_league(league):
    topscorers = get("players/topscorers", {"league": league, "season": SEASON})
    topassists = get("players/topassists", {"league": league, "season": SEASON})
    players = []
    if topscorers.get("response"):
        for p in topscorers["response"][:50]:
            pid = p["player"]["id"]
            pj = get("players", {"id": pid, "season": SEASON})
            players.append(pj)
            time.sleep(0.25)  # limiter les appels

    # Archive Azure
    upload_blob(f"raw/{SEASON}/league={league}/topscorers_{ts_now}.json", json.dumps(topscorers))
    upload_blob(f"raw/{SEASON}/league={league}/topassists_{ts_now}.json", json.dumps(topassists))
    upload_blob(f"raw/{SEASON}/league={league}/players_{ts_now}.json", json.dumps(players))

    # Insert Snowflake
    insert_json("FOOTBALL.RAW.TOPSCORERS_JSON", league, topscorers)
    insert_json("FOOTBALL.RAW.TOPASSISTS_JSON", league, topassists)
    for pj in players:
        insert_json("FOOTBALL.RAW.PLAYERS_JSON", league, pj)

def main():
    for lg in LEAGUES:
        print(f"Ingestion league={lg}")
        ingest_league(lg)
    conn.commit()
    print("Ingestion OK")

if __name__ == "__main__":
    main()
