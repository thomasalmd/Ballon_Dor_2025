import os
import pandas as pd
import snowflake.connector
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from dotenv import load_dotenv

# 1) Charger .env
load_dotenv()

# 2) Sanity check env vars
acct = os.getenv("SNOWFLAKE_ACCOUNT")
user = os.getenv("SNOWFLAKE_USER")
role = os.getenv("SNOWFLAKE_ROLE")
wh   = os.getenv("SNOWFLAKE_WAREHOUSE")

if not all([acct, user, wh]):
    raise RuntimeError(f"Env manquantes: "
                       f"SNOWFLAKE_ACCOUNT={acct}, SNOWFLAKE_USER={user}, SNOWFLAKE_WAREHOUSE={wh}")

# 3) Connexion (si tu es en SSO, décommente authenticator)
conn = snowflake.connector.connect(
    account=acct,
    user=user,
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    role=role,
    warehouse=wh,
    database="FOOTBALL",
    schema="ANALYTICS",
    # authenticator="externalbrowser",  # <= si tu utilises SSO/AzureAD
)

q = "SELECT player_name, team, w_goals, w_assists, w_sot, w_xg, w_npxg FROM V_PLAYER_AGG"
df = pd.read_sql(q, conn)

print("[DBG] rows, cols =", df.shape, list(df.columns)[:10])  # debug
# Normaliser les noms de colonnes en minuscules
df.columns = [c.lower() for c in df.columns]

feat = ["w_goals","w_assists","w_sot","w_xg","w_npxg"]  # inchangé
if df.empty:
    raise RuntimeError("V_PLAYER_AGG est vide. Vérifie les vues et les données RAW.")

from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler

X = SimpleImputer(strategy="median").fit_transform(df[feat])
Z = StandardScaler().fit_transform(X)
weights = [0.45, 0.30, 0.10, 0.10, 0.05]
df["ballon_score"] = (Z * weights).sum(axis=1)

top10 = df.sort_values("ballon_score", ascending=False).head(10)
print(top10[["player_name","team","ballon_score"]].to_string(index=False))
