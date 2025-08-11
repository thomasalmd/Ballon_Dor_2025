import os
import pandas as pd
import snowflake.connector
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer

conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    role=os.getenv("SNOWFLAKE_ROLE"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database="FOOTBALL",
    schema="ANALYTICS",
)

q = "SELECT player_id, player_name, w_goals, w_assists, avg_rating, w_shots_on, w_key_passes FROM V_PLAYER_AGG"
df = pd.read_sql(q, conn)

num = ["w_goals","w_assists","avg_rating","w_shots_on","w_key_passes"]
imp = SimpleImputer(strategy="median")
X = imp.fit_transform(df[num])

scaler = StandardScaler()
Z = scaler.fit_transform(X)

weights = [0.40, 0.25, 0.20, 0.10, 0.05]
df["ballon_score"] = (Z * weights).sum(axis=1)

top10 = df.sort_values("ballon_score", ascending=False).head(10)
print(top10[["player_name","ballon_score"]].to_string(index=False))
