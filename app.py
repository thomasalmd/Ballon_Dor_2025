# app.py — Ballon d'Or 2025 (Streamlit)
import os
import numpy as np
import pandas as pd
import streamlit as st
import snowflake.connector
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from dotenv import load_dotenv

# --------- App setup ----------
st.set_page_config(page_title="Ballon d'Or 2025", layout="wide")
load_dotenv()

try:
    import streamlit as st
    for k, v in st.secrets.items():
        os.environ.setdefault(k, str(v))
except Exception:
    pass



ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT")
USER      = os.getenv("SNOWFLAKE_USER")
PASSWORD  = os.getenv("SNOWFLAKE_PASSWORD")  # Laisse vide si SSO
ROLE      = os.getenv("SNOWFLAKE_ROLE")
WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
DB        = "FOOTBALL"
SC        = "ANALYTICS"
VIEW_NAME = "V_PLAYER_SEASON"  # vue clean & unique côté Snowflake

# Petit bandeau debug
st.caption(f"Snowflake → role={ROLE} · warehouse={WAREHOUSE} · {DB}.{SC}.{VIEW_NAME}")

# --------- Connexion Snowflake ----------
def get_conn():
    auth_kwargs = dict(
        account=ACCOUNT, user=USER, role=ROLE,
        warehouse=WAREHOUSE, database=DB, schema=SC
    )
    if PASSWORD:
        auth_kwargs["password"] = PASSWORD
    else:
        auth_kwargs["authenticator"] = "externalbrowser"  # SSO/AzureAD
    return snowflake.connector.connect(**auth_kwargs)

@st.cache_data(show_spinner=True, ttl=300)
def load_data():
    conn = get_conn()
    q = f"""
      SELECT player_name, team, position, league_name, age, minutes, nineties,
             gls, ast, sh, sot, xg, npxg, xag,
             gls_p90, ast_p90, sot_p90, xg_p90, npxg_p90, xag_p90
      FROM {DB}.{SC}.{VIEW_NAME}
    """
    try:
        df = pd.read_sql(q, conn)
    finally:
        conn.close()
    df.columns = [c.lower() for c in df.columns]

    # poste → catégorie
    def map_pos(p):
        p = (p or "").upper()
        if "GK" in p: return "GK"
        if "DF" in p or p.startswith("D"): return "DEF"
        if "FW" in p or p.startswith("F"): return "FWD"
        if "MF" in p or p.startswith("M"): return "MID"
        return "MID"
    df["pos_cat"] = df["position"].map(map_pos)
    return df

try:
    df = load_data()
except Exception as e:
    st.error(f"Impossible de charger {DB}.{SC}.{VIEW_NAME} : {e}")
    st.stop()

if df.empty:
    st.warning("La vue est vide. Vérifie que FOOTBALL.ANALYTICS.V_PLAYER_SEASON a des lignes.")
    st.stop()

# --------- UI — Filtres & Poids ----------
st.sidebar.header("Filtres & Poids")

leagues = sorted(df["league_name"].dropna().unique().tolist())
positions = ["FWD","MID","DEF","GK"]

selected_leagues = st.sidebar.multiselect("Compétitions", leagues, default=leagues)
selected_positions = st.sidebar.multiselect("Postes", positions, default=["FWD","MID","DEF"])

st.sidebar.markdown("---")
min_nineties = st.sidebar.slider("Minimum de matchs (x90)", 0.0, 38.0, 10.0, 0.5)
K_shrink     = st.sidebar.slider("Régularisation K (shrinkage)", 0.0, 30.0, 10.0, 1.0)
minutes_ref  = st.sidebar.slider("Cap minutes (score max à ... x90)", 5.0, 38.0, 15.0, 1.0)

st.sidebar.markdown("---")
st.sidebar.subheader("Poids par compétition")
comp_weights = {lg: st.sidebar.slider(lg, 0.5, 3.0, 2.0, 0.1) for lg in leagues}

st.sidebar.markdown("---")
st.sidebar.subheader("Critères par poste (per 90)")
default_metric_weights = {
    "FWD": {"gls_p90":0.45, "ast_p90":0.10, "sot_p90":0.15, "xg_p90":0.15, "npxg_p90":0.10, "xag_p90":0.05},
    "MID": {"gls_p90":0.15, "ast_p90":0.35, "sot_p90":0.05, "xg_p90":0.15, "npxg_p90":0.10, "xag_p90":0.20},
    "DEF": {"gls_p90":0.05, "ast_p90":0.10, "sot_p90":0.05, "xg_p90":0.05, "npxg_p90":0.05, "xag_p90":0.10},
    "GK" : {"gls_p90":0.00, "ast_p90":0.00, "sot_p90":0.00, "xg_p90":0.00, "npxg_p90":0.00, "xag_p90":0.00},
}
metric_weights = {}
with st.sidebar.expander("Ajuster les poids métriques par poste"):
    for pos in positions:
        metric_weights[pos] = {}
        st.markdown(f"**{pos}**")
        s = 0.0
        for m, w in default_metric_weights[pos].items():
            metric_weights[pos][m] = st.slider(f"{pos} · {m}", 0.0, 1.0, float(w), 0.05, key=f"{pos}-{m}")
            s += metric_weights[pos][m]
        s = s or 1.0
        for m in metric_weights[pos]:
            metric_weights[pos][m] /= s  # normalise à 1

st.sidebar.markdown("---")
lambda_rate = st.sidebar.slider("Mix per-90 vs totaux (λ)", 0.0, 1.0, 0.6, 0.05)
topn = st.sidebar.slider("Top N", 5, 50, 10, 1)

# --------- Préparation des données ----------
features_p90 = ["gls_p90","ast_p90","sot_p90","xg_p90","npxg_p90","xag_p90"]
features_tot = ["gls","ast","sot","xg","npxg","xag"]

dfv = df[
    df["league_name"].isin(selected_leagues) &
    df["pos_cat"].isin(selected_positions) &
    (df["nineties"].astype(float) >= float(min_nineties))
].copy()

if dfv.empty:
    st.warning("Aucune donnée après filtres : baisse le seuil de 90s ou élargis les ligues/postes.")
    st.stop()

# Appliquer poids compétitions
dfv["comp_weight"] = dfv["league_name"].map(comp_weights).astype(float)
for f in features_p90 + features_tot:
    dfv[f] = pd.to_numeric(dfv[f], errors="coerce") * dfv["comp_weight"]

# Shrinkage des per-90 (ramener vers moyenne ligue si peu de matchs)
def shrink_column(colname: str):
    mu = dfv[colname].mean(skipna=True)
    r = dfv["nineties"] / (dfv["nineties"] + K_shrink)  # [0..1]
    return mu + r * (dfv[colname] - mu)

for f in features_p90:
    dfv[f + "_adj"] = shrink_column(f)

# Z-scores (imputation médiane)
def zscore_block(cols):
    X = dfv[cols].values
    X_imp = SimpleImputer(strategy="median").fit_transform(X)
    Z = StandardScaler().fit_transform(X_imp)
    return pd.DataFrame(Z, columns=[f"z_{c}" for c in cols], index=dfv.index)

Z_rate  = zscore_block([f + "_adj" for f in features_p90])   # per90 ajustés
Z_total = zscore_block(features_tot)                         # totaux
dfv = pd.concat([dfv, Z_rate, Z_total], axis=1)

# Score par poste (mix per-90 ajustés et totaux)
map_tot = {"gls_p90":"gls","ast_p90":"ast","sot_p90":"sot","xg_p90":"xg","npxg_p90":"npxg","xag_p90":"xag"}

def row_score(row):
    pos = row["pos_cat"]
    w = metric_weights.get(pos, default_metric_weights[pos])
    s_rate = sum(row[f"z_{m}_adj"] * w[m] for m in w)
    s_tot  = sum(row[f"z_{map_tot[m]}"] * w[m] for m in w)
    return lambda_rate * s_rate + (1 - lambda_rate) * s_tot

dfv["raw_score"] = dfv.apply(row_score, axis=1)

# Facteur minutes (cap à 1 autour de minutes_ref x90) — racine pour lisser
minutes_factor = np.minimum(dfv["nineties"] / minutes_ref, 1.0) ** 0.5
dfv["ballon_score"] = dfv["raw_score"] * minutes_factor

# --------- Affichage ----------
st.title("🏆 Ballon d'Or 2025 — Classement interactif")
st.caption("Données FBref (Kaggle) · pondération par compétition · critères adaptés au poste · per-90 régularisés · mix per-90/totaux · facteur minutes")

cols_show = ["player_name","team","position","league_name","minutes","nineties",
             "gls","ast","sot","xg","npxg","xag",
             "gls_p90","ast_p90","sot_p90","xg_p90","npxg_p90","xag_p90",
             "ballon_score"]

dfv_sorted = dfv.sort_values("ballon_score", ascending=False)

st.subheader("Top joueurs (selon vos réglages)")
st.dataframe(dfv_sorted[cols_show].head(topn), use_container_width=True)

tabs = st.tabs(selected_leagues)
for i, lg in enumerate(selected_leagues):
    with tabs[i]:
        sub = dfv_sorted[dfv_sorted["league_name"] == lg].head(topn)
        st.write(f"**Top {topn} — {lg}**")
        st.dataframe(sub[cols_show], use_container_width=True)

st.download_button(
    "⬇️ Export CSV (classement filtré)",
    data=dfv_sorted[cols_show].to_csv(index=False).encode("utf-8"),
    file_name="ballon_dor_ranking.csv",
    mime="text/csv"
)
