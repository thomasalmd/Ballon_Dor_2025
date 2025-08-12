# app.py — Ballon d'Or 2025, UI pro & grand public
import os, json
import numpy as np
import pandas as pd
import altair as alt
import streamlit as st
import snowflake.connector
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from dotenv import load_dotenv

# --------- Setup & secrets ----------
st.set_page_config(page_title="Ballon d'Or 2025", layout="wide")
load_dotenv()

# En Cloud, récupérer Streamlit Secrets -> env
try:
    for k, v in st.secrets.items():
        os.environ.setdefault(k, str(v))
except Exception:
    pass

ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT")
USER      = os.getenv("SNOWFLAKE_USER")
PASSWORD  = os.getenv("SNOWFLAKE_PASSWORD")  # Cloud: obligatoire (pas de SSO)
ROLE      = os.getenv("SNOWFLAKE_ROLE")
WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
DB        = "FOOTBALL"
SC        = "ANALYTICS"
VIEW_NAME = "V_PLAYER_SEASON"  # vue unique + per90

REQUIRED = ["SNOWFLAKE_ACCOUNT","SNOWFLAKE_USER","SNOWFLAKE_WAREHOUSE"]
missing = [k for k in REQUIRED if not os.getenv(k)]
if missing:
    st.error("Secrets manquants : " + ", ".join(missing))
    st.stop()

# --------- Connexion Snowflake ----------
def get_conn():
    kwargs = dict(account=ACCOUNT, user=USER, role=ROLE, warehouse=WAREHOUSE, database=DB, schema=SC)
    if PASSWORD:
        kwargs["password"] = PASSWORD
    else:
        kwargs["authenticator"] = "externalbrowser"  # local SSO
    return snowflake.connector.connect(**kwargs)

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
    # Position -> catégorie standard
    def map_pos(p):
        p = (p or "").upper()
        if "GK" in p: return "GK"
        if "DF" in p or p.startswith("D"): return "DEF"
        if "FW" in p or p.startswith("F"): return "FWD"
        if "MF" in p or p.startswith("M"): return "MID"
        return "MID"
    df["pos_cat"] = df["position"].map(map_pos)
    # Nettoyages min
    for c in ["age","minutes","nineties","gls","ast","sh","sot","xg","npxg","xag",
              "gls_p90","ast_p90","sot_p90","xg_p90","npxg_p90","xag_p90"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

df = load_data()
if df.empty:
    st.warning("La vue FOOTBALL.ANALYTICS.V_PLAYER_SEASON est vide.")
    st.stop()

# --------- En-tête ----------
left, right = st.columns([0.72, 0.28])
with left:
    st.title("🏆 Ballon d'Or 2025 — Classement interactif")
    st.caption("Données FBref (Kaggle) · Score transparent : pondération par compétition, critères adaptés au poste, "
               "pénalités pour faible temps de jeu, mix per-90 / totaux, normalisation z-score.")
with right:
    st.metric("Joueurs", f"{df['player_name'].nunique():,}")
    st.metric("Ligues", f"{df['league_name'].nunique()}")

with st.expander("ℹ️ Méthodologie (simple à lire)"):
    st.markdown("""
**Objectif :** classer les meilleurs joueurs de la saison, de façon **équitable et transparente**.

**Comment le score est calculé ?**
1. **Filtrage** : on peut exclure les joueurs avec trop peu de temps de jeu (curseur *Minimum de matchs*).
2. **Per-90** *(qualité)* : buts/90, passes/90, xG/90, etc.
3. **Régularisation** *(anti-biais)* : si un joueur a peu joué, ses per-90 sont **tirés vers la moyenne** (paramètre *K*).
4. **Totaux saison** *(impact)* : buts, passes, xG cumulés → on mélange per-90 et totaux avec **λ** (0–1).
5. **Poids compétition** : Premier League, Liga, etc. → multiplicateur par ligue.
6. **Poste** : les critères n’ont pas la même importance selon l’**attaque / milieu / défense / gardien**.
7. **Temps de jeu** : un *minutes factor* atténue le score si le temps de jeu est faible.

👉 Le score final = **z-scores** (normalisés sur l’échantillon filtré), pondérés, puis ajustés par le temps de jeu.
""")

# --------- Sidebar : Filtres & Réglages ----------
st.sidebar.header("Réglages")
leagues = sorted(df["league_name"].dropna().unique().tolist())
positions = ["FWD","MID","DEF","GK"]

# Presets grand public
preset = st.sidebar.radio(
    "Profil de scoring",
    ["Équilibré", "Attaque ⚔️", "Création 🪄", "Défense 🛡️"],
    help="Des profils prêts à l'emploi qui ajustent les critères par poste."
)

selected_leagues = st.sidebar.multiselect(
    "Compétitions à inclure",
    leagues, default=leagues,
    help="Choisissez les ligues à inclure dans le classement."
)
selected_positions = st.sidebar.multiselect(
    "Postes à inclure",
    positions, default=["FWD","MID","DEF"],
    help="Sélectionnez les familles de postes (GK = gardiens)."
)

st.sidebar.markdown("---")
min_nineties = st.sidebar.slider(
    "Minimum de matchs (x90)",
    0.0, 38.0, 10.0, 0.5,
    help="Exclut les joueurs en dessous de ce temps de jeu. 10x90 ≈ 10 matches complets."
)
K_shrink = st.sidebar.slider(
    "Régularisation K (anti-biais per-90)",
    0.0, 30.0, 10.0, 1.0,
    help="Plus K est grand, plus on tire les per-90 vers la moyenne quand le joueur a peu joué."
)
minutes_ref = st.sidebar.slider(
    "Cap minutes (score max à ... x90)",
    5.0, 38.0, 15.0, 1.0,
    help="Le facteur minutes atteint 1 autour de ce seuil (racine pour lisser)."
)
st.sidebar.markdown("---")
st.sidebar.subheader("Poids par compétition")
comp_weights = {lg: st.sidebar.slider(lg, 0.5, 3.0, 2.0, 0.1,
                                      help="Multiplicateur pour refléter l'intensité de la ligue.")
                for lg in leagues}

st.sidebar.markdown("---")
st.sidebar.subheader("Mix per-90 vs totaux")
lambda_rate = st.sidebar.slider(
    "λ — importance des per-90",
    0.0, 1.0, 0.6, 0.05,
    help="0 = 100% totaux (volume), 1 = 100% per-90 (qualité)."
)

# Poids par poste (défaut selon preset)
def default_metric_weights(preset_name: str):
    base = {
        "FWD": {"gls_p90":0.45, "ast_p90":0.10, "sot_p90":0.15, "xg_p90":0.15, "npxg_p90":0.10, "xag_p90":0.05},
        "MID": {"gls_p90":0.15, "ast_p90":0.35, "sot_p90":0.05, "xg_p90":0.15, "npxg_p90":0.10, "xag_p90":0.20},
        "DEF": {"gls_p90":0.05, "ast_p90":0.10, "sot_p90":0.05, "xg_p90":0.05, "npxg_p90":0.05, "xag_p90":0.10},
        "GK" : {"gls_p90":0.00, "ast_p90":0.00, "sot_p90":0.00, "xg_p90":0.00, "npxg_p90":0.00, "xag_p90":0.00},
    }
    if preset_name.startswith("Attaque"):
        base["FWD"] = {"gls_p90":0.55,"ast_p90":0.10,"sot_p90":0.15,"xg_p90":0.15,"npxg_p90":0.05,"xag_p90":0.00}
    if preset_name.startswith("Création"):
        base["MID"] = {"gls_p90":0.10,"ast_p90":0.45,"sot_p90":0.05,"xg_p90":0.15,"npxg_p90":0.10,"xag_p90":0.15}
    if preset_name.startswith("Défense"):
        base["DEF"] = {"gls_p90":0.00,"ast_p90":0.10,"sot_p90":0.00,"xg_p90":0.05,"npxg_p90":0.05,"xag_p90":0.10}
    return base

weights_default = default_metric_weights(preset)

with st.sidebar.expander("Affiner les critères par poste (per-90)", expanded=False):
    metric_weights = {}
    for pos in ["FWD","MID","DEF","GK"]:
        st.markdown(f"**{pos}**")
        metric_weights[pos] = {}
        s = 0.0
        for m, w in weights_default[pos].items():
            metric_weights[pos][m] = st.slider(f"{pos} · {m}",
                                               0.0, 1.0, float(w), 0.05, key=f"{pos}-{m}",
                                               help="Poids relatif de cette métrique per-90 pour ce poste.")
            s += metric_weights[pos][m]
        s = s or 1.0
        for m in metric_weights[pos]:
            metric_weights[pos][m] /= s  # normalise à 1

topn = st.sidebar.slider("Top N", 5, 50, 10, 1)

# --------- Préparation dataset ----------
features_p90 = ["gls_p90","ast_p90","sot_p90","xg_p90","npxg_p90","xag_p90"]
features_tot = ["gls","ast","sot","xg","npxg","xag"]

dfv = df[
    df["league_name"].isin(selected_leagues) &
    df["pos_cat"].isin(selected_positions) &
    (df["nineties"].astype(float) >= float(min_nineties))
].copy()

if dfv.empty:
    st.warning("Aucune donnée après filtres — baissez le seuil de 90s ou élargissez les ligues/postes.")
    st.stop()

# Appliquer poids de compétition
dfv["comp_weight"] = dfv["league_name"].map(comp_weights).astype(float)
for f in features_p90 + features_tot:
    dfv[f] = pd.to_numeric(dfv[f], errors="coerce") * dfv["comp_weight"]

# Shrinkage per-90 (vers moyenne si peu de matchs)
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

# Score par poste (mix per-90 & totaux)
map_tot = {"gls_p90":"gls","ast_p90":"ast","sot_p90":"sot","xg_p90":"xg","npxg_p90":"npxg","xag_p90":"xag"}
def row_score(row):
    pos = row["pos_cat"]
    w = metric_weights.get(pos, weights_default[pos])
    s_rate = sum(row[f"z_{m}_adj"] * w[m] for m in w)
    s_tot  = sum(row[f"z_{map_tot[m]}"] * w[m] for m in w)
    return lambda_rate * s_rate + (1 - lambda_rate) * s_tot

dfv["raw_score"] = dfv.apply(row_score, axis=1)

# Facteur minutes (cap à 1 autour de minutes_ref x90) — racine pour lisser
minutes_factor = np.minimum(dfv["nineties"] / minutes_ref, 1.0) ** 0.5
dfv["ballon_score"] = dfv["raw_score"] * minutes_factor

# --------- Affichage principal ----------
cols_show = ["player_name","team","position","league_name","minutes","nineties",
             "gls","ast","sot","xg","npxg","xag",
             "gls_p90","ast_p90","sot_p90","xg_p90","npxg_p90","xag_p90",
             "ballon_score"]
dfv_sorted = dfv.sort_values("ballon_score", ascending=False)

st.success("Réglez les paramètres à gauche pour explorer le classement. "
           "Passez la souris sur les ⓘ pour comprendre chaque critère.")

# Top global + bar chart
top_global = dfv_sorted.head(topn).copy()
c1, c2 = st.columns([0.52, 0.48], gap="large")

with c1:
    st.subheader(f"Top {topn} — Classement global")
    st.dataframe(top_global[cols_show].style.format(precision=3), use_container_width=True, height=480)

with c2:
    st.subheader("Score global (bar chart)")
    chart = (
        alt.Chart(top_global.assign(rank=np.arange(1, len(top_global)+1)))
        .mark_bar()
        .encode(
            x=alt.X("ballon_score:Q", title="Score"),
            y=alt.Y("player_name:N", sort='-x', title=None),
            tooltip=["player_name","team","league_name","ballon_score"]
        )
        .properties(height=420)
    )
    st.altair_chart(chart, use_container_width=True)

# Onglets par compétition
st.markdown("### Par compétition")
tabs = st.tabs(selected_leagues)
for i, lg in enumerate(selected_leagues):
    with tabs[i]:
        sub = dfv_sorted[dfv_sorted["league_name"]==lg].head(topn)
        st.write(f"**Top {topn} — {lg}**")
        st.dataframe(sub[cols_show].style.format(precision=3), use_container_width=True, height=420)

# Fiche joueur (recherche)
st.markdown("---")
st.subheader("🔎 Fiche joueur")
colA, colB = st.columns([0.5, 0.5])
with colA:
    q = st.text_input("Rechercher un joueur", help="Tapez au moins 3 lettres du nom.")
with colB:
    min_age, max_age = st.slider("Âge", 15, 45, (15, 40), help="Filtre par âge (si disponible dans les données).")

if q and len(q) >= 3:
    sub = dfv_sorted[
        dfv_sorted["player_name"].str.contains(q, case=False, na=False) &
        dfv_sorted["age"].between(min_age, max_age, inclusive="both")
    ].head(25)
    if sub.empty:
        st.info("Aucun joueur trouvé avec ces critères.")
    else:
        st.dataframe(sub[cols_show].style.format(precision=3), use_container_width=True)

# Export
st.markdown("---")
cL, cR = st.columns([0.6, 0.4])
with cL:
    st.download_button(
        "⬇️ Export CSV (classement filtré)",
        data=dfv_sorted[cols_show].to_csv(index=False).encode("utf-8"),
        file_name="ballon_dor_ranking.csv",
        mime="text/csv"
    )
with cR:
    payload = dfv_sorted[["player_name","team","position","league_name","ballon_score"]].to_dict(orient="records")
    st.download_button(
        "⬇️ Export JSON (Top filtré)",
        data=json.dumps(payload, ensure_ascii=False, indent=2),
        file_name="ballon_dor_ranking.json",
        mime="application/json"
    )

st.caption("© 2025 · Projet Ballon d'Or — Métriques FBref (Kaggle) · Score paramétrable et transparent.")
