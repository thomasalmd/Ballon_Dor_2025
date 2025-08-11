USE DATABASE FOOTBALL; USE SCHEMA ANALYTICS;

CREATE OR REPLACE VIEW V_TOPSCORERS AS
SELECT
  t.league,
  value:player:id::NUMBER       AS player_id,
  value:player:name::STRING     AS player_name,
  value:statistics[0]:goals:total::NUMBER AS goals
FROM FOOTBALL.RAW.TOPSCORERS_JSON t,
LATERAL FLATTEN (input => t.payload:response);

CREATE OR REPLACE VIEW V_TOPASSISTS AS
SELECT
  t.league,
  value:player:id::NUMBER       AS player_id,
  value:player:name::STRING     AS player_name,
  value:statistics[0]:goals:assists::NUMBER AS assists
FROM FOOTBALL.RAW.TOPASSISTS_JSON t,
LATERAL FLATTEN (input => t.payload:response);

CREATE OR REPLACE VIEW V_PLAYER_STATS AS
SELECT
  p.league,
  v:value:player:id::NUMBER                       AS player_id,
  v:value:player:name::STRING                     AS player_name,
  TRY_TO_NUMBER(v:value:statistics[0]:rating)::FLOAT AS rating,
  v:value:statistics[0]:shots:on::NUMBER          AS shots_on,
  v:value:statistics[0]:passes:key::NUMBER        AS key_passes
FROM FOOTBALL.RAW.PLAYERS_JSON p,
LATERAL FLATTEN (input => p.payload:response) v;

CREATE OR REPLACE VIEW V_PLAYER_AGG AS
WITH g AS (
  SELECT league, player_id, player_name, SUM(goals) AS goals
  FROM V_TOPSCORERS
  GROUP BY 1,2,3
),
a AS (
  SELECT league, player_id, SUM(assists) AS assists
  FROM V_TOPASSISTS
  GROUP BY 1,2
),
s AS (
  SELECT league, player_id, AVG(NULLIF(rating,0)) AS avg_rating,
         SUM(NULLIF(shots_on,0)) AS shots_on,
         SUM(NULLIF(key_passes,0)) AS key_passes
  FROM V_PLAYER_STATS
  GROUP BY 1,2
),
m AS (
  SELECT COALESCE(g.league,a.league,s.league) league,
         COALESCE(g.player_id,a.player_id,s.player_id) player_id,
         ANY_VALUE(COALESCE(g.player_name,'')) player_name,
         COALESCE(g.goals,0) goals,
         COALESCE(a.assists,0) assists,
         COALESCE(s.avg_rating, NULL) avg_rating,
         COALESCE(s.shots_on,0) shots_on,
         COALESCE(s.key_passes,0) key_passes
  FROM g
  FULL OUTER JOIN a USING (league, player_id)
  FULL OUTER JOIN s USING (league, player_id)
)
SELECT
  m.player_id, m.player_name,
  SUM(m.goals * r.weight)      AS w_goals,
  SUM(m.assists * r.weight)    AS w_assists,
  AVG(m.avg_rating)            AS avg_rating,
  SUM(m.shots_on * r.weight)   AS w_shots_on,
  SUM(m.key_passes * r.weight) AS w_key_passes
FROM m
JOIN FOOTBALL.ANALYTICS.REF_COMPETITIONS r ON m.league = r.comp_id
GROUP BY 1,2;
