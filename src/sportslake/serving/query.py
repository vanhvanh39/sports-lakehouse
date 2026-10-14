"""Truy vấn ad-hoc trực tiếp trên bảng Delta bằng DuckDB (không cần ETL sang hệ thống khác).

  python -m sportslake.serving.query --list
  python -m sportslake.serving.query --preset standings
  python -m sportslake.serving.query "SELECT category, count(*) FROM silver_match_events GROUP BY 1"
  python -m sportslake.serving.query --version silver_match_events=3 "SELECT count(*) FROM silver_match_events"
"""
from __future__ import annotations

import argparse
import time

from .lake import TABLES, duckdb_connection

PRESETS = {
    "live": """SELECT match_id, home_team, home_goals, away_goals, away_team, status_short, elapsed, league_name
               FROM silver_match_state ORDER BY kickoff_ts DESC LIMIT 30""",
    "events": """SELECT match_id, minute, category, event_detail, team_name, player_name, is_deleted, version_count
                 FROM silver_match_events WHERE category NOT IN ('OTHER','FOUL')
                 ORDER BY processed_ts DESC LIMIT 40""",
    "corrections": """SELECT match_id, minute, category, player_name, is_deleted, version_count, op
                      FROM silver_match_events WHERE version_count > 1 OR is_deleted ORDER BY updated_ts DESC LIMIT 40""",
    "alerts": """SELECT version, match_id, minute, kind, score, message, alert_ts FROM silver_alerts
                 ORDER BY alert_ts DESC LIMIT 30""",
    "standings": """SELECT league_name, season, rank, team_name, played, won, drawn, lost, gf, ga, gd, points
                    FROM gold_standings ORDER BY league_name, season, rank LIMIT 60""",
    "form": """SELECT league_name, team_name, form_latest_first, form_points, avg_xg FROM gold_team_form
               ORDER BY form_points DESC LIMIT 30""",
    "scorers": """SELECT player_name, team_name, goals, shots, xg, goals_minus_xg, matches FROM gold_player_summary
                  ORDER BY goals DESC, xg DESC LIMIT 25""",
    "distance": """SELECT match_id, team, player, distance_km, top_speed_kmh, high_speed_m FROM gold_player_distance
                   ORDER BY distance_km DESC LIMIT 25""",
    "volume": """SELECT 'silver_match_events' t, count(*) n FROM silver_match_events
                 UNION ALL SELECT 'bronze_kafka_raw', count(*) FROM bronze_kafka_raw
                 UNION ALL SELECT 'bronze_tracking_positions', count(*) FROM bronze_tracking_positions""",
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("sql", nargs="?")
    ap.add_argument("--preset", choices=sorted(PRESETS))
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--version", action="append", default=[], help="time travel: bang=phien_ban")
    ap.add_argument("--csv", help="ghi kết quả ra file CSV")
    a = ap.parse_args()
    versions = {k: int(v) for k, v in (x.split("=") for x in a.version)}
    con = duckdb_connection(version=versions)
    if a.list:
        for name in TABLES:
            try:
                n = con.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
                print(f"{name:32s} {n:>12,d} dòng")
            except Exception:  # noqa: BLE001
                print(f"{name:32s} (chưa có)")
        return
    sql = PRESETS[a.preset] if a.preset else a.sql
    if not sql:
        ap.error("cần câu SQL hoặc --preset")
    t0 = time.time()
    df = con.execute(sql).fetchdf()
    import pandas as pd

    with pd.option_context("display.max_rows", 200, "display.max_columns", 30, "display.width", 200):
        print(df)
    print(f"\n{len(df)} dòng, {time.time() - t0:.2f}s")
    if a.csv:
        df.to_csv(a.csv, index=False)


if __name__ == "__main__":
    main()
