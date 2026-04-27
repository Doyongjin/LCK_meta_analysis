"""
시나리오 E — 패치별 승리 공식
Logistic Regression으로 진영/픽순서/15분 지표/오브젝트 기여도 산출
"""
import numpy as np
import pandas as pd
from sqlalchemy import text
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from .db import get_engine


def get_win_formula(patch_id: str | None = None) -> dict:
    """
    패치별(또는 전체) 승리 공식
    반환:
    {
      "patch": str | "전체",
      "n_games": int,
      "features": [
        {"name": str, "coefficient": float, "odds_ratio": float}
      ],
      "accuracy": float,
    }
    """
    engine = get_engine()
    with engine.connect() as conn:
        where = "AND g.patch_id = :pid" if patch_id else ""
        params = {"pid": patch_id} if patch_id else {}

        rows = conn.execute(text(f"""
            SELECT
                gt.side,
                gt.pick_order,
                gt.result,
                gt.gold_at_15,
                gt.first_dragon::int,
                gt.first_herald::int,
                gt.first_tower::int
            FROM game_teams gt
            JOIN games g ON g.game_id = gt.game_id
            WHERE gt.gold_at_15 IS NOT NULL
              {where}
        """), params).fetchall()

    if len(rows) < 20:
        return {"error": "데이터 부족 (최소 20경기 필요)"}

    df = pd.DataFrame(rows, columns=[
        "side", "pick_order", "result",
        "gold_at_15", "first_dragon", "first_herald", "first_tower"
    ])
    df["is_blue"] = (df["side"] == "blue").astype(int)
    df["is_first_pick"] = (df["pick_order"] == "first").astype(int)
    df["result"] = df["result"].astype(int)

    feature_cols = ["is_blue", "is_first_pick", "gold_at_15",
                    "first_dragon", "first_herald", "first_tower"]
    X = df[feature_cols].values
    y = df["result"].values

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    model = LogisticRegression(max_iter=1000)
    model.fit(X_scaled, y)
    accuracy = float(np.mean(model.predict(X_scaled) == y))

    feature_names = ["블루 진영", "선픽", "15분 골드 우위", "첫 드래곤", "첫 전령", "첫 타워"]
    features = []
    for name, coef in zip(feature_names, model.coef_[0]):
        features.append({
            "name": name,
            "coefficient": round(float(coef), 4),
            "odds_ratio": round(float(np.exp(coef)), 4),
        })

    features.sort(key=lambda x: abs(x["coefficient"]), reverse=True)

    return {
        "patch": patch_id or "전체",
        "n_games": len(df),
        "features": features,
        "accuracy": round(accuracy, 3),
    }


def get_win_formula_by_patch() -> list:
    """모든 패치별 승리 공식 목록"""
    engine = get_engine()
    with engine.connect() as conn:
        patches = conn.execute(text(
            "SELECT DISTINCT patch_id FROM games ORDER BY patch_id"
        )).fetchall()

    return [get_win_formula(r[0]) for r in patches]
