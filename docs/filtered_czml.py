#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
対話式フィルタで CSV 群を絞り込み、CZML を出力。
"""

import csv
import json
import glob
import os
import re
import zipfile
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple, Any, Optional, Set

# ====== 定数 ======
REQ_COLS = [
    "latitude", "longitude",
    "altitude_hae", "altitude_hat",
    "log_ts", "is_worker", "gender", "age",
]

STAY_CODE_MAP: Dict[str, Tuple[str, str]] = {
    "01": ("駅ナカのみ滞在", "01_駅ナカのみ滞在"),
    "02": ("西側滞在",       "02_西側滞在"),
    "03": ("東側滞在",       "03_東側滞在"),
    "04": ("東西回遊",       "04_東西回遊"),
}

# 路線のメニュー（数値→名称）
RAILWAY_MENU = {
    "1": "JR",
    "2": "東武鉄道",
    "3": "西武鉄道",
    "4": "丸ノ内線",
    "5": "有楽町線",
    "6": "副都心線",
    "7": "未確定",
    "8": "その他",
}
DEFAULT_RAILWAYS = {"JR", "東武鉄道", "西武鉄道", "丸ノ内線", "有楽町線", "副都心線"}  # 既定：未確定/その他は除外

JST = timezone(timedelta(hours=9))
ZIP_NAME = "railway_stay_ikebukuro_hat.zip"  # 出力ZIP名（ASCII）

# ====== 色分け（池袋HAT基準） ======
def color_by_hat(hat: float) -> List[int]:
    if hat is None:
        return [153, 51, 153, 255]  # 紫
    if hat >= 5:
        return [0, 166, 81, 255]    # 緑
    if 0 <= hat < 5:
        return [255, 205, 0, 255]   # 黄
    if -5 <= hat < 0:
        return [232, 93, 41, 255]   # 橙
    if -19 <= hat < -5:
        return [227, 0, 15, 255]    # 赤
    return [153, 51, 153, 255]      # 紫

# ====== ユーティリティ ======
def parse_ts_to_jst(ts: str) -> Optional[datetime]:
    try:
        ts_utc = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return ts_utc.astimezone(JST)
    except Exception:
        return None

def to_int(v: Any) -> Optional[int]:
    try:
        return int(str(v).strip())
    except Exception:
        return None

def to_float(v: Any) -> Optional[float]:
    try:
        return float(str(v).strip())
    except Exception:
        return None

def normalize_worker(v: Any) -> int:
    s = str(v).strip().lower()
    return 1 if s in ("1", "true", "t", "yes", "y") else 0

def parse_code_and_railway_from_filename(path: str):
    base = os.path.basename(path)
    stem = os.path.splitext(base)[0]
    parts = stem.split("_")
    if len(parts) < 3:
        return (None, None, None, None)
    railway = parts[1]  # 路線名（日本語）
    code = parts[2]     # 2桁コード（01..04）
    if re.fullmatch(r"\d{2}", code) and code in STAY_CODE_MAP:
        label, folder = STAY_CODE_MAP[code]
        return (code, label, folder, railway)
    return (None, None, None, None)

def ensure_dir(path: str):
    if not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)

# ====== 対話（メニュー） ======
def prompt_worker_mode() -> int:
    print("対象は来街者のみですか？")
    print("選択肢：0:来街者のみ、1:定期滞留者、2:両方")
    s = input("入力例（数字を入力）：0 > ").strip()
    return int(s) if s in ("0", "1", "2") else 0

def prompt_gender_mode() -> List[str]:
    print("性別を選択（all で両方）：")
    print("選択肢： 0: 女性, 1:男性、 2:両方")
    s = input("入力例（数字を入力）：0 > ").strip()
    if s == "0":
        return ["女性"]
    if s == "1":
        return ["男性"]
    if s == "2":
        return ["女性", "男性"]
    return ["女性", "男性"]

def prompt_ages() -> List[int]:
    print("年代コードを選択（空/allで全て）：")
    print("選択肢： 20, 30, 40, 50, 60")
    s = input("入力例：20（またはカンマ区切りで複数選択 例：20,30 など） > ").strip()
    if s == "" or s.lower() == "all":
        return [20, 30, 40, 50, 60]
    vals = []
    for t in s.split(","):
        t = t.strip()
        if t in {"20","30","40","50","60"}:
            vals.append(int(t))
    return vals or [20, 30, 40, 50, 60]

def prompt_stay_codes() -> List[str]:
    print("滞在カテゴリコードを選択（空/allで全て）：")
    print("選択肢： 1：駅ナカのみ滞在, 2：西側滞在, 3：東側滞在, 4：東西回遊")
    s = input("入力例：1 （またはカンマ区切りで複数選択 例：1,2 など） > ").strip()
    if s == "" or s.lower() == "all":
        return ["01", "02", "03", "04"]
    mapping = {"1":"01","2":"02","3":"03","4":"04"}
    out = []
    for t in s.split(","):
        t = t.strip()
        if t in mapping:
            out.append(mapping[t])
    return out or ["01", "02", "03", "04"]

def prompt_day_filter() -> str:
    print("曜日フィルタを選択：（空Enterで all）")
    print("選択肢：0：すべて、1：平日、2：土日")
    s = input("入力例（数字を入力）：0 > ").strip()
    if s == "1":
        return "weekday"
    if s == "2":
        return "weekend"
    return "all"

def prompt_hour_range() -> Tuple[int, int]:
    print("時間帯を0-24で指定（例：午前中 6-12）。空Enterで全時間帯。")
    s = input("hour_from-hour_to > ").strip()
    if not s:
        return (0, 24)
    try:
        a, b = s.split("-")
        h1, h2 = int(a), int(b)
        h1 = max(0, min(24, h1))
        h2 = max(0, min(24, h2))
        if h1 == h2:
            h2 = (h1 + 1) if h1 < 24 else 24
        return (min(h1, h2), max(h1, h2))
    except Exception:
        return (0, 24)

def prompt_railways() -> Set[str]:
    print("路線を選択（空/allで既定＝全て※未確定/その他を除外）：")
    print("選択肢： 1:JR, 2:東武鉄道, 3:西武鉄道, 4:丸ノ内線, 5:有楽町線, 6:副都心線, 7:未確定, 8:その他")
    s = input("入力例：1,2 > ").strip()
    if s == "" or s.lower() == "all":
        return set(DEFAULT_RAILWAYS)  # 既定：未確定/その他は含めない
    picked = set()
    for t in s.split(","):
        t = t.strip()
        if t in RAILWAY_MENU:
            picked.add(RAILWAY_MENU[t])
    return picked or set(DEFAULT_RAILWAYS)

def prompt_output_format() -> int:
    print("CZMLの出力形式は？")
    print("選択肢：1: 年代 × 性別ごと  2: 路線 × 滞在カテゴリ")
    s = input("入力例（数字を入力）：1 > ").strip()
    return 1 if s not in ("1","2") else int(s)

def interactive_filters():
    print("=== 対話式フィルタ設定（CZML出力版・形式選択付き） ===")
    worker_mode = prompt_worker_mode()        # 0/1/2
    genders = set(prompt_gender_mode())       # {"女性", ...}
    ages = set(prompt_ages())                 # {20,...}
    stay_codes = set(prompt_stay_codes())     # {"01","02","03","04"}
    day_filter = prompt_day_filter()          # "all"/"weekday"/"weekend"
    hour_from, hour_to = prompt_hour_range()
    railways = prompt_railways()              # 既定：未確定/その他は除外
    out_format = prompt_output_format()       # 1 or 2

    print("\n--- 入力サマリ ---")
    print("対象：", {0:"来街者のみ",1:"定期滞留者のみ",2:"両方"}[worker_mode])
    print("性別：", sorted(list(genders)))
    print("年代：", sorted(list(ages)))
    print("滞在カテゴリ：", sorted(list(stay_codes)))
    print("曜日：", day_filter)
    print(f"時間帯：{hour_from}-{hour_to}")
    print("路線：", "・".join(sorted(list(railways))))
    print("出力形式：", {1:"年代×性別ごと", 2:"路線×滞在カテゴリ"}[out_format])
    print("------------------\n")

    return {
        "worker_mode": worker_mode,
        "genders": genders,
        "ages": ages,
        "stay_codes": stay_codes,
        "day_filter": day_filter,
        "hour_from": hour_from,
        "hour_to": hour_to,
        "railways": railways,
        "out_format": out_format,
    }

# ====== 判定 ======
def row_passes_time_filters(ts_jst: Optional[datetime], day_filter: str, h_from: int, h_to: int) -> bool:
    if ts_jst is None:
        return False
    wd = ts_jst.weekday()
    if day_filter == "weekday" and wd >= 5:
        return False
    if day_filter == "weekend" and wd < 5:
        return False
    return (h_from <= ts_jst.hour < h_to)

def worker_match(isw: int, mode: int) -> bool:
    if mode == 2:  # 両方
        return True
    if mode == 0:  # 来街者
        return isw == 0
    if mode == 1:  # 定期滞留者
        return isw == 1
    return True

# ====== データ読み込み＆フィルタ ======
def load_filtered_rows(cfg) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    skipped_files: List[str] = []

    for path in glob.glob("*.csv"):
        code, stay_label, stay_folder, railway = parse_code_and_railway_from_filename(path)
        # 滞在カテゴリ・路線のフィルタ（ファイル単位）
        if code is None or code not in cfg["stay_codes"]:
            continue
        if railway not in cfg["railways"]:
            continue

        try:
            with open(path, "r", encoding="utf-8-sig", newline="") as f:
                reader = csv.reader(f)
                header = next(reader)
                name_to_idx = {name: i for i, name in enumerate(header)}
                if not all(col in name_to_idx for col in REQ_COLS):
                    skipped_files.append(path)
                    continue

                for r in reader:
                    try:
                        lat = to_float(r[name_to_idx["latitude"]])
                        lon = to_float(r[name_to_idx["longitude"]])
                        hae = to_float(r[name_to_idx["altitude_hae"]])
                        hat = to_float(r[name_to_idx["altitude_hat"]])
                        ts = parse_ts_to_jst(r[name_to_idx["log_ts"]])
                        isw = normalize_worker(r[name_to_idx["is_worker"]])
                        gender = str(r[name_to_idx["gender"]]).strip()
                        age = to_int(r[name_to_idx["age"]])

                        if None in (lat, lon, hae, hat, age):
                            continue
                        if not worker_match(isw, cfg["worker_mode"]):
                            continue
                        if gender not in cfg["genders"]:
                            continue
                        if age not in cfg["ages"]:
                            continue
                        if not row_passes_time_filters(ts, cfg["day_filter"], cfg["hour_from"], cfg["hour_to"]):
                            continue

                        rows.append({
                            "latitude": lat,
                            "longitude": lon,
                            "altitude_hae": hae,
                            "altitude_hat": hat,
                            "timestamp_jst": ts.isoformat() if ts else "",
                            "is_worker": isw,
                            "gender": gender,
                            "age": age,
                            "stay_code": code,
                            "stay_label": stay_label,
                            "stay_folder": stay_folder,
                            "railway": railway,
                        })
                    except Exception:
                        continue
        except Exception:
            skipped_files.append(path)
            continue

    if skipped_files:
        print("Skipped files (schema/code mismatch):", *skipped_files, sep="\n - ")
    return rows

# ====== CZML 構築 ======
def build_czml(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    czml = [{
        "id": "document",
        "name": "People Flow Data",
        "version": "1.0"
    }]
    for i, rec in enumerate(records, start=1):
        rgba = color_by_hat(rec["altitude_hat"])
        czml.append({
            "id": f"11111111111111111111111111111111_{i}",
            "name": f"Point_{i}",
            "position": {
                "cartographicDegrees": [rec["longitude"], rec["latitude"], rec["altitude_hae"]]
            },
            "point": {
                "color": {"rgba": rgba},
                "pixelSize": 10
            },
            "properties": {
                "Timestamp": {"string": rec["timestamp_jst"]},
                "Latitude": {"number": rec["latitude"]},
                "Longitude": {"number": rec["longitude"]},
                "HAE": {"number": rec["altitude_hae"]},
                "HAT": {"number": rec["altitude_hat"]},
                "is_worker": {"number": rec["is_worker"]},
                "Gender": {"string": rec["gender"]},
                "Age": {"number": rec["age"]},
                "StayCategory": {"string": rec["stay_label"]},
                "Railway": {"string": rec["railway"]},
            }
        })
    return czml

# ====== 出力（2形式） ======
def output_by_gender_age(rows: List[Dict[str, Any]]) -> List[str]:
    """
    グルーピング： (gender, age, stay_code) で出力。
    - 抽出条件で選択された滞在カテゴリのみを対象。
    - レコードが1件以上のグループのみファイル作成。
    ファイル名：『女性_20_駅ナカのみ滞在.czml』等
    保存先：滞在タイプ別フォルダ
    """
    created: List[str] = []
    # gender→age→stay_code の順でグルーピング
    groups: Dict[Tuple[str,int,str], List[Dict[str, Any]]] = {}
    for r in rows:
        key = (r["gender"], r["age"], r["stay_code"])
        groups.setdefault(key, []).append(r)

    for (gender, age, stay_code), recs in groups.items():
        if not recs:
            continue
        stay_label, stay_folder = STAY_CODE_MAP[stay_code]
        czml = build_czml(recs)
        fname = f"{gender}_{age}_{stay_label}.czml"
        out_path = os.path.join(stay_folder, fname)
        with open(out_path, "w", encoding="utf-8") as fp:
            json.dump(czml, fp, ensure_ascii=False, separators=(",", ":"))
        created.append(out_path)
    return created

def output_by_railway_stay(rows: List[Dict[str, Any]]) -> List[str]:
    """
    グルーピング： (railway, stay_code) で出力。
    - 抽出条件で選択された路線・滞在カテゴリのみを対象。
    - レコードが1件以上のグループのみファイル作成。
    ファイル名：『JR_東西回遊.czml』等
    保存先：滞在タイプ別フォルダ
    """
    created: List[str] = []
    groups: Dict[Tuple[str,str], List[Dict[str, Any]]] = {}
    for r in rows:
        key = (r["railway"], r["stay_code"])
        groups.setdefault(key, []).append(r)

    for (railway, stay_code), recs in groups.items():
        if not recs:
            continue
        stay_label, stay_folder = STAY_CODE_MAP[stay_code]
        czml = build_czml(recs)
        fname = f"{railway}_{stay_label}.czml"
        out_path = os.path.join(stay_folder, fname)
        with open(out_path, "w", encoding="utf-8") as fp:
            json.dump(czml, fp, ensure_ascii=False, separators=(",", ":"))
        created.append(out_path)
    return created

# ====== メイン ======
def main():
    cfg = interactive_filters()

    # 出力フォルダ（滞在タイプ別）を事前に用意
    for _, folder in STAY_CODE_MAP.values():
        ensure_dir(folder)

    rows = load_filtered_rows(cfg)

    # 出力形式で分岐（空グループは出力しない）
    if cfg["out_format"] == 1:
        created = output_by_gender_age(rows)
    else:
        created = output_by_railway_stay(rows)

    # ZIP 化（存在するファイルのみ）
    with zipfile.ZipFile(ZIP_NAME, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for _, folder in STAY_CODE_MAP.values():
            for root, _, files in os.walk(folder):
                for f in files:
                    full = os.path.join(root, f)
                    zf.write(full, arcname=os.path.relpath(full, start=os.getcwd()))

    print("\n=== 完了 ===")
    print("作成CZML数：", len(created))
    for p in created[:10]:
        print(" -", p)
    if len(created) > 10:
        print(f" ... (+{len(created)-10} more)")
    print("ZIP:", ZIP_NAME)

if __name__ == "__main__":
    main()
