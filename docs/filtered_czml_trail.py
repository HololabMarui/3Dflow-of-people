#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
対話式で CSV を横断フィルタ →（任意でCSV書き出し）→ CZML 生成する一体型スクリプト。
"""

import csv
import glob
import json
import os
import re
from datetime import datetime, timezone, timedelta, date, time
from typing import Dict, List, Tuple, Any, Optional, Set, DefaultDict
from collections import defaultdict

# ====== 必須列 ======
REQ_COLS = [
    "latitude", "longitude",
    "altitude_hae", "altitude_hat",
    "log_ts", "is_worker", "gender", "age",
]

# ====== 滞在カテゴリ（コード→ラベル） ======
STAY_CODE_MAP: Dict[str, Tuple[str, str]] = {
    "01": ("駅ナカのみ滞在", "01_駅ナカのみ滞在"),
    "02": ("西側滞在",       "02_西側滞在"),
    "03": ("東側滞在",       "03_東側滞在"),
    "04": ("東西回遊",       "04_東西回遊"),
}

# ====== 路線メニュー（未確定・その他は除外） ======
RAILWAY_MENU = {
    "1": "JR",
    "2": "東武鉄道",
    "3": "西武鉄道",
    "4": "丸ノ内線",
    "5": "有楽町線",
    "6": "副都心線",
}
DEFAULT_RAILWAYS = set(RAILWAY_MENU.values())

# ====== JST ======
JST = timezone(timedelta(hours=9))

# ====== 池袋HAT色分け ======
def color_by_hat(hat: Optional[float]) -> List[int]:
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

# ------------------------------------------------
# Utilities
# ------------------------------------------------
def parse_ts_fallback(ts: str) -> Optional[datetime]:
    """Robust parser: return UTC-aware datetime or None."""
    if not ts:
        return None
    s = ts.strip()
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass
    fmts = [
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S%z",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S",
    ]
    for fmt in fmts:
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            continue
    return None

def parse_ts_to_jst(ts: str) -> Optional[datetime]:
    dt_utc = parse_ts_fallback(ts)
    return dt_utc.astimezone(JST) if dt_utc else None

def to_int(v: Any) -> Optional[int]:
    try:
        return int(str(v).strip())
    except Exception:
        return None

def normalize_worker(v: Any) -> int:
    s = str(v).strip().lower()
    return 1 if s in ("1", "true", "t", "yes", "y") else 0

def parse_code_and_railway_from_filename(path: str):
    """
    例: 01_JR_03_xxx.csv → railway='JR', code='03'
    想定：<prefix>_<railway>_<code>_...
    """
    base = os.path.basename(path)
    stem = os.path.splitext(base)[0]
    parts = stem.split("_")
    if len(parts) < 3:
        return (None, None)
    railway = parts[1]
    code = parts[2]
    if re.fullmatch(r"\d{2}", code) and code in STAY_CODE_MAP:
        return (railway, code)
    # 寛容化：1桁(1-4)も受け入れ → 0埋め
    if re.fullmatch(r"[1-4]", code):
        code2 = f"0{code}"
        if code2 in STAY_CODE_MAP:
            return (railway, code2)
    return (None, None)

# ------------------------------------------------
# 入力プロンプト（バリデーション付き）
# ------------------------------------------------
def prompt_worker_mode() -> int:
    while True:
        print("対象は来街者のみですか？")
        print("選択肢：0:来街者のみ、1:定期滞留者、2:両方")
        s = input("入力例（数字を入力）：0 > ").strip()
        if s in ("0", "1", "2", ""):
            return int(s) if s else 0
        print("※ 0/1/2 のいずれかを入力してください。\n")

def prompt_gender_mode() -> List[str]:
    while True:
        print("\n性別を選択（all で両方）：")
        print("選択肢： 0: 女性, 1:男性、 2:両方")
        s = input("入力例（数字を入力）：0 > ").strip()
        if s in ("0", "1", "2", "", "all", "ALL"):
            if s == "0": return ["女性"]
            if s == "1": return ["男性"]
            return ["女性", "男性"]
        print("※ 0/1/2/Enter のいずれかを入力してください。\n")

def prompt_ages() -> List[int]:
    valid = {"20", "30", "40", "50", "60"}
    while True:
        print("\n年代コードを選択（空/allで全て）：")
        print("選択肢： 20, 30, 40, 50, 60")
        s = input("入力例：20（またはカンマ区切りで複数選択 例：20,30 など） > ").strip()
        if s == "" or s.lower() == "all":
            return [20, 30, 40, 50, 60]
        picks = [t.strip() for t in s.split(",") if t.strip()]
        if picks and all(t in valid for t in picks):
            return [int(t) for t in picks]
        print("※ 20/30/40/50/60 をカンマ区切りで入力してください（例：20,30）。\n")

def prompt_stay_codes() -> List[str]:
    mapping = {"1": "01", "2": "02", "3": "03", "4": "04"}
    while True:
        print("\n滞在カテゴリコードを選択（空/allで全て）：")
        print("選択肢： 1：駅ナカのみ滞在, 2：西側滞在, 3：東側滞在, 4：東西回遊")
        s = input("入力例：1 （またはカンマ区切りで複数選択 例：1,2 など） > ").strip()
        if s == "" or s.lower() == "all":
            return ["01", "02", "03", "04"]
        picks = [t.strip() for t in s.split(",") if t.strip()]
        if picks and all(t in mapping for t in picks):
            return [mapping[t] for t in picks]
        print("※ 1/2/3/4 をカンマ区切りで入力してください（例：1,3）。\n")

def prompt_day_filter() -> str:
    while True:
        print("\n曜日フィルタを選択：（空Enterで all）")
        print("選択肢：0：すべて、1：平日、2：土日")
        s = input("入力例（数字を入力）：0 > ").strip()
        if s in ("", "0"):
            return "all"
        if s == "1":
            return "weekday"
        if s == "2":
            return "weekend"
        print("※ 0/1/2 か Enter を入力してください。\n")

def prompt_hour_range() -> Tuple[int, int]:
    while True:
        print("\n時間帯を0-24で指定（例：午前中 6-12）。空Enterで全時間帯。")
        s = input("hour_from-hour_to > ").strip()
        if not s:
            return (0, 24)
        if "-" in s:
            a, b = s.split("-", 1)
            try:
                h1 = max(0, min(24, int(a)))
                h2 = max(0, min(24, int(b)))
                if h1 == h2:
                    h2 = (h1 + 1) if h1 < 24 else 24
                return (min(h1, h2), max(h1, h2))
            except Exception:
                pass
        print("※ 例：6-12 のように 0-24 の範囲で指定してください（Enterで全時間帯）。\n")

def prompt_railways() -> Set[str]:
    # 未確定・その他は選択肢から排除済み
    choices_str = ", ".join([f"{k}:{v}" for k, v in RAILWAY_MENU.items()])
    while True:
        print("\n路線を選択（空/allで既定＝全て）：")
        print(f"選択肢： {choices_str}")
        s = input("入力例：1,2 > ").strip()
        if s == "" or s.lower() == "all":
            return set(DEFAULT_RAILWAYS)
        picks = [t.strip() for t in s.split(",") if t.strip()]
        if picks and all(t in RAILWAY_MENU for t in picks):
            return set(RAILWAY_MENU[t] for t in picks)
        print(f"※ {', '.join(RAILWAY_MENU.keys())} の番号をカンマ区切りで入力してください。\n")

def prompt_merge_dates() -> bool:
    while True:
        print("\n抽出された全データの日付を最初の1日にマージしますか？ 入力例：y、n")
        s = input("> ").strip().lower()
        if s in ("y", "n", ""):
            return s == "y"
        print("※ y / n / Enter を入力してください。\n")

def prompt_write_csv() -> bool:
    while True:
        print("\nCSVを書き出しますか？（Enter=スキップ / 1=出力）")
        s = input("> ").strip()
        if s in ("", "1"):
            return s == "1"
        print("※ Enter か 1 を入力してください。\n")

# ------------------------------------------------
# フィルタ（ストリーミング：一時CSVへ）
# ------------------------------------------------
def build_output_filename(cfg) -> str:
    target_map = {0: "来街者", 1: "定期滞留者", 2: "両方"}
    target = target_map.get(cfg["worker_mode"], "来街者")
    gender_order = {"女性": 0, "男性": 1}
    genders_sorted = sorted(list(cfg["genders"]), key=lambda x: gender_order.get(x, 99))
    gender_token = "-".join(genders_sorted) if len(genders_sorted) < 2 else "女性-男性"
    ages_sorted = sorted(list(cfg["ages"]))
    ages_token = "-".join(str(a) for a in ages_sorted) if ages_sorted else "all"
    stays_sorted = sorted(list(cfg["stay_codes"]))
    stays_token = "-".join(stays_sorted) if stays_sorted else "all"
    day_map = {"all": "すべて", "weekday": "平日", "weekend": "土日"}
    day_token = day_map.get(cfg["day_filter"], "すべて")
    hour_token = f"{cfg['hour_from']}-{cfg['hour_to']}"
    rails_sorted = sorted(list(cfg["railways"]))
    rails_token = "-".join(rails_sorted) if rails_sorted else "all"
    name = f"{target}_{gender_token}_{ages_token}_{stays_token}_{day_token}_{hour_token}_{rails_token}.csv"
    for a, b in [("/", "-"), ("\\", "-"), (":", "-"), ("*", "-"), ("?", "？"), ('"', "”"), ("<", "＜"), (">", "＞"), ("|", "｜")]:
        name = name.replace(a, b)
    return name

def run_filter_stream():
    worker_mode = prompt_worker_mode()
    genders = set(prompt_gender_mode())
    ages = set(prompt_ages())
    stay_codes = set(prompt_stay_codes())
    day_filter = prompt_day_filter()
    hour_from, hour_to = prompt_hour_range()
    railways = prompt_railways()
    merge_dates = prompt_merge_dates()
    write_csv = prompt_write_csv()

    print("\n--- 入力サマリ ---")
    print("対象：", {0:"来街者のみ",1:"定期滞留者のみ",2:"両方"}[worker_mode])
    print("性別：", sorted(list(genders)))
    print("年代：", sorted(list(ages)))
    print("滞在カテゴリ：", sorted(list(stay_codes)))
    print("曜日：", day_filter)
    print(f"時間帯：{hour_from}-{hour_to}")
    print("路線：", "・".join(sorted(list(railways))))
    print("最初の日にマージ：", "Yes" if merge_dates else "No")
    print("CSV出力：", "Yes" if write_csv else "Skip")
    print("------------------\n")

    cfg = {
        "worker_mode": worker_mode,
        "genders": genders,
        "ages": ages,
        "stay_codes": stay_codes,
        "day_filter": day_filter,
        "hour_from": hour_from,
        "hour_to": hour_to,
        "railways": railways,
    }
    out_csv_name = build_output_filename(cfg)

    # 診断用カウンタ
    cnt_total = cnt_route = cnt_stay = 0
    cnt_tsparse_fail = cnt_day = cnt_hour = cnt_worker = cnt_gender = cnt_age = 0
    cnt_kept = 0

    header_ref: Optional[List[str]] = None
    tmp_csv = "__filtered_tmp__.csv"
    tmp_meta = "__filtered_tmp_meta__.csv"

    for p in (tmp_csv, tmp_meta):
        try:
            if os.path.exists(p): os.remove(p)
        except Exception:
            pass

    with open(tmp_csv, "w", encoding="utf-8", newline="") as wf, \
         open(tmp_meta, "w", encoding="utf-8", newline="") as mf:
        w = csv.writer(wf)
        m = csv.writer(mf)
        for path in glob.glob("*.csv"):
            railway, code = parse_code_and_railway_from_filename(path)
            if code is None:
                continue
            cnt_total += 1
            if code not in stay_codes:
                cnt_stay += 1
                continue
            if railway not in railways:
                cnt_route += 1
                continue
            try:
                with open(path, "r", encoding="utf-8-sig", newline="") as f:
                    reader = csv.reader(f)
                    try:
                        header = next(reader)
                    except StopIteration:
                        continue

                    if header_ref is None:
                        header_ref = header[:]
                        name_to_idx = {c: i for i, c in enumerate(header_ref)}
                        if not all(col in name_to_idx for col in REQ_COLS):
                            header_ref = None
                            continue
                        w.writerow(header_ref)
                    else:
                        if header != header_ref:
                            continue

                    name_to_idx = {c: i for i, c in enumerate(header_ref)}
                    stay_label = STAY_CODE_MAP[code][0] if code in STAY_CODE_MAP else ""

                    for row in reader:
                        try:
                            ts = row[name_to_idx["log_ts"]]
                            dt_jst = parse_ts_to_jst(ts)
                            if dt_jst is None:
                                cnt_tsparse_fail += 1
                                continue

                            wd = dt_jst.weekday()
                            if day_filter == "weekday" and wd >= 5:
                                cnt_day += 1; continue
                            if day_filter == "weekend" and wd < 5:
                                cnt_day += 1; continue

                            if not (hour_from <= dt_jst.hour < hour_to):
                                cnt_hour += 1; continue

                            isw = normalize_worker(row[name_to_idx["is_worker"]])
                            if cfg["worker_mode"] == 0 and isw != 0:
                                cnt_worker += 1; continue
                            if cfg["worker_mode"] == 1 and isw != 1:
                                cnt_worker += 1; continue

                            gender = str(row[name_to_idx["gender"]]).strip()
                            if gender not in genders:
                                cnt_gender += 1; continue

                            age = to_int(row[name_to_idx["age"]])
                            if age is None or age not in ages:
                                cnt_age += 1; continue

                            w.writerow(row)
                            m.writerow([STAY_CODE_MAP[code][0], railway])
                            cnt_kept += 1

                        except Exception:
                            continue
            except Exception:
                continue

    if header_ref is None:
        print("一致するスキーマのCSVが見つかりませんでした。")
        return None

    if write_csv:
        try:
            import shutil
            shutil.copyfile(tmp_csv, out_csv_name)
            print("完了[CSV出力]:", out_csv_name)
        except Exception as e:
            print("CSV出力に失敗:", e)

    print("抽出レコード数:", cnt_kept)
    print("== 诊断 ==")
    print(f"対象CSV(命名一致)   : {cnt_total}")
    print(f"除外:滞在カテゴリ   : {cnt_stay}")
    print(f"除外:路線           : {cnt_route}")
    print(f"除外:TSパース失敗   : {cnt_tsparse_fail}")
    print(f"除外:曜日           : {cnt_day}")
    print(f"除外:時間帯         : {cnt_hour}")
    print(f"除外:is_worker      : {cnt_worker}")
    print(f"除外:性別           : {cnt_gender}")
    print(f"除外:年代           : {cnt_age}")

    return {
        "header": header_ref,
        "tmp_csv": "__filtered_tmp__.csv",
        "tmp_meta": "__filtered_tmp_meta__.csv",
        "merge_dates": merge_dates,
        "out_csv_name": out_csv_name,
    }

# ------------------------------------------------
# 一時CSV → CZML 変換（ストリーミング）
# ------------------------------------------------
def csvstream_to_czml_hat(data: dict, fade_seconds: float = 10.0) -> str:
    header = data["header"]
    tmp_csv = data["tmp_csv"]
    tmp_meta = data["tmp_meta"]
    merge_dates = data["merge_dates"]
    out_csv_name = data["out_csv_name"]

    metas: List[Tuple[str,str]] = []
    with open(tmp_meta, "r", encoding="utf-8", newline="") as mf:
        mr = csv.reader(mf)
        for r in mr:
            metas.append((r[0], r[1]))

    # 最初のJST日付（date-merge用）
    first_jst_date: Optional[date] = None
    idx = {c: i for i, c in enumerate(header)}
    i_ts  = idx.get("log_ts")

    with open(tmp_csv, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        _ = next(reader)
        for row in reader:
            dt_jst = parse_ts_to_jst(row[i_ts])
            if dt_jst:
                d = dt_jst.date()
                if first_jst_date is None or d < first_jst_date:
                    first_jst_date = d

    i_lat = idx.get("latitude")
    i_lon = idx.get("longitude")
    i_hae = idx.get("altitude_hae")
    i_hat = idx.get("altitude_hat")
    i_dev = idx.get("deviceID")
    i_isw = idx.get("is_worker")
    i_gen = idx.get("gender")
    i_age = idx.get("age")

    by_group: DefaultDict[Tuple[str,str], List[Dict[str,Any]]] = defaultdict(list)
    singles: List[Dict[str,Any]] = []

    with open(tmp_csv, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        next(reader)
        for row_i, row in enumerate(reader):
            try:
                lat = float(row[i_lat]); lon = float(row[i_lon]); hae = float(row[i_hae])
                hat = float(row[i_hat]) if i_hat is not None and row[i_hat] != "" else None
                dt_utc = parse_ts_fallback(row[i_ts])
                if None in (lat, lon, hae) or dt_utc is None:
                    continue

                if merge_dates and first_jst_date is not None:
                    jst = dt_utc.astimezone(JST)
                    t = time(jst.hour, jst.minute, jst.second, jst.microsecond, tzinfo=JST)
                    merged_jst = datetime.combine(first_jst_date, t).astimezone(JST)
                    dt_utc = merged_jst.astimezone(timezone.utc)

                dev = row[i_dev] if i_dev is not None else ""
                isw = row[i_isw] if i_isw is not None else ""
                gen = row[i_gen] if i_gen is not None else ""
                ag  = row[i_age] if i_age is not None else ""

                stay_label, railway = metas[row_i] if row_i < len(metas) else ("", "")

                rec = {
                    "lat": lat, "lon": lon, "hae": hae, "hat": hat,
                    "utc": dt_utc,
                    "deviceID": dev, "is_worker": isw, "gender": gen, "age": ag,
                    "StayCategory": stay_label, "Railway": railway
                }

                if dev:
                    jst_day = dt_utc.astimezone(JST).strftime("%Y-%m-%d")
                    by_group[(dev, jst_day)].append(rec)
                else:
                    singles.append(rec)
            except Exception:
                continue

    czml = [{
        "id": "document",
        "name": "CSV to CZML (HAT colors, exact style)",
        "version": "1.0"
    }]

    min_times: List[datetime] = []
    max_times: List[datetime] = []

    # 軌跡エンティティ
    for (dev, day_key), recs in by_group.items():
        recs.sort(key=lambda r: r["utc"])
        start = recs[0]["utc"]; end = recs[-1]["utc"] + timedelta(seconds=fade_seconds)
        min_times.append(start); max_times.append(end)

        epoch_iso = start.astimezone(timezone.utc).isoformat().replace("+00:00","Z")
        carto: List[float] = []; rgba_ts: List[Any] = []; last_rgba = None

        for r in recs:
            t = (r["utc"] - start).total_seconds()
            carto.extend([round(t,3), r["lon"], r["lat"], r["hae"]])
            rgba = color_by_hat(r["hat"])
            rgba_ts.extend([round(t,3), *rgba]); last_rgba = rgba

        if last_rgba is None:
            last_rgba = [128,128,128,200]
        rgba_ts.extend([round((recs[-1]["utc"] - start).total_seconds() + fade_seconds,3),
                        last_rgba[0], last_rgba[1], last_rgba[2], 0])

        last = recs[-1]
        props = {
            "Timestamp": last["utc"].astimezone(JST).isoformat(),
            "Latitude": last["lat"], "Longitude": last["lon"],
            "HAE": last["hae"], "HAT": last["hat"] if last["hat"] is not None else 0.0,
            "is_worker": int(normalize_worker(last["is_worker"])) if last["is_worker"] != "" else 0,
            "Gender": str(last["gender"]),
            "Age": int(last["age"]) if str(last["age"]).strip().isdigit() else 0,
            "StayCategory": last["StayCategory"], "Railway": last["Railway"],
        }

        czml.append({
            "id": f"{dev}_{day_key}",
            "name": f"{dev}_{day_key}",
            "availability": f"{epoch_iso}/{end.astimezone(timezone.utc).isoformat().replace('+00:00','Z')}",
            "position": {
                "epoch": epoch_iso,
                "cartographicDegrees": carto,
                "interpolationAlgorithm": "LINEAR",
                "interpolationDegree": 1
            },
            "point": {
                "pixelSize": 10,
                "color": {"epoch": epoch_iso, "rgba": rgba_ts},
                "heightReference": "NONE",
                "outlineColor": {"rgba": [255,127,80,220]},
                "outlineWidth": 2
            },
            "path": {
                "show": True,
                "width": 2,
                "material": {
                    "polylineOutline": {
                        "color": {"epoch": epoch_iso, "rgba": rgba_ts},
                        "outlineWidth": 1
                    }
                },
                "resolution": 1,
                "trailTime": 30.0,
                "leadTime": 0.0
            },
            "properties": props
        })

    # 単点エンティティ
    for i, r in enumerate(singles, start=1):
        start = r["utc"]; end = r["utc"] + timedelta(seconds=fade_seconds)
        epoch_iso = start.astimezone(timezone.utc).isoformat().replace("+00:00","Z")
        rgba = color_by_hat(r["hat"])
        rgba_ts = [0.0, *rgba, float(fade_seconds), rgba[0], rgba[1], rgba[2], 0]
        min_times.append(start); max_times.append(end)

        props = {
            "Timestamp": r["utc"].astimezone(JST).isoformat(),
            "Latitude": r["lat"], "Longitude": r["lon"],
            "HAE": r["hae"], "HAT": r["hat"] if r["hat"] is not None else 0.0,
            "is_worker": int(normalize_worker(r["is_worker"])) if r["is_worker"] != "" else 0,
            "Gender": str(r["gender"]),
            "Age": int(r["age"]) if str(r["age"]).strip().isdigit() else 0,
            "StayCategory": r["StayCategory"], "Railway": r["Railway"],
        }

        czml.append({
            "id": f"pt_{i}",
            "name": f"pt_{i}",
            "availability": f"{epoch_iso}/{end.astimezone(timezone.utc).isoformat().replace('+00:00','Z')}",
            "position": {
                "epoch": epoch_iso,
                "cartographicDegrees": [0.0, r["lon"], r["lat"], r["hae"]],
                "interpolationAlgorithm": "LINEAR",
                "interpolationDegree": 1
            },
            "point": {
                "pixelSize": 10,
                "color": {"epoch": epoch_iso, "rgba": rgba_ts},
                "heightReference": "NONE",
                "outlineColor": {"rgba": [255,127,80,220]},
                "outlineWidth": 2
            },
            "path": {
                "show": True,
                "width": 2,
                "material": {
                    "polylineOutline": {
                        "color": {"epoch": epoch_iso, "rgba": rgba_ts},
                        "outlineWidth": 1
                    }
                },
                "resolution": 1,
                "trailTime": 30.0,
                "leadTime": 0.0
            },
            "properties": props
        })

    # document clock
    if min_times and max_times:
        start = min(min_times).astimezone(timezone.utc)
        end   = max(max_times).astimezone(timezone.utc)
        czml[0]["clock"] = {
            "interval": f"{start.isoformat().replace('+00:00','Z')}/{end.isoformat().replace('+00:00','Z')}",
            "currentTime": start.isoformat().replace("+00:00","Z"),
            "multiplier": 1,
            "range": "UNBOUNDED"
        }

    out_czml = os.path.splitext(out_csv_name)[0] + ".czml"
    with open(out_czml, "w", encoding="utf-8") as w:
        json.dump(czml, w, ensure_ascii=False, separators=(",", ":"))

    print("完了[CZML出力]:", out_czml, "(entities:", len(czml)-1, ")")

    for p in ("__filtered_tmp__.csv", "__filtered_tmp_meta__.csv"):
        try:
            os.remove(p)
        except Exception:
            pass

    return out_czml

# ------------------------------------------------
# メイン
# ------------------------------------------------
def main():
    data = run_filter_stream()
    if not data:
        return
    csvstream_to_czml_hat(data, fade_seconds=10.0)

if __name__ == "__main__":
    main()