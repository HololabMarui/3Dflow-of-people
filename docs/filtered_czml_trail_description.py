
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSVフィルタ →（任意でCSV出力）→ CZML生成（B版 / A方式固定 / properties未出力）
- InfoBox は description を「interval の配列」で付与（A方式）
- document.clock を availability に合わせて設定
- entities 件数を出力
"""

import csv, glob, json, os, re, shutil
from datetime import datetime, timezone, timedelta, date, time
from typing import Dict, List, Tuple, Any, Optional, Set, DefaultDict
from collections import defaultdict

REQ_COLS = ["latitude","longitude","altitude_hae","altitude_hat","log_ts","is_worker","gender","age"]

STAY_CODE_MAP: Dict[str, Tuple[str, str]] = {
    "01": ("駅ナカのみ滞在", "01_駅ナカのみ滞在"),
    "02": ("西側滞在",       "02_西側滞在"),
    "03": ("東側滞在",       "03_東側滞在"),
    "04": ("東西回遊",       "04_東西回遊"),
}

RAILWAY_MENU = {"1":"JR","2":"東武鉄道","3":"西武鉄道","4":"丸ノ内線","5":"有楽町線","6":"副都心線"}
DEFAULT_RAILWAYS = set(RAILWAY_MENU.values())
JST = timezone(timedelta(hours=9))

def color_by_hat(hat: Optional[float]) -> List[int]:
    if hat is None: return [153,51,153,255]              # 紫（HAT欠損）
    if hat >= 5: return [0,166,81,255]                   # 緑
    if 0 <= hat < 5: return [255,205,0,255]              # 黄
    if -5 <= hat < 0: return [232,93,41,255]             # だいだい
    if -19 <= hat < -5: return [227,0,15,255]            # 赤
    return [153,51,153,255]                               # 紫（-20m未満）

def parse_ts_fallback(ts: str) -> Optional[datetime]:
    if not ts: return None
    s = ts.strip()
    try:
        dt = datetime.fromisoformat(s.replace("Z","+00:00"))
        if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass
    fmts = ["%Y-%m-%d %H:%M:%S%z","%Y-%m-%d %H:%M:%S",
            "%Y/%m/%d %H:%M:%S%z","%Y/%m/%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S%z","%Y-%m-%dT%H:%M:%S"]
    for fmt in fmts:
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            continue
    return None

def parse_ts_to_jst(ts: str) -> Optional[datetime]:
    u = parse_ts_fallback(ts)
    return u.astimezone(JST) if u else None

def to_int(v: Any) -> Optional[int]:
    try: return int(str(v).strip())
    except Exception: return None

def normalize_worker(v: Any) -> int:
    return 1 if str(v).strip().lower() in ("1","true","t","yes","y") else 0

def parse_code_and_railway_from_filename(path: str):
    stem = os.path.splitext(os.path.basename(path))[0]
    parts = stem.split("_")
    if len(parts)<3: return (None,None)
    railway, code = parts[1], parts[2]
    if re.fullmatch(r"\d{2}",code) and code in STAY_CODE_MAP: return (railway, code)
    if re.fullmatch(r"[1-4]",code):
        c=f"0{code}"
        if c in STAY_CODE_MAP: return (railway,c)
    return (None,None)

# ==== 対話メニュー ====

def prompt_worker_mode() -> int:
    while True:
        print("\n対象は来街者のみですか？")
        print("選択肢：0:来街者のみ（Enterでも可）, 1:定期滞留者, 2:両方")
        s=input("入力例：0 > ").strip()
        if s in ("","0","1","2"): return int(s) if s else 0
        print("※ 0/1/2 または Enter を入力してください。\n")

def prompt_gender_mode() -> List[str]:
    while True:
        print("\n性別を選択：")
        print("選択肢：0:女性, 1:男性, 2:両方（Enterでも可）")
        s=input("入力例：0 > ").strip()
        if s in ("","0","1","2"):
            if s=="0": return ["女性"]
            if s=="1": return ["男性"]
            return ["女性","男性"]
        print("※ 0/1/2 または Enter を入力してください。\n")

def prompt_ages() -> List[int]:
    valid={"20","30","40","50","60"}
    while True:
        print("\n年代コードを選択：")
        print("選択肢：20, 30, 40, 50, 60, Enter または all = すべて")
        s=input("入力例：20 > ").strip()
        if s=="" or s.lower()=="all": return [20,30,40,50,60]
        picks=[t.strip() for t in s.split(",") if t.strip()]
        if picks and all(t in valid for t in picks): return [int(t) for t in picks]
        print("※ 20/30/40/50/60 または Enter/all を入力してください。\n")

def prompt_stay_codes() -> List[str]:
    mapping={"1":"01","2":"02","3":"03","4":"04"}
    while True:
        print("\n滞在カテゴリコードを選択：")
        print("選択肢：1:駅ナカのみ滞在, 2:西側滞在, 3:東側滞在, 4:東西回遊, Enter または all = すべて")
        s=input("入力例：1 > ").strip()
        if s=="" or s.lower()=="all": return ["01","02","03","04"]
        picks=[t.strip() for t in s.split(",") if t.strip()]
        if picks and all(t in mapping for t in picks): return [mapping[t] for t in picks]
        print("※ 1/2/3/4 または Enter/all を入力してください。\n")

def prompt_day_filter() -> str:
    while True:
        print("\n曜日フィルタを選択：")
        print("選択肢：0:すべて（Enterでも可）, 1:平日, 2:土日")
        s=input("入力例：0 > ").strip()
        if s in ("","0"): return "all"
        if s=="1": return "weekday"
        if s=="2": return "weekend"
        print("※ 0/1/2 または Enter を入力してください。\n")

def prompt_hour_range() -> Tuple[int,int]:
    while True:
        print("\n時間帯を0-24で指定（例：午前中 6-12）")
        print("入力形式：開始-終了（Enterで全時間帯）")
        s=input("入力例：6-12 > ").strip()
        if not s: return (0,24)
        if "-" in s:
            try:
                a,b=s.split("-",1); h1=int(a); h2=int(b)
                if h1==h2: h2 = h1+1 if h1<24 else 24
                return (min(h1,h2), max(h1,h2))
            except Exception:
                pass
        print("※ 例：6-12 または Enter を入力してください。\n")

def prompt_railways() -> Set[str]:
    while True:
        print("\n路線を選択：")
        print("選択肢：1:JR, 2:東武鉄道, 3:西武鉄道, 4:丸ノ内線, 5:有楽町線, 6:副都心線, Enter または all = すべて")
        s=input("入力例：1,2 > ").strip()
        if s=="" or s.lower()=="all": return set(DEFAULT_RAILWAYS)
        picks=[t.strip() for t in s.split(",") if t.strip()]
        if picks and all(t in RAILWAY_MENU for t in picks): return {RAILWAY_MENU[t] for t in picks}
        print("※ 1〜6 または Enter/all を入力してください。\n")

def prompt_merge_dates() -> bool:
    while True:
        print("\n抽出された全データの日付を最初の1日にマージしますか？")
        print("選択肢：y:マージする, n:マージしない（Enterでも可）")
        s=input("入力例：y > ").strip().lower()
        if s in ("y","n",""): return s=="y"
        print("※ y/n または Enter を入力してください。\n")

def prompt_write_csv() -> bool:
    while True:
        print("\nCSVを書き出しますか？")
        print("選択肢：1:出力, Enter = スキップ")
        s=input("入力例：1 > ").strip()
        if s in ("","1"): return s=="1"
        print("※ 1 または Enter を入力してください。\n")

# ==== フィルタ処理 ====

def sanitize_filename(name: str) -> str:
    for a,b in [("/","-"),("\\","-"),(":","-"),("*","-"),("?","？"),('"',"”"),("<","＜"),(">","＞"),("|","｜")]:
        name = name.replace(a,b)
    return name

def build_output_filename(cfg) -> str:
    target_map={0:"来街者",1:"定期滞留者",2:"両方"}
    target=target_map.get(cfg["worker_mode"],"来街者")
    gender_order={"女性":0,"男性":1}
    genders_sorted=sorted(list(cfg["genders"]), key=lambda x: gender_order.get(x,99))
    gender_token="-".join(genders_sorted) if genders_sorted else "すべて"
    ages_token="-".join(str(a) for a in sorted(cfg["ages"])) if cfg["ages"] else "すべて"
    stays_labels=[STAY_CODE_MAP[c][0] for c in sorted(cfg["stay_codes"]) if c in STAY_CODE_MAP]
    stays_token="-".join(stays_labels) if stays_labels else "すべて"
    day_map={"all":"すべて","weekday":"平日","weekend":"土日"}
    day_token=day_map.get(cfg["day_filter"],"すべて")
    hour_token=f"{cfg['hour_from']}-{cfg['hour_to']}"
    rails_token="-".join(sorted(cfg["railways"])) if cfg["railways"] else "すべて"
    return sanitize_filename(f"{target}_{gender_token}_{ages_token}_{stays_token}_{day_token}_{hour_token}_{rails_token}.csv")

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

    cfg = {"worker_mode":worker_mode,"genders":genders,"ages":ages,"stay_codes":stay_codes,
           "day_filter":day_filter,"hour_from":hour_from,"hour_to":hour_to,"railways":railways}
    out_csv_name = build_output_filename(cfg)

    cnt_total=cnt_route=cnt_stay=0
    cnt_tsparse_fail=cnt_day=cnt_hour=cnt_worker=cnt_gender=cnt_age=0
    cnt_kept=0

    header_ref: Optional[List[str]] = None
    tmp_csv="__filtered_tmp__.csv"; tmp_meta="__filtered_tmp_meta__.csv"
    for p in (tmp_csv,tmp_meta):
        try:
            if os.path.exists(p): os.remove(p)
        except Exception:
            pass

    with open(tmp_csv,"w",encoding="utf-8",newline="") as wf, open(tmp_meta,"w",encoding="utf-8",newline="") as mf:
        w=csv.writer(wf); m=csv.writer(mf)
        for path in glob.glob("*.csv"):
            railway, code = parse_code_and_railway_from_filename(path)
            if code is None: continue
            cnt_total += 1
            if code not in stay_codes: cnt_stay += 1; continue
            if railway not in railways: cnt_route += 1; continue
            try:
                with open(path,"r",encoding="utf-8-sig",newline="") as f:
                    reader=csv.reader(f)
                    try: header=next(reader)
                    except StopIteration: continue
                    if header_ref is None:
                        header_ref=header[:]
                        name_to_idx={c:i for i,c in enumerate(header_ref)}
                        if not all(col in name_to_idx for col in REQ_COLS):
                            header_ref=None; continue
                        w.writerow(header_ref)
                    else:
                        if header!=header_ref: continue
                    name_to_idx={c:i for i,c in enumerate(header_ref)}
                    stay_label=STAY_CODE_MAP[code][0] if code in STAY_CODE_MAP else ""
                    for row in reader:
                        try:
                            ts=row[name_to_idx["log_ts"]]
                            dt_jst=parse_ts_to_jst(ts)
                            if dt_jst is None: cnt_tsparse_fail+=1; continue
                            wd=dt_jst.weekday()
                            if day_filter=="weekday" and wd>=5: cnt_day+=1; continue
                            if day_filter=="weekend" and wd<5: cnt_day+=1; continue
                            if not (hour_from <= dt_jst.hour < hour_to): cnt_hour+=1; continue
                            isw=normalize_worker(row[name_to_idx["is_worker"]])
                            if (worker_mode==0 and isw!=0) or (worker_mode==1 and isw!=1): cnt_worker+=1; continue
                            gender=str(row[name_to_idx["gender"]]).strip()
                            if gender not in genders: cnt_gender+=1; continue
                            age=to_int(row[name_to_idx["age"]])
                            if age is None or age not in ages: cnt_age+=1; continue
                            w.writerow(row); m.writerow([stay_label, railway]); cnt_kept+=1
                        except Exception:
                            continue
            except Exception:
                continue

    if header_ref is None:
        print("一致するスキーマのCSVが見つかりませんでした。"); return None

    if write_csv:
        try:
            shutil.copyfile(tmp_csv, out_csv_name)
            print("完了[CSV出力]:", out_csv_name)
        except Exception as e:
            print("CSV出力に失敗:", e)

    print("抽出レコード数:", cnt_kept)
    print("== 診断 ==")
    print(f"対象CSV(命名一致)   : {cnt_total}")
    print(f"除外:滞在カテゴリ   : {cnt_stay}")
    print(f"除外:路線           : {cnt_route}")
    print(f"除外:TSパース失敗   : {cnt_tsparse_fail}")
    print(f"除外:曜日           : {cnt_day}")
    print(f"除外:時間帯         : {cnt_hour}")
    print(f"除外:is_worker      : {cnt_worker}")
    print(f"除外:性別           : {cnt_gender}")
    print(f"除外:年代           : {cnt_age}")

    return {"header":header_ref,"tmp_csv":tmp_csv,"tmp_meta":tmp_meta,"merge_dates":merge_dates,"out_csv_name":out_csv_name}

# ==== CZML生成（A方式・properties未出力） ====

def csvstream_to_czml_hat(data: dict, fade_seconds: float = 10.0) -> str:
    header = data["header"]
    tmp_csv = data["tmp_csv"]
    tmp_meta = data["tmp_meta"]
    merge_dates = data["merge_dates"]
    out_csv_name = data["out_csv_name"]

    metas: List[Tuple[str, str]] = []
    with open(tmp_meta, "r", encoding="utf-8", newline="") as mf:
        for r in csv.reader(mf):
            if not r:
                continue
            stay_label = r[0] if len(r) > 0 else ""
            railway = r[1] if len(r) > 1 else ""
            metas.append((stay_label, railway))

    first_jst_date: Optional[date] = None
    idx = {c: i for i, c in enumerate(header)}
    i_lat = idx.get("latitude")
    i_lon = idx.get("longitude")
    i_hae = idx.get("altitude_hae")
    i_hat = idx.get("altitude_hat")
    i_ts  = idx.get("log_ts")
    i_dev = idx.get("deviceID")
    i_isw = idx.get("is_worker")
    i_gen = idx.get("gender")
    i_age = idx.get("age")

    with open(tmp_csv, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        try:
            next(reader)
        except StopIteration:
            pass
        else:
            for row in reader:
                try:
                    dt_jst = parse_ts_to_jst(row[i_ts])
                except Exception:
                    dt_jst = None
                if dt_jst:
                    d = dt_jst.date()
                    if first_jst_date is None or d < first_jst_date:
                        first_jst_date = d

    by_group: DefaultDict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    singles: List[Dict[str, Any]] = []

    with open(tmp_csv, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        next(reader, None)
        for row_i, row in enumerate(reader):
            try:
                lat = float(row[i_lat])
                lon = float(row[i_lon])
                hae = float(row[i_hae])
                hat = float(row[i_hat]) if (i_hat is not None and row[i_hat] != "") else None
                dt_utc = parse_ts_fallback(row[i_ts])
                if None in (lat, lon, hae) or dt_utc is None:
                    continue

                orig_jst_day = dt_utc.astimezone(JST).strftime("%Y-%m-%d")

                vis_dt_utc = dt_utc
                if merge_dates and first_jst_date is not None:
                    jst = dt_utc.astimezone(JST)
                    t = time(jst.hour, jst.minute, jst.second, jst.microsecond, tzinfo=JST)
                    merged_jst = datetime.combine(first_jst_date, t).astimezone(JST)
                    vis_dt_utc = merged_jst.astimezone(timezone.utc)

                dev = row[i_dev] if i_dev is not None else ""
                isw = row[i_isw] if i_isw is not None else ""
                gen = row[i_gen] if i_gen is not None else ""
                ag  = row[i_age] if i_age is not None else ""

                stay_label, railway = metas[row_i] if row_i < len(metas) else ("", "")

                rec = {
                    "lat": lat,
                    "lon": lon,
                    "hae": hae,
                    "hat": hat,
                    "utc": vis_dt_utc,
                    "utc_orig": dt_utc,
                    "deviceID": dev,
                    "is_worker": isw,
                    "gender": gen,
                    "age": ag,
                    "StayCategory": stay_label,
                    "Railway": railway,
                }

                if dev:
                    by_group[(dev, orig_jst_day)].append(rec)
                else:
                    singles.append(rec)
            except Exception:
                continue

    czml: List[Dict[str, Any]] = [
        {"id": "document", "name": "CSV to CZML (HAT colors; description intervals array)", "version": "1.0"}
    ]
    min_times: List[datetime] = []
    max_times: List[datetime] = []
    entity_count = 0

    for (dev, day_key), recs in by_group.items():
        if not recs:
            continue
        recs.sort(key=lambda r: r["utc"])
        start = recs[0]["utc"]
        end = recs[-1]["utc"] + timedelta(seconds=fade_seconds)
        min_times.append(start)
        max_times.append(end)
        epoch_iso = start.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

        carto: List[float] = []
        rgba_ts: List[Any] = []
        last_rgba: Optional[List[int]] = None

        for r in recs:
            t = (r["utc"] - start).total_seconds()
            carto.extend([round(t, 3), r["lon"], r["lat"], r["hae"]])
            rgba = color_by_hat(r["hat"])
            rgba_ts.extend([round(t, 3), *rgba])
            last_rgba = rgba

        if last_rgba is None:
            last_rgba = [128, 128, 128, 200]
        rgba_ts_end = round((recs[-1]["utc"] - start).total_seconds() + fade_seconds, 3)
        rgba_ts.extend([rgba_ts_end, last_rgba[0], last_rgba[1], last_rgba[2], 0])

        # description intervals（クリック時に InfoBox に表示）
        desc_intervals: List[Dict[str, str]] = []
        for idx_r, r in enumerate(recs):
            t0 = r["utc"].astimezone(timezone.utc)
            if idx_r + 1 < len(recs):
                t1 = recs[idx_r + 1]["utc"].astimezone(timezone.utc)
            else:
                t1 = end.astimezone(timezone.utc)

            hat_val = float(r["hat"]) if r["hat"] is not None else 0.0
            hae_val = float(r["hae"])
            lat_val = float(r["lat"])
            lon_val = float(r["lon"])
            ts_str  = r["utc_orig"].astimezone(JST).isoformat()
            isw_val = int(normalize_worker(r["is_worker"])) if r["is_worker"] != "" else 0
            age_val = int(r["age"]) if str(r["age"]).strip().isdigit() else 0

            html = (
                f"<b>Timestamp</b>：{ts_str}<br>"
                f"<b>Latitude</b>：{lat_val}<br>"
                f"<b>Longitude</b>：{lon_val}<br>"
                f"<b>HAE</b>：{hae_val}<br>"
                f"<b>HAT</b>：{hat_val}<br>"
                f"<b>is_worker</b>：{isw_val}<br>"
                f"<b>Gender</b>：{r['gender']}<br>"
                f"<b>Age</b>：{age_val}<br>"
                f"<b>StayCategory</b>：{r['StayCategory']}<br>"
                f"<b>Railway</b>：{r['Railway']}"
            )

            desc_intervals.append({
                "interval": f"{t0.isoformat().replace('+00:00','Z')}/{t1.isoformat().replace('+00:00','Z')}",
                "string": html
            })

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
                "show": True,
                "pixelSize": 12,
                "color": {"epoch": epoch_iso, "rgba": rgba_ts},
                "heightReference": "NONE",
                "outlineColor": {"rgba": [255, 127, 80, 220]},
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
            "description": desc_intervals
            # ※ properties は出力しない（InfoBox抑止の原因だったため）
        })
        entity_count += 1

    for i, r in enumerate(singles, start=1):
        start = r["utc"]
        end = r["utc"] + timedelta(seconds=fade_seconds)
        epoch_iso = start.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        rgba = color_by_hat(r["hat"])
        rgba_ts = [0.0, *rgba, float(fade_seconds), rgba[0], rgba[1], rgba[2], 0]
        min_times.append(start)
        max_times.append(end)

        desc_html = (
            f"<b>Timestamp</b>：{r['utc_orig'].astimezone(JST).isoformat()}<br>"
            f"<b>Latitude</b>：{float(r['lat'])}<br>"
            f"<b>Longitude</b>：{float(r['lon'])}<br>"
            f"<b>HAE</b>：{float(r['hae'])}<br>"
            f"<b>HAT</b>：{float(r['hat']) if r['hat'] is not None else 0.0}<br>"
            f"<b>is_worker</b>：{int(normalize_worker(r['is_worker'])) if r['is_worker'] != '' else 0}<br>"
            f"<b>Gender</b>：{r['gender']}<br>"
            f"<b>Age</b>：{int(r['age']) if str(r['age']).strip().isdigit() else 0}<br>"
            f"<b>StayCategory</b>：{r['StayCategory']}<br>"
            f"<b>Railway</b>：{r['Railway']}"
        )

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
                "show": True,
                "pixelSize": 12,
                "color": {"epoch": epoch_iso, "rgba": rgba_ts},
                "heightReference": "NONE",
                "outlineColor": {"rgba": [255, 127, 80, 220]},
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
            "description": [{
                "interval": f"{epoch_iso}/{end.astimezone(timezone.utc).isoformat().replace('+00:00','Z')}",
                "string": desc_html
            }]
        })
        entity_count += 1

    if min_times and max_times:
        c_start = min(min_times).astimezone(timezone.utc)
        c_end   = max(max_times).astimezone(timezone.utc)
        czml[0]["clock"] = {
            "interval": f"{c_start.isoformat().replace('+00:00','Z')}/{c_end.isoformat().replace('+00:00','Z')}",
            "currentTime": c_start.isoformat().replace("+00:00", "Z"),
            "multiplier": 1,
            "range": "UNBOUNDED"
        }

    out_czml = os.path.splitext(out_csv_name)[0] + ".czml"
    with open(out_czml, "w", encoding="utf-8") as w:
        json.dump(czml, w, ensure_ascii=False, separators=(",", ":"))
    print(f"完了[CZML出力]: {out_czml} (entities: {entity_count})")

    for p in (data["tmp_csv"], data["tmp_meta"]):
        try:
            os.remove(p)
        except Exception:
            pass

    return out_czml

# ==== メイン ====

def main():
    data=run_filter_stream()
    if not data: return
    csvstream_to_czml_hat(data, fade_seconds=10.0)

if __name__=="__main__":
    main()
