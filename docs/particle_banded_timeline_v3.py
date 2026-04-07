#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
particle_banded_timeline_daywindow.py
CSV → パーティクル（点＋Path）＋「高さ帯×時系列」の区間ポリラインを出力。
・各デバイス×各日（ローカル日）ごとにエンティティを分け、可視時間を 07:00〜23:00 に限定
・23:00 を過ぎるとその日の線は消え、翌 07:00 から新たに描画（リセット）
・線は“区間ごとに色固定”で積み上がる（その日の中では過去色が残る）
・高度帯（Δ=HAE-基準）既定: [-19,-5,0,5] → 4帯
・配色既定: [-19〜-5]=#32CD32, [-5〜0]=#ADFF2F, [0〜5]=#FFD700, [5+]=#FF4500

[コマンド]
python3 particle_banded_timeline_v3.py \
  your_data.csv \
  --out particle_heightband.czml \
  --bands 0 5 \
  --band-colors "#FFD700" "#32CD32" \
  --seg-alpha 200 \
  --path-width 2.5 \
  --point-size 6 \
  --tz Asia/Tokyo \
  --assume-naive-as-local \
  --debug

"""

from __future__ import annotations
import argparse, json
from datetime import datetime, time, timezone, timedelta, date
import pandas as pd

try:
    import pytz
except ImportError:
    pytz = None


# ---------- CLI ----------
def parse_args():
    ap = argparse.ArgumentParser(description="CZML particle + band-colored timeline (07:00-23:00 daily)")
    ap.add_argument("input_csv")
    ap.add_argument("--out", default="particle_banded_timeline_daywindow.czml")

    # 列指定（未指定なら自動推定）
    ap.add_argument("--id-col", default=None)
    ap.add_argument("--lat-col", default=None)
    ap.add_argument("--lon-col", default=None)
    ap.add_argument("--time-col", default=None)
    ap.add_argument("--alt-col", default=None)

    # 時刻
    ap.add_argument("--tz", default="Asia/Tokyo", help="ローカル日付の基準TZ（07-23の判定に使用）")
    ap.add_argument("--assume-naive-as-local", action="store_true")

    # パーティクル描画（点＋Path）
    ap.add_argument("--trail-time", type=float, default=60.0)
    ap.add_argument("--lead-time", type=float, default=0.0)
    ap.add_argument("--point-size", type=float, default=6.0)
    ap.add_argument("--path-width", type=float, default=2.0)

    # 高さ帯
    ap.add_argument("--bands", nargs="+", type=float, default=[-19.0, -5.0, 0.0, 5.0],
                    help="高さ帯の境界（例: -19 -5 0 5）")
    ap.add_argument("--band-colors", nargs="+",
                    default=["#32CD32", "#ADFF2F", "#FFD700", "#FF4500"])
    ap.add_argument("--seg-alpha", type=int, default=160, help="区間線の透過(0-255)")
    ap.add_argument("--seg-width", type=float, default=3.0)
    ap.add_argument("--subdivide", type=int, default=1, help="区間をN分割して色補間（その日の中で滑らかに）")

    # Δ=HAE-基準 の基準値
    ap.add_argument("--hae-ground", type=float, default=0.0)

    # 表示開始時刻
    ap.add_argument("--clock-at", choices=["start","end"], default="end",
                    help="ビューワー初期時刻（全期間の先頭/末尾）")
    ap.add_argument("--debug", action="store_true")

    return ap.parse_args()


# ---------- helpers ----------
def _norm(s: str) -> str:
    return str(s).lstrip("\ufeff").strip().lower()

def to_datetime_utc(s: str, tz_name: str, assume_naive_as_local: bool) -> datetime:
    # ISO優先
    try:
        dt = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        dt = None
    if dt is None:
        dt = pd.to_datetime(s, utc=False, errors="raise")
        if isinstance(dt, pd.Timestamp):
            dt = dt.to_pydatetime()
    # タイムゾーン付与/変換
    if dt.tzinfo is None:
        if assume_naive_as_local and tz_name.upper() != "UTC" and pytz is not None:
            dt = pytz.timezone(tz_name).localize(dt).astimezone(timezone.utc)
        else:
            dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt

def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00","Z")

def rgba(hexstr: str, a: int):
    h = hexstr.lstrip("#")
    if len(h)==3: h = "".join([c*2 for c in h])
    r = int(h[0:2],16); g = int(h[2:4],16); b = int(h[4:6],16)
    return {"rgba":[r,g,b,int(a)]}

def lerp_color(c1_hex: str, c2_hex: str, t: float, alpha: int):
    def rgb(hx):
        hx = hx.lstrip("#")
        return int(hx[0:2],16), int(hx[2:4],16), int(hx[4:6],16)
    r1,g1,b1 = rgb(c1_hex); r2,g2,b2 = rgb(c2_hex)
    r = round(r1 + (r2-r1)*t); g = round(g1 + (g2-g1)*t); b = round(b1 + (b2-b1)*t)
    return {"rgba":[r,g,b,alpha]}

def epoch_and_samples(times, lats, lons, alts):
    epoch = min(times); epoch_iso = iso_utc(epoch)
    samples = []
    for t, lat, lon, alt in zip(times,lats,lons,alts):
        sec = (t-epoch).total_seconds()
        samples.extend([sec, float(lon), float(lat), float(alt)])
    return epoch_iso, samples

def band_index(delta: float, breaks: list[float]) -> int:
    for i, b in enumerate(breaks):
        if delta < b:
            return i
    return len(breaks)

def band_color_hex(delta: float, breaks: list[float], colors: list[str]) -> str:
    idx = band_index(delta, breaks)
    return colors[idx] if idx < len(colors) else colors[-1]

def band_color(delta: float, breaks: list[float], colors: list[str], alpha: int):
    return rgba(band_color_hex(delta, breaks, colors), alpha)

def local_day(dt_utc: datetime, tz_name: str) -> date:
    if tz_name.upper()=="UTC" or pytz is None:
        return dt_utc.date()
    return dt_utc.astimezone(pytz.timezone(tz_name)).date()

def local_window_utc(day_d: date, tz_name: str, h_start=7, h_end=23) -> tuple[datetime, datetime]:
    """ローカル日 day_d の [h_start:00, h_end:00] を UTC に変換して返す"""
    if tz_name.upper()=="UTC" or pytz is None:
        start_local = datetime.combine(day_d, time(h_start,0), tzinfo=timezone.utc)
        end_local   = datetime.combine(day_d, time(h_end,0), tzinfo=timezone.utc)
        return start_local, end_local
    tz = pytz.timezone(tz_name)
    start_local = tz.localize(datetime.combine(day_d, time(h_start,0)))
    end_local   = tz.localize(datetime.combine(day_d, time(h_end,0)))
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


# ---------- column mapping ----------
def auto_map_columns(df: pd.DataFrame, args):
    df = df.rename(columns={c:_norm(c) for c in df.columns})
    cmap = {
        "deviceid":"device_id","device_id":"device_id","id":"device_id","uuid":"device_id",
        "端末id":"device_id","端末":"device_id",
        "lat":"lat","latitude":"lat","y":"lat","緯度":"lat",
        "lon":"lon","lng":"lon","long":"lon","longitude":"lon","x":"lon","経度":"lon",
        "timestamp":"timestamp","time":"timestamp","datetime":"timestamp","log_ts":"timestamp","日時":"timestamp","時刻":"timestamp",
        "alt":"alt","height":"alt","altitude":"alt","高度":"alt",
        "altitude_hae":"alt_hae","altitude_hat":"alt_hat"
    }
    df = df.rename(columns={c:cmap.get(c,c) for c in df.columns})

    def nk(x): return _norm(x) if x else x
    if args.id_col  and nk(args.id_col)  in df.columns: df = df.rename(columns={nk(args.id_col):"device_id"})
    if args.lat_col and nk(args.lat_col) in df.columns: df = df.rename(columns={nk(args.lat_col):"lat"})
    if args.lon_col and nk(args.lon_col) in df.columns: df = df.rename(columns={nk(args.lon_col):"lon"})
    if args.time_col and nk(args.time_col) in df.columns: df = df.rename(columns={nk(args.time_col):"timestamp"})
    if args.alt_col and nk(args.alt_col) in df.columns: df = df.rename(columns={nk(args.alt_col):"alt"})

    if "alt" not in df.columns:
        if "alt_hae" in df.columns: df = df.rename(columns={"alt_hae":"alt"})
        elif "alt_hat" in df.columns: df = df.rename(columns={"alt_hat":"alt"})

    req = ["device_id","lat","lon","timestamp"]
    miss = [c for c in req if c not in df.columns]
    if miss:
        raise ValueError(f"Missing required columns after mapping: {miss}")
    return df


# ---------- builder ----------
def build_packets(df: pd.DataFrame, args, breaks: list[float], colors: list[str]):
    # 全体の時間範囲（clock用）
    all_times = df["_dt"].tolist()
    global_start, global_end = min(all_times), max(all_times)
    current = global_end if args.clock_at == "end" else global_start

    packets = [{
        "id":"document","name":"Particle + Banded Timeline (07-23 daily)","version":"1.0",
        "clock":{
            "interval": f"{iso_utc(global_start)}/{iso_utc(global_end)}",
            "currentTime": iso_utc(current),
            "range":"LOOP_STOP",
            "multiplier": 1.0
        }
    }]

    # ローカル日で列を追加
    df["_localday"] = df["_dt"].map(lambda x: local_day(x, args.tz))

    # 各 device × 各日で処理
    for (did, day_d), g in df.groupby(["device_id","_localday"]):
        # その日の可視ウィンドウ（UTC）
        win_start, win_end = local_window_utc(day_d, args.tz, 7, 23)

        # ウィンドウ内のサンプルのみ採用（境界を厳密に切りたい場合は補間を追加）
        g = g[(g["_dt"] >= win_start) & (g["_dt"] <= win_end)].copy()
        if len(g) < 2:
            continue  # 線を作れない

        g = g.sort_values("_dt")
        T  = g["_dt"].tolist()
        LA = g["lat"].astype(float).tolist()
        LO = g["lon"].astype(float).tolist()
        AL = g["_alt"].astype(float).tolist()

        # 動く点＋Path（その日 07-23 のみ可視）
        epoch_iso, samples = epoch_and_samples(T, LA, LO, AL)
        packets.append({
            "id": f"particle-{did}-{day_d.isoformat()}",
            "availability": f"{iso_utc(win_start)}/{iso_utc(win_end)}",
            "position": {"epoch": epoch_iso, "cartographicDegrees": samples},
            "point": {
                "pixelSize": float(args.point_size),
                "color": rgba("#FFD700", 220),
                "outlineColor": rgba("#000000", 180),
                "outlineWidth": 1
            },
            "path": {
                "show": True, "width": float(args.path_width),
                "material": {"polylineGlow":{"glowPower":0.2,"color": rgba("#FFA500", 120)}},
                "leadTime": float(args.lead_time), "trailTime": float(args.trail_time)
            },
            "properties": {"device_id":{"string":str(did)}, "date":{"string":str(day_d)}}
        })

        # 区間線（その日の中では“残る”。翌日には見えない＝リセット）
        Nsub = max(1, int(args.subdivide))
        t_end = T[-1]
        for i in range(len(T)-1):
            t0, t1 = T[i], T[i+1]
            # 区間が可視ウィンドウにかかっているか（端が窓内なら採用）
            if t1 < win_start or t0 > win_end:
                continue
            for k in range(Nsub):
                s0 = k / Nsub
                s1 = (k+1) / Nsub
                lon0 = LO[i] + (LO[i+1]-LO[i]) * s0
                lat0 = LA[i] + (LA[i+1]-LA[i]) * s0
                alt0 = AL[i] + (AL[i+1]-AL[i]) * s0
                lon1 = LO[i] + (LO[i+1]-LO[i]) * s1
                lat1 = LA[i] + (LA[i+1]-LA[i]) * s1
                alt1 = AL[i] + (AL[i+1]-AL[i]) * s1

                d0 = alt0 - args.hae_ground
                d1 = alt1 - args.hae_ground
                c0_hex = band_color_hex(d0, breaks, colors)
                c1_hex = band_color_hex(d1, breaks, colors)
                col = lerp_color(c0_hex, c1_hex, 0.5, args.seg_alpha) if Nsub>1 else band_color(0.5*(d0+d1), breaks, colors, args.seg_alpha)

                packets.append({
                    "id": f"seg-{did}-{day_d.isoformat()}-{i}-{k}",
                    "availability": f"{iso_utc(max(t0, win_start))}/{iso_utc(win_end)}",  # その日の中では“残る”
                    "polyline": {
                        "positions": {"cartographicDegrees":[
                            float(lon0), float(lat0), float(alt0),
                            float(lon1), float(lat1), float(alt1),
                        ]},
                        "material": {"polylineOutline":{
                            "color": col,
                            "outlineWidth": 1.0,
                            "outlineColor": rgba("#000000", int(args.seg_alpha*0.4))
                        }},
                        "width": float(args.seg_width),
                        "clampToGround": False
                    },
                    "properties": {"device_id":{"string":str(did)}, "date":{"string":str(day_d)}}
                })

    return packets


# ---------- main ----------
def main():
    args = parse_args()

    # 読み込み（区切り自動判定）
    df = pd.read_csv(args.input_csv, sep=None, engine="python")
    df = auto_map_columns(df, args)

    # 時刻・高度
    df["_dt"] = df["timestamp"].astype(str).map(lambda s: to_datetime_utc(s, args.tz, args.assume_naive_as_local))
    df = df.sort_values(["device_id","_dt"])
    if "alt" in df.columns:
        s = pd.Series(df["alt"]); df["_alt"] = pd.to_numeric(s, errors="coerce").fillna(0.0)
    else:
        df["_alt"] = pd.Series(0.0, index=df.index)

    # デバッグ
    if args.debug:
        n = len(df)
        if n:
            tmin, tmax = df["_dt"].min(), df["_dt"].max()
            latmin, latmax = df["lat"].min(), df["lat"].max()
            lonmin, lonmax = df["lon"].min(), df["lon"].max()
            altmin, altmax = df["_alt"].min(), df["_alt"].max()
            print(f"[DEBUG] rows={n}")
            print(f"[DEBUG] time: {iso_utc(tmin)} .. {iso_utc(tmax)}")
            print(f"[DEBUG] lat : {latmin:.6f} .. {latmax:.6f}")
            print(f"[DEBUG] lon : {lonmin:.6f} .. {lonmax:.6f}")
            print(f"[DEBUG] alt : {altmin:.2f} .. {altmax:.2f}")
        else:
            print("[DEBUG] rows=0")

    # バンド色（不足ぶんは末尾色で埋める）
    breaks = list(map(float, args.bands))
    colors = list(args.band_colors)
    need = (len(breaks)+1) - len(colors)
    if need > 0:
        colors += [colors[-1]]*need

    packets = build_packets(df, args, breaks, colors)

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(packets, f, ensure_ascii=False)
    print(f"Wrote: {args.out} (entities: {len(packets)-1})")


if __name__ == "__main__":
    main()
