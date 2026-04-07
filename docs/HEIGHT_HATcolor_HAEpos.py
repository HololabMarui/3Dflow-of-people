#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV → CZML（人流／時系列｜HATで色分け／HAEで位置｜アニメ＋先端点｜単色＆到達後保持）
- 位置（position/path）は HAE（altitude_hae → alt）を使用
- 色の判定とバンド自動計算は HAT（altitude_hat）を優先（無ければ HAE）
- 区間線は「i→i+1」単位。pathで線が伸びる/到達後は保持（既定）
- 先端点（動く点）を付与可能（--head-point）


  python3 csv_to_czml_HEIGHT_HATcolor_HAEpos.py input.csv \
  --out-prefix people_flow \
  --tz Asia/Tokyo --window 7 23 \
   --bands -10 -5 0 5 18 24 \
--band-colors "#8c00ff" "#d000ff" "#ffffff" "#00ff65" "#eeff00" "#00e5ff" \
  --seg-draw path --seg-mode span --seg-width 5 --seg-alpha 255 \
  --head-point --head-point-size 10 --head-no-depth-test \
  --point-mode persist --point-size 10 \
  --chunk-hours 0 --debug

"""
from __future__ import annotations

import argparse, json, os
from datetime import datetime, time, timedelta, timezone, date
from typing import List, Tuple
import pandas as pd

try:
    import pytz
except ImportError:
    pytz = None

def _norm(s: str) -> str:
    return str(s).lstrip("\ufeff").strip().lower()

def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00","Z")

def to_datetime_utc(s: str, tz_name: str, assume_naive_as_local: bool) -> datetime:
    try:
        dt = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        dt = None
    if dt is None:
        dt = pd.to_datetime(s, utc=False, errors="raise")
        if isinstance(dt, pd.Timestamp):
            dt = dt.to_pydatetime()
    if dt.tzinfo is None:
        if assume_naive_as_local and tz_name.upper() != "UTC" and pytz is not None:
            dt = pytz.timezone(tz_name).localize(dt).astimezone(timezone.utc)
        else:
            dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt

def rgba_from_hex(hexstr: str, a: int) -> dict:
    h = hexstr.lstrip("#")
    if len(h) == 3:
        h = "".join([c*2 for c in h])
    r = int(h[0:2],16); g = int(h[2:4],16); b = int(h[4:6],16)
    return {"rgba":[r,g,b,int(a)]}

def band_index(v: float, breaks: List[float]) -> int:
    for i, b in enumerate(breaks):
        if v < b:
            return i
    return len(breaks)

def band_color_hex(v: float, breaks: List[float], colors: List[str]) -> str:
    idx = band_index(v, breaks)
    return colors[idx] if idx < len(colors) else colors[-1]

def local_day(dt_utc: datetime, tz_name: str) -> date:
    if tz_name.upper() == "UTC" or pytz is None:
        return dt_utc.date()
    return dt_utc.astimezone(pytz.timezone(tz_name)).date()

def local_window_utc(day_d: date, tz_name: str, h_start: int, h_end: int) -> Tuple[datetime, datetime]:
    if tz_name.upper() == "UTC" or pytz is None:
        s = datetime.combine(day_d, time(h_start,0), tzinfo=timezone.utc)
        e = datetime.combine(day_d, time(h_end,0), tzinfo=timezone.utc)
        return s, e
    tz = pytz.timezone(tz_name)
    s_loc = tz.localize(datetime.combine(day_d, time(h_start,0)))
    e_loc = tz.localize(datetime.combine(day_d, time(h_end,0)))
    return s_loc.astimezone(timezone.utc), e_loc.astimezone(timezone.utc)

def auto_map_columns(df: pd.DataFrame, args) -> pd.DataFrame:
    df = df.rename(columns={c:_norm(c) for c in df.columns})
    cmap = {
        "deviceid":"device_id","device_id":"device_id","id":"device_id","uuid":"device_id","端末id":"device_id","端末":"device_id",
        "lat":"lat","latitude":"lat","y":"lat","緯度":"lat",
        "lon":"lon","lng":"lon","long":"lon","longitude":"lon","x":"lon","経度":"lon",
        "timestamp":"timestamp","time":"timestamp","datetime":"timestamp","date_time":"timestamp",
        "log_ts":"timestamp","datetimetz":"timestamp","datetime_utc":"timestamp","日時":"timestamp","時刻":"timestamp",
        "alt":"alt","height":"alt","altitude":"alt","高度":"alt",
        "altitude_hae":"alt_hae","altitude_hat":"alt_hat"
    }
    df = df.rename(columns={c:cmap.get(c,c) for c in df.columns})
    def _apply_override(arg_val: str | None, target: str):
        if not arg_val: return
        k = _norm(arg_val)
        if k in df.columns and k != target:
            df.rename(columns={k:target}, inplace=True)
    _apply_override(getattr(args,"id_col",None), "device_id")
    _apply_override(getattr(args,"lat_col",None), "lat")
    _apply_override(getattr(args,"lon_col",None), "lon")
    _apply_override(getattr(args,"time_col",None), "timestamp")
    # 位置用ALT: HAE優先（無ければ HAT）→ alt として使う
    if "alt" not in df.columns:
        if "alt_hae" in df.columns:
            df.rename(columns={"alt_hae":"alt"}, inplace=True)
        elif "alt_hat" in df.columns:
            df.rename(columns={"alt_hat":"alt"}, inplace=True)
    req = ["device_id","lat","lon","timestamp","alt"]
    miss = [c for c in req if c not in df.columns]
    if miss:
        raise ValueError(f"Missing required columns: {miss}. Mapped columns: {list(df.columns)}")
    return df

def build_packets_chunk(
    df: pd.DataFrame,
    tz_name: str,
    h_start: int, h_end: int,
    bands: List[float], band_colors: List[str],
    seg_draw: str,
    seg_mode: str, seg_alpha: int, seg_width: float, seg_hold: float,
    trail_time: float, lead_time: float, point_size: float,
    clock_at: str,
    t_from: datetime, t_to: datetime,
    point_mode: str = "persist", point_hold: float = 15.0,
    point_no_depth_test: bool = False,
    head_point: bool = False, head_point_size: float = 10.0, head_no_depth_test: bool = False,
    keep_after_arrival: bool = True
) -> List[dict]:

    global_start, global_end = t_from, t_to
    current = global_start if clock_at == "start" else global_end

    packets: List[dict] = [{
        "id":"document","name":"HAT-color / HAE-position","version":"1.0",
        "clock":{
            "interval": f"{iso_utc(global_start)}/{iso_utc(global_end)}",
            "currentTime": iso_utc(current),
            "range":"LOOP_STOP","multiplier":1.0
        }
    }]

    df["_localday"] = df["_dt"].map(lambda x: local_day(x, tz_name))

    for (did, day_d), g0 in df.groupby(["device_id","_localday"]):
        win_start, win_end = local_window_utc(day_d, tz_name, h_start, h_end)
        vis_start = max(win_start, t_from)
        vis_end   = min(win_end,   t_to)
        if vis_end <= vis_start:
            continue

        g = g0[(g0["_dt"] >= vis_start) & (g0["_dt"] <= vis_end)].copy()
        if len(g) < 1: continue
        g = g.sort_values("_dt")

        T  = g["_dt"].tolist()
        LA = g["lat"].astype(float).tolist()
        LO = g["lon"].astype(float).tolist()
        AL = g["alt"].astype(float).tolist()  # HAE (position)

        # 色判定用（HAT優先）
        if "alt_hat" in g.columns:
            AL_COLOR = g["alt_hat"].astype(float).tolist()
        else:
            AL_COLOR = AL

        for i in range(len(T)-1):
            t0, t1 = T[i], T[i+1]
            if t1 <= vis_start or t0 >= vis_end:
                continue
            lon0, lat0, alt0 = float(LO[i]),   float(LA[i]),   float(AL[i])
            lon1, lat1, alt1 = float(LO[i+1]), float(LA[i+1]), float(AL[i+1])

            alt0_for_color = float(AL_COLOR[i])
            c_hex = band_color_hex(alt0_for_color, bands, band_colors)
            col   = rgba_from_hex(c_hex, seg_alpha)

            if keep_after_arrival:
                a0 = max(t0, vis_start); a1 = vis_end
            else:
                if seg_mode == "flash":
                    a0 = max(t0, vis_start); a1 = min(t0 + timedelta(seconds=seg_hold), vis_end)
                elif seg_mode == "span":
                    a0 = max(t0, vis_start); a1 = min(t1, vis_end)
                else:
                    a0 = max(t0, vis_start); a1 = vis_end
            if a1 <= a0: continue

            epoch_iso = iso_utc(t0)
            dt_sec = (t1 - t0).total_seconds()
            pos_block = {
                "epoch": epoch_iso,
                "cartographicDegrees": [
                    0,      lon0, lat0, alt0,
                    dt_sec, lon1, lat1, alt1
                ]
            }

            ent = {
                "id": f"seg-{did}-{day_d}-{i}-{int(a0.timestamp())}",
                "availability": f"{iso_utc(a0)}/{iso_utc(a1)}",
                "properties": {
                    "device_id": {"string": str(did)},
                    "start_ts": {"string": iso_utc(t0)},
                    "end_ts": {"string": iso_utc(t1)},
                    "start_lon": {"number": lon0},
                    "start_lat": {"number": lat0},
                    "start_alt_hae": {"number": alt0},
                    "start_alt_hat": {"number": float(AL_COLOR[i])} if "alt_hat" in g.columns else {"string":"NA"},
                    "end_lon": {"number": lon1},
                    "end_lat": {"number": lat1},
                    "end_alt_hae": {"number": alt1},
                    "end_alt_hat": {"number": float(AL_COLOR[i+1])} if "alt_hat" in g.columns else {"string":"NA"},
                    "height_band_thresholds": {"string": ",".join(map(str, bands))},
                    "color_hex": {"string": c_hex}
                }
            }

            if seg_draw == "polyline":
                ent["polyline"] = {
                    "positions": {"cartographicDegrees":[lon0, lat0, alt0,  lon1, lat1, alt1]},
                    "width": float(seg_width),
                    "material": {"solidColor":{"color": col}},
                    "clampToGround": False
                }
                if head_point:
                    ent["position"] = pos_block
            else:
                ent["position"] = pos_block
                if keep_after_arrival:
                    trail_seconds = (a1 - a0).total_seconds()
                else:
                    if seg_mode in ("flash","span"):
                        trail_seconds = (min(t1, vis_end) - a0).total_seconds()
                    else:
                        trail_seconds = max(86400.0, (a1 - a0).total_seconds())
                ent["path"] = {
                    "show": True,
                    "width": float(seg_width),
                    "leadTime": 0,
                    "trailTime": float(trail_seconds),
                    "material": {"solidColor":{"color": col}}
                }

            if head_point:
                ent["point"] = {
                    "pixelSize": float(head_point_size),
                    "color": rgba_from_hex(c_hex, 240),
                    "outlineColor": rgba_from_hex("#000000", 160),
                    "outlineWidth": 1
                }
                if head_no_depth_test:
                    ent["point"]["disableDepthTestDistance"] = 1.0e9

            packets.append(ent)

        # 各サンプル点
        for k, (t, la, lo, al) in enumerate(zip(T, LA, LO, AL)):
            if t < vis_start or t > vis_end: continue
            if point_mode == "flash":
                a0 = max(t, vis_start); a1 = min(t + timedelta(seconds=max(1.0, point_hold)), vis_end)
            else:
                a0 = max(t, vis_start); a1 = vis_end

            al_color = float(AL_COLOR[k])
            c_hex_pt = band_color_hex(al_color, bands, band_colors)

            point_block = {
                "id": f"pt-{did}-{day_d}-{k}-{int(a0.timestamp())}",
                "availability": f"{iso_utc(a0)}/{iso_utc(a1)}",
                "position": {"cartographicDegrees":[float(lo), float(la), float(al)]},
                "point": {
                    "pixelSize": max(4.0, float(point_size)-1.0),
                    "color": rgba_from_hex(c_hex_pt, 230),
                    "outlineColor": rgba_from_hex("#000000", 160),
                    "outlineWidth": 1
                },
                "properties": {
                    "device_id": {"string": str(did)},
                    "timestamp": {"string": iso_utc(t)},
                    "lon": {"number": float(lo)},
                    "lat": {"number": float(la)},
                    "alt_hae": {"number": float(al)},
                    "alt_hat": {"number": al_color} if "alt_hat" in g.columns else {"string":"NA"},
                    "height_band_thresholds": {"string": ",".join(map(str, bands))},
                    "color_hex": {"string": c_hex_pt}
                }
            }
            if point_no_depth_test:
                point_block["point"]["disableDepthTestDistance"] = 1.0e9

            packets.append(point_block)

    return packets

def main():
    ap = argparse.ArgumentParser(description="CSV→CZML（HAT色/HAE位置｜単色線・到達後保持・アニメパス・先端点）" )
    ap.add_argument("input_csv")
    ap.add_argument("--out-prefix", required=True)

    ap.add_argument("--id-col", default=None)
    ap.add_argument("--lat-col", default=None)
    ap.add_argument("--lon-col", default=None)
    ap.add_argument("--time-col", default=None)

    ap.add_argument("--tz", default="Asia/Tokyo")
    ap.add_argument("--assume-naive-as-local", action="store_true")
    ap.add_argument("--window", nargs=2, type=int, default=[7,23])
    ap.add_argument("--clock-at", choices=["start","end"], default="end")

    ap.add_argument("--bands", nargs="+", type=float, default=[-19.0, -5.0, 0.0, 5.0, 10.0, 20.0])
    ap.add_argument("--band-colors", nargs="+", default=["#8c00ff","#d000ff","#ffffff","#00ff65","#eeff00","#00e5ff"])    
    ap.add_argument("--auto-bands", type=int, default=0)
    ap.add_argument("--auto-scope", choices=["global","chunk"], default="global")
    ap.add_argument("--bands-method", choices=["quantile","minmax"], default="quantile")

    ap.add_argument("--seg-draw", choices=["polyline","path"], default="path")
    ap.add_argument("--seg-mode", choices=["flash","span","persist"], default="span")
    ap.add_argument("--seg-alpha", type=int, default=220)
    ap.add_argument("--seg-width", type=float, default=4.0)
    ap.add_argument("--seg-hold", type=float, default=10.0)

    ap.add_argument("--point-size", type=float, default=7.0)
    ap.add_argument("--point-mode", choices=["flash","persist"], default="persist")
    ap.add_argument("--point-hold", type=float, default=15.0)
    ap.add_argument("--point-no-depth-test", action="store_true")

    ap.add_argument("--head-point", action="store_true")
    ap.add_argument("--head-point-size", type=float, default=10.0)
    ap.add_argument("--head-no-depth-test", action="store_true")

    ap.add_argument("--no-keep-after-arrival", dest="keep_after_arrival", action="store_false")
    ap.set_defaults(keep_after_arrival=True)

    ap.add_argument("--chunk-hours", type=int, default=0)
    ap.add_argument("--debug", action="store_true")

    args = ap.parse_args()

    df = pd.read_csv(args.input_csv, sep=None, engine="python")
    df = auto_map_columns(df, args)

    df["_dt"] = df["timestamp"].astype(str).map(lambda s: to_datetime_utc(s, args.tz, args.assume_naive_as_local))
    df = df.sort_values(["device_id","_dt"]).reset_index(drop=True)
    if not len(df): raise SystemExit("No rows after parsing.")

    gstart = df["_dt"].min().replace(minute=0, second=0, microsecond=0)
    gend   = df["_dt"].max().replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    step   = (gend - gstart) if int(args.chunk_hours) <= 0 else timedelta(hours=max(1, int(args.chunk_hours)))

    def compute_auto_bands(values: pd.Series, n: int, method: str) -> List[float]:
        vals = pd.Series(values).dropna().astype(float).sort_values()
        if len(vals) < 2 or n <= 1: return []
        if method == "minmax":
            vmin, vmax = float(vals.min()), float(vals.max())
            if vmin == vmax: return [vmin]
            stepv = (vmax - vmin) / n
            return [vmin + stepv * k for k in range(1, n)]
        else:
            qs = [k / n for k in range(1, n)]
            thr = [float(vals.quantile(q)) for q in qs]
            return sorted(list({round(x, 6) for x in thr}))

    base_bands = list(map(float, args.bands))
    colors = list(args.band_colors)
    def ensure_colors(num_bands: int, palette: List[str]) -> List[str]:
        need = (num_bands + 1) - len(palette)
        if need > 0: palette = palette + [palette[-1]] * need
        return palette

    h_start, h_end = args.window
    part = 1
    t = gstart

    global_bands = None
    if int(args.auto_bands) > 0 and args.auto_scope == "global":
        series_for_bands = df["alt_hat"] if ("alt_hat" in df.columns) else df["alt"]
        global_bands = compute_auto_bands(series_for_bands, int(args.auto_bands), args.bands_method)
        if args.debug: print(f"[DEBUG] global auto-bands ({args.bands_method}) on {'alt_hat' if 'alt_hat' in df.columns else 'alt'}: {global_bands}")

    while t < gend:
        t2 = min(t + step, gend)
        sub = df[(df["_dt"] >= t) & (df["_dt"] < t2)].copy()
        if not len(sub):
            t = t2; continue

        if int(args.auto_bands) > 0:
            if args.auto_scope == "global" and global_bands is not None:
                bands = list(global_bands)
            else:
                series_for_bands = sub["alt_hat"] if ("alt_hat" in sub.columns) else sub["alt"]
                bands = compute_auto_bands(series_for_bands, int(args.auto_bands), args.bands_method)
                if args.debug: print(f"[DEBUG] chunk auto-bands ({args.bands_method}) on {'alt_hat' if 'alt_hat' in sub.columns else 'alt'}: {bands}")
        else:
            bands = list(base_bands)

        use_colors = ensure_colors(len(bands), list(colors))

        packets = build_packets_chunk(
            sub, args.tz, h_start, h_end,
            bands, use_colors,
            args.seg_draw, args.seg_mode, args.seg_alpha, args.seg_width, args.seg_hold,
            trail_time=0, lead_time=0, point_size=args.point_size,
            clock_at=args.clock_at,
            t_from=t, t_to=t2,
            point_mode=args.point_mode, point_hold=args.point_hold,
            point_no_depth_test=args.point_no_depth_test,
            head_point=args.head_point, head_point_size=args.head_point_size, head_no_depth_test=args.head_no_depth_test,
            keep_after_arrival=args.keep_after_arrival
        )

        if int(args.chunk_hours) <= 0:
            out_path = f"{args.out_prefix}_ALL.czml"
        else:
            tag = f"{t.astimezone(timezone.utc).strftime('%Y%m%d_%H%M')}-{t2.astimezone(timezone.utc).strftime('%H%M')}"
            out_path = f"{args.out_prefix}_{tag}_part{part:02d}.czml"

        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(packets, f, ensure_ascii=False)

        size_mb = os.path.getsize(out_path) / (1024*1024)
        print(f"Wrote: {out_path}  entities={len(packets)-1}  size={size_mb:.2f}MB")

        part += 1
        t = t2

if __name__ == "__main__":
    main()
