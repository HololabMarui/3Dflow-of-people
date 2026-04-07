#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV -> CZML converter (HAE-only) with:
- OD features (straight/bent lines, start/end markers, sample points)
- Auto Palette per device
- East/West split coloring by reference longitude
- Sample-points visibility window (--sp-window-seconds)
- No time-of-day filter, no TZ reinterpretation

Spec:
- CSVの時刻はそのまま採用（±補正なし / 文字列のタイムゾーンを信頼）
- 時間帯フィルタ無し（全サンプル使用）
- 高さは HAE（altitude_hae）を使用
- デバイス×ローカル日(既定 Asia/Tokyo) でエンティティ分割
- Gapでセグメント化（フェードなし、セグメント終端で消える）
- 色:
    - 既定: デバイス別自動色（--od-auto-color）
    - 手動指定 (--od-*/color)
    - 東西分け (--split-lon): サンプル点は「点ごと」に経度で判定して単色切替
      ライン/マーカーは「その日平均経度」で西/東のどちらかのパレット

CSV headers (case-sensitive):
  deviceID, log_ts, longitude, latitude, altitude_hae, altitude_hat, is_worker, gender, age
  * 本スクリプトは altitude_hae を使用します。

Usage:
  python csv_to_czml.py input.csv output.czml [--options...]
"""
from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from datetime import datetime, time, timezone, timedelta
from typing import Optional, Tuple, Dict, List
from zoneinfo import ZoneInfo  # Python 3.9+


# ---------- Utilities ----------

def parse_iso8601_to_utc(s: str) -> datetime:
    """Trust offset in the string; if none, assume UTC."""
    s = s.strip().replace("Z", "+00:00")
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def rgba_from_device_id(device_id: str, alpha: int = 255) -> List[int]:
    """Stable pseudo-random color per deviceID (avoid extremes)."""
    h = 0
    for ch in str(device_id):
        h = (h * 131 + ord(ch)) & 0xFFFFFFFF
    def chn(shift):
        val = (h >> shift) & 0xFF
        return int(50 + (val / 255.0) * 155)  # 50..205
    a = max(0, min(255, int(alpha)))
    return [chn(0), chn(8), chn(16), a]

def parse_hex_rgba(hex_str: Optional[str], alpha_default: int = 255) -> Optional[List[int]]:
    if not hex_str:
        return None
    s = hex_str.strip().lstrip("#")
    if len(s) == 6:
        r = int(s[0:2], 16); g = int(s[2:4], 16); b = int(s[4:6], 16); a = alpha_default
    elif len(s) == 8:
        r = int(s[0:2], 16); g = int(s[2:4], 16); b = int(s[4:6], 16); a = int(s[6:8], 16)
    else:
        raise ValueError(f"Unsupported color format: {hex_str}")
    return [r, g, b, a]

def isoformat_z(dt: datetime) -> str:
    return dt.replace(tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# ---- Palette helpers ----
def clamp(v, lo=0, hi=255): return max(lo, min(hi, int(v)))
def lerp(a, b, t): return a + (b - a) * t
def shade(rgb, factor):
    """factor<1: darker / >1: lighter"""
    r, g, b = rgb
    return [clamp(r*factor), clamp(g*factor), clamp(b*factor)]

def derive_palette_from_device(device_id: str, opacity: int):
    """Device-based stable palette -> (line=start, start=dark, end=light)."""
    base = rgba_from_device_id(device_id, alpha=opacity)  # [r,g,b,a]
    base_rgb = base[:3]
    start_rgb = shade(base_rgb, 0.85)
    end_rgb   = shade(base_rgb, 1.25)
    start_rgba = [start_rgb[0], start_rgb[1], start_rgb[2], opacity]
    end_rgba   = [end_rgb[0],   end_rgb[1],   end_rgb[2],   opacity]
    line_rgba  = start_rgba[:]
    return line_rgba, start_rgba, end_rgba


# ---------- Core ----------

def build_packets(grouped_rows_per_day: Dict[Tuple[str, datetime], List[dict]],
                  show_path: bool,
                  trail: float,
                  lead: float,
                  point_size: float,
                  opacity: int,
                  outline_width: float,
                  interp: str,
                  gap_threshold: float,
                  window_tz: ZoneInfo,
                  single_color_rgba: Optional[List[int]],
                  outline_color_hex: Optional[str],
                  # OD straight (start->end)
                  od_line: bool,
                  od_width: float,
                  od_color: Optional[str],
                  od_start_color: Optional[str],
                  od_end_color: Optional[str],
                  od_point_size: float,
                  od_labels: bool,
                  # Bent line through all samples
                  od_bent_line: bool,
                  od_bent_width: float,
                  od_bent_color: Optional[str],
                  # Per-sample clickable points
                  od_sample_points: bool,
                  od_sample_point_size: float,
                  od_sample_point_color: Optional[str],  # kept for compat
                  od_point_step: int,
                  # availability full-day
                  od_fullday: bool,
                  # auto palette
                  od_auto_color: bool,
                  # East/West split
                  split_lon: Optional[float],
                  west_start_hex: str,
                  west_end_hex: str,
                  east_start_hex: str,
                  east_end_hex: str,
                  # sample point visibility window
                  sp_window_seconds: Optional[float]):

    packets: List[dict] = []

    # Document packet (clock set later)
    doc = {
        "id": "document",
        "name": "CSV to CZML (HAE + OD + AutoPalette + E/W split + sp-window)",
        "version": "1.0",
        "clock": {}
    }
    packets.append(doc)

    # Global time bounds
    global_start = None
    global_end = None
    for rows in grouped_rows_per_day.values():
        for r in rows:
            t = r["_dt"]
            global_start = t if global_start is None or t < global_start else global_start
            global_end   = t if global_end   is None or t > global_end   else global_end
    if global_start is None:
        raise RuntimeError("No valid rows found in input CSV.")

    doc["clock"] = {
        "interval": f"{isoformat_z(global_start)}/{isoformat_z(global_end)}",
        "currentTime": isoformat_z(global_start),
        "multiplier": 1,
        "range": "UNBOUNDED"
    }

    interp_algo = "LINEAR" if interp.lower() == "linear" else "LAGRANGE"
    interp_degree = 1 if interp_algo == "LINEAR" else 5

    # Per-device per-day
    for (device_id, local_day_start_utc), rows in grouped_rows_per_day.items():
        rows.sort(key=lambda r: r["_dt"])
        if not rows:
            continue

        start = rows[0]["_dt"]
        end = rows[-1]["_dt"]
        epoch = isoformat_z(start)

        # Local-day availability override
        local_day_start_local = local_day_start_utc.astimezone(window_tz)
        local_day_end_local = local_day_start_local + timedelta(days=1)
        local_day_end_utc = local_day_end_local.astimezone(timezone.utc)

        avail_start = start if not od_fullday else local_day_start_utc
        avail_end   = end   if not od_fullday else local_day_end_utc

        # positions (HAE) + mean lon for per-entity palette
        coords: List[float] = []
        lon_sum = 0.0
        for r in rows:
            secs = (r["_dt"] - start).total_seconds()
            if secs < 0: secs = 0.0
            lon = float(r["longitude"])
            lat = float(r["latitude"])
            h   = float(r["altitude_hae"])   # HAE
            coords.extend([secs, lon, lat, h])
            lon_sum += lon
        mean_lon = lon_sum / len(rows)

        # time-dynamic alpha for moving point: segments by long gap (no fade)
        if single_color_rgba is not None:
            base_rgb = [single_color_rgba[0], single_color_rgba[1], single_color_rgba[2], opacity]
        else:
            base_rgb = rgba_from_device_id(device_id, alpha=opacity)
        r0, g0, b0, _ = base_rgb
        seg_starts = [rows[0]["_dt"]]
        seg_ends: List[datetime] = []
        for i in range(len(rows) - 1):
            t_cur = rows[i]["_dt"]; t_next = rows[i+1]["_dt"]
            gap = (t_next - t_cur).total_seconds()
            if gap_threshold > 0 and gap >= gap_threshold:
                seg_ends.append(t_cur); seg_starts.append(t_next)
        seg_ends.append(rows[-1]["_dt"])

        rgba_samples: List[float] = []
        def push_rgba(at_dt: datetime, a: int):
            sec = max(0.0, (at_dt - start).total_seconds())
            if rgba_samples:
                last_sec = rgba_samples[-5]
                sec = max(sec, last_sec)
            rgba_samples.extend([sec, r0, g0, b0, int(max(0, min(255, a)))])

        for s_dt, e_dt in zip(seg_starts, seg_ends):
            push_rgba(s_dt, 255)
            push_rgba(e_dt, 0)

        # Info from the day's LAST sample
        last_row = rows[-1]
        last_lon = float(last_row["longitude"])
        last_lat = float(last_row["latitude"])
        last_hae = float(last_row["altitude_hae"])
        last_ts  = str(last_row["log_ts"])

        entity_id = f"{device_id}_{local_day_start_local.strftime('%Y-%m-%d')}"

        props = {
            "deviceID": device_id,
            "日時": last_ts,
            "経度": last_lon,
            "緯度": last_lat,
            "高さ(HAE)": last_hae
        }
        for _k in ("is_worker", "gender", "age"):
            if _k in last_row and (last_row.get(_k) not in (None, "")):
                props[_k] = last_row.get(_k)

        # ------ Decide entity-level palette ------
        def ew_start_end_pairs():
            west_start = parse_hex_rgba(west_start_hex, opacity) if west_start_hex else [13,71,161,opacity]   # #0D47A1
            west_end   = parse_hex_rgba(west_end_hex,   opacity) if west_end_hex   else [144,202,249,opacity] # #90CAF9
            east_start = parse_hex_rgba(east_start_hex, opacity) if east_start_hex else [183,28,28,opacity]   # #B71C1C
            east_end   = parse_hex_rgba(east_end_hex,   opacity) if east_end_hex   else [255,205,210,opacity] # #FFCDD2
            return west_start, west_end, east_start, east_end

        if split_lon is not None:
            west_start, west_end, east_start, east_end = ew_start_end_pairs()
            if mean_lon < float(split_lon):  # West entity
                start_rgba, end_rgba = west_start, west_end
            else:                            # East entity
                start_rgba, end_rgba = east_start, east_end
            line_rgba = start_rgba[:]
        elif od_auto_color:
            line_rgba, start_rgba, end_rgba = derive_palette_from_device(device_id, opacity)
        else:
            start_rgba = parse_hex_rgba(od_start_color, alpha_default=opacity) if od_start_color else [0,200,83,opacity]
            end_rgba   = parse_hex_rgba(od_end_color,   alpha_default=opacity) if od_end_color   else [213,0,0,opacity]
            line_rgba  = parse_hex_rgba(od_color,       alpha_default=opacity) if od_color       else start_rgba[:]

        # Base moving-point packet (entity)
        packet = {
            "id": entity_id,
            "name": entity_id,
            "availability": f"{isoformat_z(avail_start)}/{isoformat_z(avail_end)}",
            "position": {
                "epoch": epoch,
                "cartographicDegrees": coords,
                "interpolationAlgorithm": interp_algo,
                "interpolationDegree": interp_degree
            },
            "point": {
                "pixelSize": float(point_size),
                "color": {"epoch": epoch, "rgba": rgba_samples},
                "heightReference": "NONE"
            },
            "properties": props
        }
        if outline_width > 0:
            outline_rgba = parse_hex_rgba(outline_color_hex, alpha_default=min(220, opacity)) if outline_color_hex else [0,0,0,min(220, opacity)]
            packet["point"]["outlineColor"] = {"rgba": outline_rgba}
            packet["point"]["outlineWidth"] = float(outline_width)

        # ---- OD straight line & start/end markers ----
        if od_line and len(rows) >= 1:
            first_row = rows[0]
            f_lon = float(first_row["longitude"]); f_lat = float(first_row["latitude"]); f_h = float(first_row["altitude_hae"])
            l_lon = float(last_row["longitude"]);  l_lat = float(last_row["latitude"]);  l_h = float(last_row["altitude_hae"])

            packet["polyline"] = {
                "positions": {"cartographicDegrees": [f_lon, f_lat, f_h,  l_lon, l_lat, l_h]},
                "width": float(od_width),
                "material": {"solidColor": {"color": {"rgba": line_rgba}}}
            }

            start_packet = {
                "id": f"{entity_id}_start",
                "name": f"{entity_id}_start",
                "availability": f"{isoformat_z(avail_start)}/{isoformat_z(avail_end)}",
                "position": {"cartographicDegrees": [f_lon, f_lat, f_h]},
                "point": {"pixelSize": float(od_point_size),
                          "color": {"rgba": start_rgba},
                          "outlineColor": {"rgba": [255,255,255,220]},
                          "outlineWidth": 1}
            }
            end_packet = {
                "id": f"{entity_id}_end",
                "name": f"{entity_id}_end",
                "availability": f"{isoformat_z(avail_start)}/{isoformat_z(avail_end)}",
                "position": {"cartographicDegrees": [l_lon, l_lat, l_h]},
                "point": {"pixelSize": float(od_point_size),
                          "color": {"rgba": end_rgba},
                          "outlineColor": {"rgba": [255,255,255,220]},
                          "outlineWidth": 1}
            }
            if od_labels:
                start_packet["label"] = {"text": "start","font":"12px sans-serif","fillColor":{"rgba":start_rgba},"pixelOffset":[0,-20]}
                end_packet["label"]   = {"text": "end",  "font":"12px sans-serif","fillColor":{"rgba":end_rgba},  "pixelOffset":[0,-20]}
            packets.append(start_packet); packets.append(end_packet)
        # ---- /OD straight ----

        # ---- Bent polyline through all samples (static) ----
        if od_bent_line and len(rows) >= 2:
            pl_positions: List[float] = []
            for r in rows:
                pl_positions.extend([float(r["longitude"]), float(r["latitude"]), float(r["altitude_hae"])])
            bent_packet = {
                "id": f"{entity_id}_bent",
                "name": f"{entity_id}_bent",
                "availability": f"{isoformat_z(avail_start)}/{isoformat_z(avail_end)}",
                "polyline": {
                    "positions": {"cartographicDegrees": pl_positions},
                    "width": float(od_bent_width),
                    "material": {"solidColor": {"color": {"rgba": line_rgba}}}
                }
            }
            packets.append(bent_packet)
        # ---- /bent ----

        # ---- Per-sample clickable points ----
        if od_sample_points and len(rows) >= 1:
            step = max(1, int(od_point_step))

            # For per-point E/W coloring when split-lon is set
            def ew_point_rgba(lon: float):
                if split_lon is None:
                    # fallback: entity start color（単色）
                    return start_rgba
                west_start = parse_hex_rgba(west_start_hex, opacity) if west_start_hex else [13,71,161,opacity]
                east_start = parse_hex_rgba(east_start_hex, opacity) if east_start_hex else [183,28,28,opacity]
                return west_start if lon < float(split_lon) else east_start

            for k, r in enumerate(rows[::step]):
                lon = float(r["longitude"]); lat = float(r["latitude"]); h = float(r["altitude_hae"])
                dt = r["_dt"]

                # availability for this sample point
                if sp_window_seconds is not None and sp_window_seconds > 0:
                    avail = f"{isoformat_z(dt)}/{isoformat_z(dt + timedelta(seconds=sp_window_seconds))}"
                else:
                    avail = f"{isoformat_z(avail_start)}/{isoformat_z(avail_end)}"

                # color for this sample point (per-point E/W if split-lon provided)
                sp_color = ew_point_rgba(lon)

                prop = {
                    "deviceID": device_id,
                    "日時": str(r["log_ts"]),
                    "経度": lon, "緯度": lat, "高さ(HAE)": h
                }
                for _k in ("is_worker", "gender", "age"):
                    if _k in r and (r.get(_k) not in (None, "")):
                        prop[_k] = r.get(_k)
                sp_packet = {
                    "id": f"{entity_id}_pt_{k*step}",
                    "name": f"{entity_id}_pt_{k*step}",
                    "availability": avail,
                    "position": {"cartographicDegrees": [lon, lat, h]},
                    "point": {
                        "pixelSize": float(od_sample_point_size),
                        "color": {"rgba": sp_color},
                        "outlineColor": {"rgba": [255,255,255,180]},
                        "outlineWidth": 1
                    },
                    "properties": prop
                }
                packets.append(sp_packet)
        # ---- /sample points ----

        if show_path:
            time_dynamic = {"epoch": epoch, "rgba": rgba_samples}
            packet["path"] = {
                "show": True,
                "width": 2,
                "material": {"polylineOutline": {
                    "color": time_dynamic,
                    "outlineColor": time_dynamic,
                    "outlineWidth": 1
                }},
                "resolution": 1
            }
            if trail >= 0: packet["path"]["trailTime"] = trail
            if lead  >= 0: packet["path"]["leadTime"]  = lead

        packets.append(packet)

    return packets


def convert_csv_to_czml(in_path: str, out_path: str,
                        show_path: bool, trail: float, lead: float,
                        point_size: float, opacity: int, outline_width: float,
                        interp: str,
                        # Gap (no fade)
                        gap_threshold: float,
                        # Color override
                        point_color_hex: Optional[str], outline_color_hex: Optional[str],
                        # OD straight
                        od_line: bool, od_width: float, od_color: Optional[str],
                        od_start_color: Optional[str], od_end_color: Optional[str],
                        od_point_size: float, od_labels: bool,
                        # bent / sample / fullday
                        od_bent_line: bool, od_bent_width: float, od_bent_color: Optional[str],
                        od_sample_points: bool, od_sample_point_size: float, od_sample_point_color: Optional[str],
                        od_point_step: int, od_fullday: bool,
                        # auto palette
                        od_auto_color: bool,
                        # day grouping / availability base tz
                        window_tz_name: str,
                        # East/West split
                        split_lon: Optional[float],
                        west_start_hex: str, west_end_hex: str,
                        east_start_hex: str, east_end_hex: str,
                        # sample point visibility window
                        sp_window_seconds: Optional[float]):

    win_tz = ZoneInfo(window_tz_name) if window_tz_name else timezone.utc

    # Grouped by (device_id, local_day_start_utc)
    grouped_per_day: Dict[Tuple[str, datetime], List[dict]] = defaultdict(list)

    with open(in_path, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames:
            reader.fieldnames = [h.strip().lstrip("\ufeff") for h in reader.fieldnames]

        required = {"deviceID", "log_ts", "longitude", "latitude", "altitude_hae"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise RuntimeError(
                f"CSV missing required headers: {sorted(list(missing))}. Found: {reader.fieldnames}"
            )

        for idx, row in enumerate(reader, 1):
            try:
                device_id = (row.get("deviceID") or "").strip()
                if not device_id:
                    continue

                # parse timestamps as-is
                dt_raw_utc = parse_iso8601_to_utc(row["log_ts"])
                local_dt = dt_raw_utc.astimezone(win_tz)

                # numeric validation
                _ = float(row["longitude"]); _ = float(row["latitude"])
                _ = float(row["altitude_hae"])

                # Local day bucket (00:00 local -> UTC)
                local_day = local_dt.date()
                local_day_start_local = datetime.combine(local_day, time(0, 0), tzinfo=win_tz)
                local_day_start_utc = local_day_start_local.astimezone(timezone.utc)

                row["_dt"] = dt_raw_utc
                grouped_per_day[(device_id, local_day_start_utc)].append(row)

            except Exception as e:
                print(f"[WARN] Skipping row {idx}: {e}")

    if not grouped_per_day:
        raise RuntimeError("No valid rows.")

    single_color_rgba = parse_hex_rgba(point_color_hex, alpha_default=opacity) if point_color_hex else None

    packets = build_packets(
        grouped_rows_per_day=grouped_per_day,
        show_path=show_path,
        trail=trail,
        lead=lead,
        point_size=point_size,
        opacity=opacity,
        outline_width=outline_width,
        interp=interp,
        gap_threshold=gap_threshold,
        window_tz=win_tz,
        single_color_rgba=single_color_rgba,
        outline_color_hex=outline_color_hex,
        # OD straight
        od_line=od_line, od_width=od_width, od_color=od_color,
        od_start_color=od_start_color, od_end_color=od_end_color,
        od_point_size=od_point_size, od_labels=od_labels,
        # bent / sample / fullday
        od_bent_line=od_bent_line, od_bent_width=od_bent_width, od_bent_color=od_bent_color,
        od_sample_points=od_sample_points, od_sample_point_size=od_sample_point_size,
        od_sample_point_color=od_sample_point_color, od_point_step=od_point_step,
        od_fullday=od_fullday,
        # auto palette
        od_auto_color=od_auto_color,
        # E/W split
        split_lon=split_lon,
        west_start_hex=west_start_hex, west_end_hex=west_end_hex,
        east_start_hex=east_start_hex, east_end_hex=east_end_hex,
        # sample point visibility window
        sp_window_seconds=sp_window_seconds
    )

    with open(out_path, "w", encoding="utf-8", newline="") as out:
        json.dump(packets, out, ensure_ascii=False, separators=(",", ":"))

    total_rows = sum(len(v) for v in grouped_per_day.values())
    print(f"[OK] Wrote CZML: {out_path}")
    print(f"     Devices(days): {len(grouped_per_day)}  Rows used: {total_rows}")


# ---------- CLI ----------

def main():
    ap = argparse.ArgumentParser(description="CSV -> CZML (HAE, per-day split, OD/static lines & points, E/W split, sp-window).")
    ap.add_argument("input_csv", help="Path to input CSV")
    ap.add_argument("output_czml", help="Path to output CZML (no -o option; positional)")

    # Visuals (points & path)
    ap.add_argument("--show-path", action="store_true", help="Show path (trail/lead). Default off.")
    ap.add_argument("--trail", type=float, default=60.0, help="Trail seconds (only if --show-path). Use -1 to disable.")
    ap.add_argument("--lead", type=float, default=0.0, help="Lead seconds (only if --show-path). Use -1 to disable.")
    ap.add_argument("--point-size", type=float, default=10.0, help="Point pixel size (default 10).")
    ap.add_argument("--opacity", type=int, default=255, help="Point alpha 0-255 (default 255).")
    ap.add_argument("--outline-width", type=float, default=1.0, help="Point outline width px (0 to disable).")
    ap.add_argument("--outline-color", type=str, default="#000000FF", help='Outline color, e.g. "#FF0000" or "#FF000080"')
    ap.add_argument("--point-color", type=str, default=None, help='Override to single color, e.g. "#00BCD4FF" or "#00BCD4"')

    # OD straight (origin→destination)
    ap.add_argument("--od-line", action="store_true", help="Add a static straight O→D line per device/day.")
    ap.add_argument("--od-width", type=float, default=3.0, help="OD straight line width.")
    ap.add_argument("--od-color", type=str, default=None, help='OD straight line color (used when not using auto/split).')
    ap.add_argument("--od-start-color", type=str, default=None, help='Start point color (manual mode).')
    ap.add_argument("--od-end-color", type=str, default=None, help='End point color (manual mode).')
    ap.add_argument("--od-point-size", type=float, default=12.0, help="Start/End point pixel size.")
    ap.add_argument("--od-labels", action="store_true", help='Show "start"/"end" labels.')

    # Bent polyline through all samples
    ap.add_argument("--od-bent-line", action="store_true", help="Add a static bent polyline through all samples.")
    ap.add_argument("--od-bent-width", type=float, default=3.0, help="Bent polyline width.")
    ap.add_argument("--od-bent-color", type=str, default=None, help="Bent polyline color (manual mode).")

    # Per-sample clickable points
    ap.add_argument("--od-sample-points", action="store_true", help="Add clickable points for each sample (decimatable).")
    ap.add_argument("--od-sample-point-size", type=float, default=8.0, help="Sample point size.")
    ap.add_argument("--od-sample-point-color", type=str, default=None, help="(Unused when E/W is used; kept for compatibility).")
    ap.add_argument("--od-point-step", type=int, default=1, help="Use every Nth sample to reduce density (>=1).")

    # availability full-day (local day in specified window_tz)
    ap.add_argument("--od-fullday", action="store_true", help="Make entity availability the full local day (00:00–24:00).")

    # Auto palette (device-based same hue)
    ap.add_argument("--od-auto-color", action="store_true",
                    help="DeviceID-based auto palette (line=start darkish, end lighter, samples gradient).")

    # Day grouping / availability base TZ (default JST)
    ap.add_argument("--window-tz", type=str, default="Asia/Tokyo",
                    help='IANA timezone for per-day split & --od-fullday boundary (default "Asia/Tokyo").')

    # Gap default: 30min
    ap.add_argument("--gap-threshold", type=float, default=1800.0, help="Gap seconds to end a segment (default 1800=30min).")

    # East/West split by reference longitude
    ap.add_argument("--split-lon", type=float, default=None,
                    help="Reference longitude: points with lon<split are West, else East. Lines/markers palette is chosen by day's mean lon.")
    ap.add_argument("--west-start-color", type=str, default="#0D47A1FF", help="West start (deep blue).")
    ap.add_argument("--west-end-color",   type=str, default="#90CAF9FF", help="West end (pale blue).")
    ap.add_argument("--east-start-color", type=str, default="#B71C1CFF", help="East start (deep red).")
    ap.add_argument("--east-end-color",   type=str, default="#FFCDD2FF", help="East end (pale red).")

    # NEW: sample point visibility window (seconds)
    ap.add_argument("--sp-window-seconds", type=float, default=None,
                    help="各サンプル点の可視ウィンドウ秒。例: 3600 で各点は時刻tからt+3600秒だけ表示。未指定なら従来どおり。")

    args = ap.parse_args()

    convert_csv_to_czml(
        in_path=args.input_csv,
        out_path=args.output_czml,
        show_path=args.show_path,
        trail=args.trail,
        lead=args.lead,
        point_size=args.point_size,
        opacity=args.opacity,
        outline_width=args.outline_width,
        interp="linear",
        gap_threshold=args.gap_threshold,
        point_color_hex=args.point_color,
        outline_color_hex=args.outline_color,
        # OD straight
        od_line=args.od_line, od_width=args.od_width, od_color=args.od_color,
        od_start_color=args.od_start_color, od_end_color=args.od_end_color,
        od_point_size=args.od_point_size, od_labels=args.od_labels,
        # bent / sample / fullday
        od_bent_line=args.od_bent_line, od_bent_width=args.od_bent_width, od_bent_color=args.od_bent_color,
        od_sample_points=args.od_sample_points, od_sample_point_size=args.od_sample_point_size,
        od_sample_point_color=args.od_sample_point_color, od_point_step=args.od_point_step,
        od_fullday=args.od_fullday,
        # auto palette
        od_auto_color=args.od_auto_color,
        # day grouping base tz
        window_tz_name=args.window_tz,
        # E/W split
        split_lon=args.split_lon,
        west_start_hex=args.west_start_color, west_end_hex=args.west_end_color,
        east_start_hex=args.east_start_color, east_end_hex=args.east_end_color,
        # sample point visibility window
        sp_window_seconds=args.sp_window_seconds
    )

if __name__ == "__main__":
    main()
