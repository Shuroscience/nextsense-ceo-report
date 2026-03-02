#!/usr/bin/env python3
"""
NextSense User Report: Smartbuds users activated since Feb 9, 2026
Pulls data from Mixpanel via direct HTTP, computes metrics, generates HTML report.
Designed to run in CI via GitHub Actions.
"""

import json
import os
import sys
import time
import urllib.request
import urllib.parse
import urllib.error
import base64
from datetime import datetime, timedelta, timezone
from collections import defaultdict

# ─── Configuration ───────────────────────────────────────────────────────────

API_SECRET = os.environ.get("MIXPANEL_API_SECRET")
if not API_SECRET:
    print("ERROR: MIXPANEL_API_SECRET environment variable not set.")
    sys.exit(1)

AUTH = base64.b64encode(f"{API_SECRET}:".encode()).decode()

TODAY = datetime.now(timezone.utc).date()
ACTIVATION_DATE = datetime(2026, 2, 9).date()
FROM_DATE = ACTIVATION_DATE
TO_DATE = TODAY
SESSION_MIN_DURATION_SEC = 90 * 60  # 90 minutes in seconds

fmt = lambda d: d.strftime("%Y-%m-%d")

# This will be populated before export_events is called
INTERNAL_IDS = set()


# ─── Mixpanel API helpers ────────────────────────────────────────────────────

_last_export_time = 0  # track time of last API call for throttling


def _export_single_chunk(from_date, to_date, label, event_names=None):
    """Export a single date range from Mixpanel, excluding internal users."""
    global _last_export_time
    # Throttle: wait at least 7 seconds between consecutive export calls
    elapsed = time.time() - _last_export_time
    if _last_export_time > 0 and elapsed < 7:
        time.sleep(7 - elapsed)

    params = {"from_date": fmt(from_date), "to_date": fmt(to_date)}
    if event_names:
        params["event"] = json.dumps(event_names)
    url = f"https://data.mixpanel.com/api/2.0/export?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"Authorization": f"Basic {AUTH}"})
    events = []
    elapsed_s = 0
    print(f"  [{label}] Requesting {fmt(from_date)} to {fmt(to_date)}...", flush=True)
    for attempt in range(5):
        try:
            t0 = time.time()
            with urllib.request.urlopen(req, timeout=600) as resp:
                for line in resp:
                    line = line.decode("utf-8").strip()
                    if line:
                        try:
                            ev = json.loads(line)
                            did = ev.get("properties", {}).get("distinct_id", "")
                            if did not in INTERNAL_IDS:
                                events.append(ev)
                        except json.JSONDecodeError:
                            pass
            elapsed_s = time.time() - t0
            break
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < 4:
                wait = 60 * (2 ** attempt)  # exponential: 60, 120, 240, 480
                wait = min(wait, 300)
                print(f"  Rate limited (429), retrying in {wait}s...", flush=True)
                time.sleep(wait)
                events = []
            else:
                raise
    _last_export_time = time.time()
    suffix = f" ({', '.join(event_names)})" if event_names else ""
    print(f"  [{label}] {len(events)} events in {elapsed_s:.1f}s{suffix}", flush=True)
    return events


def export_events(from_date, to_date, label="", event_names=None, chunk_days=30):
    """Export events from Mixpanel, excluding internal users.

    Automatically chunks large date ranges into smaller pieces to avoid
    CI timeouts on slow runners. Default chunk size is 30 days.
    """
    total_days = (to_date - from_date).days
    if total_days <= chunk_days:
        return _export_single_chunk(from_date, to_date, label, event_names)

    # Split into chunks
    all_events = []
    chunk_start = from_date
    chunk_num = 1
    while chunk_start <= to_date:
        chunk_end = min(chunk_start + timedelta(days=chunk_days - 1), to_date)
        chunk_label = f"{label} chunk {chunk_num}"
        all_events.extend(_export_single_chunk(chunk_start, chunk_end, chunk_label, event_names))
        chunk_start = chunk_end + timedelta(days=1)
        chunk_num += 1
    print(f"  [{label}] Total: {len(all_events)} events across {chunk_num - 1} chunks", flush=True)
    return all_events


def fetch_all_profiles():
    """Fetch all user profiles with pagination."""
    all_profiles = []
    page = 0
    session_id = None
    while True:
        params = {"page": page}
        if session_id:
            params["session_id"] = session_id
        data = urllib.parse.urlencode(params).encode()
        url = "https://mixpanel.com/api/2.0/engage"
        req = urllib.request.Request(url, data=data, headers={"Authorization": f"Basic {AUTH}"})
        with urllib.request.urlopen(req) as resp:
            result = json.loads(resp.read().decode("utf-8"))
        all_profiles.extend(result.get("results", []))
        session_id = result.get("session_id")
        total = result.get("total", 0)
        if len(all_profiles) >= total or not result.get("results"):
            break
        page += 1
    return all_profiles


def identify_internal_users(profiles):
    """Return set of distinct_ids for internal/NextSense users."""
    internal = set()
    for r in profiles:
        did = r.get("$distinct_id", "")
        p = r.get("$properties", {})
        cohort = p.get("user_cohort", "")
        email = p.get("email", "")
        if cohort.lower() == "internal" or "nextsense" in email.lower():
            internal.add(did)
    return internal


def _safe_num(val, default=0):
    """Safely convert a value to a number."""
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def stats(data, label=""):
    if not data:
        return {"n": 0, "mean": 0, "median": 0, "min": 0, "max": 0, "p25": 0, "p75": 0}
    data_sorted = sorted(data)
    n = len(data_sorted)
    mean = sum(data_sorted) / n
    median = data_sorted[n // 2] if n % 2 == 1 else (data_sorted[n//2 - 1] + data_sorted[n//2]) / 2
    p25 = data_sorted[int(n * 0.25)]
    p75 = data_sorted[int(n * 0.75)]
    return {"n": n, "mean": round(mean, 1), "median": round(median, 1),
            "min": round(min(data_sorted), 1), "max": round(max(data_sorted), 1),
            "p25": round(p25, 1), "p75": round(p75, 1)}


# ── Step 1: Fetch all data ──────────────────────────────────────────────
print("=" * 60)
print("STEP 1: Fetching data from Mixpanel")
print("=" * 60)

print("\n1a. Fetching user profiles...")
all_profiles = fetch_all_profiles()
print(f"  Total profiles: {len(all_profiles)}")

# ── Step 2: Identify internal users ─────────────────────────────────────
print("\n" + "=" * 60)
print("STEP 2: Filtering internal users")
print("=" * 60)

INTERNAL_IDS = identify_internal_users(all_profiles)
print(f"  Internal users identified: {len(INTERNAL_IDS)}")

# Build profile lookup (non-internal only)
profile_lookup = {}
for p in all_profiles:
    did = p["$distinct_id"]
    if did not in INTERNAL_IDS:
        profile_lookup[did] = p.get("$properties", {})

# ── Step 1b/c/d: Fetch events (after INTERNAL_IDS is populated) ────────
print("\n1b. Fetching onboarding events...")
onboarding_events = export_events(
    FROM_DATE, TO_DATE, label="onboarding",
    event_names=["change_onboarding_completed"]
)
print(f"  Total onboarding events: {len(onboarding_events)}")

print("\n1c. Fetching session_statistics events...")
session_events = export_events(
    FROM_DATE, TO_DATE, label="session_statistics",
    event_names=["session_statistics"]
)
print(f"  Total session_statistics events: {len(session_events)}")

print("\n1d. Fetching health_kit_successful_sync events...")
healthkit_events = export_events(
    FROM_DATE, TO_DATE, label="healthkit",
    event_names=["health_kit_successful_sync"]
)
print(f"  Total HealthKit sync events: {len(healthkit_events)}")


# ── Step 3: Identify activated users (onboarded since Feb 9) ────────────
print("\n" + "=" * 60)
print("STEP 3: Identifying activated users since Feb 9")
print("=" * 60)

# Find users whose first onboarding_completed=true event is on or after Feb 9
activation_dates = {}
for evt in onboarding_events:
    props = evt.get("properties", {})
    did = props.get("distinct_id")
    if did in INTERNAL_IDS:
        continue
    if props.get("isOnboardingCompleted") != True:
        continue
    evt_time = datetime.fromtimestamp(props["time"], tz=timezone.utc).date()
    if evt_time >= ACTIVATION_DATE:
        if did not in activation_dates or evt_time < activation_dates[did]:
            activation_dates[did] = evt_time

activated_users = set(activation_dates.keys())
print(f"  Users activated since Feb 9 (via event): {len(activated_users)}")


# ── Step 4: Analyze sleep sessions for activated cohort ─────────────────
print("\n" + "=" * 60)
print("STEP 4: Analyzing sleep sessions (>90 min duration)")
print("=" * 60)

# Group session_statistics by user, filtering for sleep type and >90 min
user_sessions = defaultdict(list)
for evt in session_events:
    props = evt.get("properties", {})
    did = props.get("distinct_id")
    if did not in activated_users:
        continue
    if props.get("type") != "sleep":
        continue
    duration_sec = _safe_num(props.get("cumulativeSessionDuration", 0))
    if duration_sec < SESSION_MIN_DURATION_SEC:
        continue

    session_info = {
        "date": props.get("calendarDay", ""),
        "duration_min": round(duration_sec / 60, 1),
        "total_sleep_min": _safe_num(props.get("totalSleepMinutes", 0)),
        "slow_wave_min": _safe_num(props.get("slowWaveMinutes", 0)),
        "audio_stim": _safe_num(props.get("audioStimulation", 0)),
        "session_id": props.get("sessionId", ""),
        "wake_count": _safe_num(props.get("wakeCount", 0)),
        "total_awake_min": _safe_num(props.get("totalAwakeMinutes", 0)),
        "total_light_min": _safe_num(props.get("totalLightSleepMinutes", 0)),
    }
    user_sessions[did].append(session_info)

print(f"  Users with at least 1 qualifying sleep session: {len(user_sessions)}")


# ── Step 5: Compute all metrics ─────────────────────────────────────────
print("\n" + "=" * 60)
print("STEP 5: Computing metrics")
print("=" * 60)

# Q1: Total activated
q1_total = len(activated_users)
print(f"\n  Q1 - Activated users: {q1_total}")

# Q2: Zero nights
q2_zero = [uid for uid in activated_users if len(user_sessions.get(uid, [])) == 0]
print(f"  Q2 - Zero nights: {len(q2_zero)}")

# Q3: >50% of eligible nights
q3_high_usage = []
q3_details = []
for uid in activated_users:
    act_date = activation_dates[uid]
    eligible_nights = (TODAY - act_date).days
    if eligible_nights <= 0:
        eligible_nights = 1

    # Count unique calendar days with qualifying sessions
    session_dates = set()
    for s in user_sessions.get(uid, []):
        cal_day = s["date"]
        if cal_day:
            try:
                d = datetime.fromisoformat(cal_day).date()
                session_dates.add(d)
            except Exception:
                pass

    usage_rate = len(session_dates) / eligible_nights if eligible_nights > 0 else 0
    name = profile_lookup.get(uid, {}).get("name", "Unknown")
    detail = {
        "name": name,
        "uid": uid,
        "activated": act_date.isoformat(),
        "eligible_nights": eligible_nights,
        "sessions": len(session_dates),
        "rate": usage_rate
    }
    q3_details.append(detail)
    if usage_rate > 0.5:
        q3_high_usage.append(detail)

print(f"  Q3 - >50% eligible nights: {len(q3_high_usage)}/{q1_total}")

# Q4: Tips replacement proxy
# Method A: onboarded >3 weeks ago (21+ days since activation)
three_weeks_ago = TODAY - timedelta(days=20)
q4a_over_3wks = [uid for uid in activated_users if activation_dates[uid] <= three_weeks_ago]
# Method B: >21 unique nights with a session
q4b_over_21 = []
for uid in activated_users:
    session_nights = set()
    for s in user_sessions.get(uid, []):
        try:
            d = datetime.fromisoformat(s["date"]).date()
            session_nights.add(d)
        except Exception:
            pass
    if len(session_nights) > 21:
        q4b_over_21.append(uid)
print(f"  Q4a - Onboarded >3 weeks ago: {len(q4a_over_3wks)}/{q1_total}")
print(f"  Q4b - >21 sleep sessions: {len(q4b_over_21)}/{q1_total}")

# Q5: Potential churn (no events for >14 days)
q5_churn = []
for uid in activated_users:
    props = profile_lookup.get(uid, {})
    last_seen_str = props.get("$last_seen", "")
    if last_seen_str:
        try:
            last_seen = datetime.fromisoformat(last_seen_str).date()
            days_inactive = (TODAY - last_seen).days
            if days_inactive > 14:
                q5_churn.append({
                    "name": props.get("name", "Unknown"),
                    "uid": uid,
                    "last_seen": last_seen_str,
                    "days_inactive": days_inactive
                })
        except Exception:
            pass

print(f"  Q5 - Potential churn (>14 days inactive): {len(q5_churn)}/{q1_total}")

# Q6: Sleep metrics across all activated users
all_sws_minutes = []
all_audio_stim = []
all_total_sleep = []
all_session_duration = []

for uid in activated_users:
    for s in user_sessions.get(uid, []):
        all_sws_minutes.append(s["slow_wave_min"])
        all_audio_stim.append(s["audio_stim"])
        all_total_sleep.append(s["total_sleep_min"])
        all_session_duration.append(s["duration_min"])

sws_stats = stats(all_sws_minutes, "Slow Wave Minutes")
stim_stats = stats(all_audio_stim, "Audio Stimulations (Boost)")
sleep_stats = stats(all_total_sleep, "Total Sleep Minutes")

print(f"\n  Q6 - Sleep Metrics ({sws_stats['n']} total qualifying sessions)")
print(f"       Slow Wave Minutes: median={sws_stats['median']}, mean={sws_stats['mean']}, range=[{sws_stats['min']}-{sws_stats['max']}]")
print(f"       Boost (Audio Stim): median={stim_stats['median']}, mean={stim_stats['mean']}, range=[{stim_stats['min']}-{stim_stats['max']}]")
print(f"       Total Sleep Minutes: median={sleep_stats['median']}, mean={sleep_stats['mean']}, range=[{sleep_stats['min']}-{sleep_stats['max']}]")

# Per-user averages for Q6
user_avg_metrics = []
for uid in activated_users:
    sessions = user_sessions.get(uid, [])
    if not sessions:
        user_avg_metrics.append({
            "name": profile_lookup.get(uid, {}).get("name", "Unknown"),
            "n_sessions": 0, "avg_sws": 0, "avg_stim": 0, "avg_sleep": 0
        })
        continue
    avg_sws = sum(s["slow_wave_min"] for s in sessions) / len(sessions)
    avg_stim = sum(s["audio_stim"] for s in sessions) / len(sessions)
    avg_sleep = sum(s["total_sleep_min"] for s in sessions) / len(sessions)
    user_avg_metrics.append({
        "name": profile_lookup.get(uid, {}).get("name", "Unknown"),
        "n_sessions": len(sessions),
        "avg_sws": round(avg_sws, 1),
        "avg_stim": round(avg_stim, 1),
        "avg_sleep": round(avg_sleep, 1),
    })

# Q7: HealthKit / other wearable
healthkit_users = set()
for evt in healthkit_events:
    did = evt.get("properties", {}).get("distinct_id")
    if did in activated_users:
        healthkit_users.add(did)

print(f"\n  Q7 - Users with HealthKit sync: {len(healthkit_users)}/{q1_total}")

# ── Step 5b: Lifecycle categories ─────────────────────────────────────
print("\n  Computing lifecycle categories...")

CATEGORY_DISPLAY = [
    ("highly_active", "Highly Active", "#00C2A8"),
    ("active",        "Active",        "#71D688"),
    ("resurrected",   "Resurrected",   "#B26CDD"),
    ("at_risk",       "At-Risk",       "#D84516"),
    ("churned",       "Churned",       "#8B5E3C"),
    ("never_active",  "Never Active",  "#949494"),
]

def get_session_dates(uid):
    """Return sorted list of unique date objects for a user's qualifying sessions."""
    dates = set()
    for s in user_sessions.get(uid, []):
        try:
            dates.add(datetime.fromisoformat(s["date"]).date())
        except Exception:
            pass
    return sorted(dates)

LIFECYCLE = {}  # uid -> category key

for uid in activated_users:
    session_dates = get_session_dates(uid)

    if not session_dates:
        LIFECYCLE[uid] = "never_active"
        continue

    has_recent = any(d >= TODAY - timedelta(days=7) for d in session_dates)

    # Check for resurrection: was there ever a gap >15 days?
    gap_points = [activation_dates[uid]] + session_dates
    had_churn_gap = False
    for i in range(1, len(gap_points)):
        if (gap_points[i] - gap_points[i - 1]).days > 15:
            had_churn_gap = True
            break

    if had_churn_gap and has_recent:
        LIFECYCLE[uid] = "resurrected"
        continue

    # Highly Active: avg 4+ sessions/week over rolling 4-week window
    four_weeks_ago = TODAY - timedelta(days=28)
    recent_4w = [d for d in session_dates if d >= four_weeks_ago]
    weeks_active = min(4.0, max(1.0, (TODAY - max(activation_dates[uid], four_weeks_ago)).days / 7.0))
    if len(recent_4w) / weeks_active >= 4:
        LIFECYCLE[uid] = "highly_active"
        continue

    if has_recent:
        LIFECYCLE[uid] = "active"
        continue

    last_session = session_dates[-1]
    days_since_last = (TODAY - last_session).days

    if 8 <= days_since_last <= 15:
        LIFECYCLE[uid] = "at_risk"
        continue

    if days_since_last > 15:
        LIFECYCLE[uid] = "churned"
        continue

    # Fallback (session 1-7 days ago, didn't match above)
    LIFECYCLE[uid] = "active"

category_counts = {key: 0 for key, _, _ in CATEGORY_DISPLAY}
for uid, cat in LIFECYCLE.items():
    category_counts[cat] += 1

for key, label, _ in CATEGORY_DISPLAY:
    print(f"    {label}: {category_counts[key]}")

# Build SVG donut chart
import math
total_users = sum(category_counts.values())
radius = 80
circumference = 2 * math.pi * radius
stroke_width = 32
cx, cy = 120, 120

segments_svg = ""
offset = 0
for key, label, color in CATEGORY_DISPLAY:
    count = category_counts[key]
    if count == 0:
        continue
    pct = count / total_users
    dash = circumference * pct
    gap = circumference - dash
    segments_svg += (
        f'<circle cx="{cx}" cy="{cy}" r="{radius}" '
        f'fill="none" stroke="{color}" stroke-width="{stroke_width}" '
        f'stroke-dasharray="{dash:.2f} {gap:.2f}" '
        f'stroke-dashoffset="{-offset:.2f}" '
        f'transform="rotate(-90 {cx} {cy})" />'
    )
    offset += dash

donut_svg = (
    f'<svg width="240" height="240" viewBox="0 0 240 240">'
    f'{segments_svg}'
    f'<text x="{cx}" y="{cy - 8}" text-anchor="middle" fill="#2A2B3F" '
    f'font-size="36" font-weight="300" font-family="DM Sans, sans-serif">{total_users}</text>'
    f'<text x="{cx}" y="{cy + 14}" text-anchor="middle" fill="#999" '
    f'font-size="12" font-family="DM Sans, sans-serif">USERS</text>'
    f'</svg>'
)

legend_html = ""
for key, label, color in CATEGORY_DISPLAY:
    count = category_counts[key]
    pct = round(count * 100 / total_users) if total_users else 0
    legend_html += (
        f'<div style="display:flex;align-items:center;gap:10px;padding:6px 0;">'
        f'<span style="width:12px;height:12px;border-radius:50%;background:{color};flex-shrink:0;"></span>'
        f'<span style="font-size:14px;flex:1;color:#2A2B3F;">{label}</span>'
        f'<span style="font-size:14px;font-weight:600;color:#2A2B3F;font-variant-numeric:tabular-nums;">{count}</span>'
        f'<span style="font-size:13px;color:#888;width:40px;text-align:right;font-variant-numeric:tabular-nums;">{pct}%</span>'
        f'</div>'
    )

# Build lifecycle-aware Q5 breakdown
churned_users = sorted(
    [uid for uid, cat in LIFECYCLE.items() if cat == "churned"],
    key=lambda u: profile_lookup.get(u, {}).get("name", "Unknown")
)
at_risk_users = sorted(
    [uid for uid, cat in LIFECYCLE.items() if cat == "at_risk"],
    key=lambda u: profile_lookup.get(u, {}).get("name", "Unknown")
)
resurrected_users = sorted(
    [uid for uid, cat in LIFECYCLE.items() if cat == "resurrected"],
    key=lambda u: profile_lookup.get(u, {}).get("name", "Unknown")
)

def _last_session_days(uid):
    dates = get_session_dates(uid)
    if dates:
        return (TODAY - dates[-1]).days
    return 999

churned_rows = ""
for uid in sorted(churned_users, key=lambda u: -_last_session_days(u)):
    name = profile_lookup.get(uid, {}).get("name", "Unknown")
    days = _last_session_days(uid)
    last_seen = profile_lookup.get(uid, {}).get("$last_seen", "")[:10]
    churned_rows += f"<tr><td>{name}</td><td>{last_seen}</td><td>{days} days</td></tr>"

at_risk_rows = ""
for uid in sorted(at_risk_users, key=lambda u: -_last_session_days(u)):
    name = profile_lookup.get(uid, {}).get("name", "Unknown")
    days = _last_session_days(uid)
    last_seen = profile_lookup.get(uid, {}).get("$last_seen", "")[:10]
    at_risk_rows += f"<tr><td>{name}</td><td>{last_seen}</td><td>{days} days</td></tr>"

resurrected_rows = ""
for uid in resurrected_users:
    name = profile_lookup.get(uid, {}).get("name", "Unknown")
    dates = get_session_dates(uid)
    last_session = dates[-1].strftime("%b %d") if dates else "—"
    resurrected_rows += f"<tr><td>{name}</td><td>{last_session}</td></tr>"

q5_churned_detail = ""
if churned_users:
    q5_churned_detail = "<details><summary>See " + str(len(churned_users)) + " churned users</summary><div class='detail-content'><div class='table-wrap'><table><thead><tr><th>User</th><th>Last Seen</th><th>Days Since Last Session</th></tr></thead><tbody>" + churned_rows + "</tbody></table></div></div></details>"

q5_at_risk_detail = ""
if at_risk_users:
    q5_at_risk_detail = "<details><summary>See " + str(len(at_risk_users)) + " at-risk users</summary><div class='detail-content'><div class='table-wrap'><table><thead><tr><th>User</th><th>Last Seen</th><th>Days Since Last Session</th></tr></thead><tbody>" + at_risk_rows + "</tbody></table></div></div></details>"

q5_resurrected_detail = ""
if resurrected_users:
    q5_resurrected_detail = "<details><summary>See " + str(len(resurrected_users)) + " resurrected users</summary><div class='detail-content'><div class='table-wrap'><table><thead><tr><th>User</th><th>Last Session</th></tr></thead><tbody>" + resurrected_rows + "</tbody></table></div></div></details>"


# ── Step 6: Generate HTML report ────────────────────────────────────────
print("\n" + "=" * 60)
print("STEP 6: Generating HTML report")
print("=" * 60)

# Build user detail rows for the master table
user_rows_html = ""
for uid in sorted(activated_users, key=lambda u: activation_dates[u]):
    props = profile_lookup.get(uid, {})
    name = props.get("name", "Unknown")
    act_date = activation_dates[uid].strftime("%b %d")
    sessions = user_sessions.get(uid, [])
    n_sessions = len(sessions)

    # Usage rate
    eligible = (TODAY - activation_dates[uid]).days
    session_dates = set()
    for s in sessions:
        try:
            d = datetime.fromisoformat(s["date"]).date()
            session_dates.add(d)
        except Exception:
            pass
    rate = len(session_dates) / eligible * 100 if eligible > 0 else 0

    # Avg SWS
    avg_sws = round(sum(s["slow_wave_min"] for s in sessions) / len(sessions), 0) if sessions else 0
    avg_stim = round(sum(s["audio_stim"] for s in sessions) / len(sessions), 1) if sessions else 0

    # Last seen
    last_seen = props.get("$last_seen", "")[:10]
    days_since = ""
    try:
        ls = datetime.fromisoformat(props.get("$last_seen", "")).date()
        days_since = f"{(TODAY - ls).days}d ago"
    except Exception:
        pass

    has_hk = "Yes" if uid in healthkit_users else "No"

    # Lifecycle status
    _status_map = {
        "highly_active": '<span style="color:#00C2A8;font-weight:600">Highly Active</span>',
        "active":        '<span style="color:#71D688;font-weight:600">Active</span>',
        "resurrected":   '<span style="color:#B26CDD;font-weight:600">Resurrected</span>',
        "at_risk":       '<span style="color:#D84516;font-weight:600">At-Risk</span>',
        "churned":       '<span style="color:#8B5E3C;font-weight:600">Churned</span>',
        "never_active":  '<span style="color:#949494;font-weight:600">Never Active</span>',
    }
    status = _status_map.get(LIFECYCLE.get(uid, ""), '<span style="color:#949494">Unknown</span>')

    user_rows_html += f"""<tr>
        <td>{name}</td>
        <td>{act_date}</td>
        <td>{n_sessions}</td>
        <td>{len(session_dates)}/{eligible} ({rate:.0f}%)</td>
        <td>{avg_sws:.0f}</td>
        <td>{avg_stim}</td>
        <td>{has_hk}</td>
        <td>{last_seen}<br><small style="color:#949494">{days_since}</small></td>
        <td>{status}</td>
    </tr>"""

# Q5 churn detail
churn_rows = ""
for c in sorted(q5_churn, key=lambda x: -x["days_inactive"]):
    churn_rows += f"<tr><td>{c['name']}</td><td>{c['last_seen'][:10]}</td><td>{c['days_inactive']} days</td></tr>"

# Q6 per-user metrics
metrics_rows = ""
for m in sorted(user_avg_metrics, key=lambda x: -x["n_sessions"]):
    if m["n_sessions"] == 0:
        metrics_rows += f"<tr><td>{m['name']}</td><td>0</td><td colspan='3' style='color:#949494'>No qualifying sessions</td></tr>"
    else:
        metrics_rows += f"<tr><td>{m['name']}</td><td>{m['n_sessions']}</td><td>{m['avg_sws']}</td><td>{m['avg_stim']}</td><td>{m['avg_sleep']}</td></tr>"

BRAND_LOGO = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAIIAAABMCAYAAACh4W85AAAACXBIWXMAAAWJAAAFiQFtaJ36AAAAAXNSR0IArs4c6QAAAARnQU1BAACxjwv8YQUAAAXbSURBVHgB7Z2NceM2EEbXMyngSmAHcQlKB+4g7CDuQOzA7kBOBb4OOKnA1wGuAzsVvBADOtbJksnFPym+GUxycScAAj8uFrsAJFIpwJ9D6YfyTTauk+Hh7/ngZRPDlWEf+FCe+cwmhmtheNDNUAyXsWJoZGO9DA94N5RXpjGbGFbK8GD/QofZxLAicP7AAT/MJoYVgPMHXgjDbGJYMMPDu+Vrp3ATw9pB7w9sYsjEjWRieFgPw3/uJQ0/h/LHzc3NT6mQUai3Q3mPhbyN5cfQ5ze5BnD+QE96jFQCzhG+H7/31LLY+kqHodzKWiGuPzBFJ4XBif6BeTGRc/RD2cmawCWNfAdEi6FgCBpnAfbE48Aa/J7IgzKHVgqBi4oa4mNYqhhwb0ZPXor5BqQXvLWoy/IdmE4apaKRAuB8gRy8shTLwPykUWwOUgD8Q+O+GGpPw5PfHzgenEYyQ34RvPMgNUJY0ugYu462a26Djk4yQzkRvLOTmiBO0sjyxGjyOL8z6RLZHUTKi8DSSy0Qb7nUHdXZ6D6ad7lIuAh6nNWzY2eDbHe4l8CH8r4CcZJG1qncndTbKz6f1UEkTASGL8w5fpY1Vb5mHsRZLhlOHLzhzy06GskEYSL4f9qbaOMbOgv7XUpAPH/g+XRg0MceHiUTBIpA2dZOUXf+ABrxkkbdhfoPijoMmawBGUVw1KYmDpPPTyBO0sh+vr1Qv3ZKaCUDFBDB2G6vaKeRHBAnSGS4ECdHPyVkMYcUEsHYdq9oq5GUEC9p1POF+UI/4I0khoIiGNvX+GGNpIJ4SaPHiXY0jpEl+XKR8iJQxVEkFbgAR4yk0eQaF53YkmfeKCyCsQ8af+lFUkBifyCwrU4SQgUiGPthFO2miSPYwSaMWYdT0YeRkzqI1CMCbbS2lVTgL4ZHZq5p0fsfrSSCekTg45c1khL0Ypgd80Y/JTxLAghfFT1JRNALMk+ehXli+JQ0mqhTOyVYGokMTgQhofIniYjnuOzEA3wikXwtBoPyIaF/A6OrnspEMPbJoMPLSuJ8EIPPy8V5MXxKGs2oR7MsggT5BOoUgXZcQP8C2u/9EDy2/CqGTpTg5wi1EhHqFIE27WzplG1cyhgbAsTQigfo9y9EXS5SoQjGfu3RoRoXpjPG9u8ayQFOkdpIZbSDHNQrAh8Hcfa4MD8mYcghBvRzYDQHkUpFMPbtgI7ZG3HwsMBkiEloH0QjEaBuEdzpujLvUAthsRFDKjGgN3+dRICKRTD2z6CjnVFnjB1khhQ7ntApP4qDSP0iiD5VkngHWYwv3Sk6Erw1m/pFoF1G23/bTNS5JxxDypPW6E4sBXWEykUw9vGAjla+/r6a8b1E+uuMme+4vEoALEMEWn/pMFGXIZw8RwKYLwRv/4AFiGDsZ6/ok+HCW0q8awfynZJCYbrEA5YjAq2D2F6oZ084htwnqdEd7twp6w4VQSeZQGfGezn/XQ+Ek94fuDAA94pOzj7mzbJEsFf069MGXRJcO5AdXJBDw35GnaED00km0DuI9yeft/6AIZxOSoPesTlwxnzxcX9hiKPUSUZQnuc8+ax2I+s5VDvIkoL/ZtgeN5AH5l1fO0UnGUFvDZujzya5dqAouDe5xC1rx3SSGTzOcxLvruqeGm9eI/y8RAidZAa/7Wc9cV6YTmoFvy1ZMegkM5S7bDRd0igmuDkz5xTRSQEoc+OaYUnX8+JnMn3opAD4bT8LpWeJF3bjxJDKMth676QQxMkGash2j1QSSDOP9hR8M8hn7d4pe7VeLHAO5CPhGCoImpDPQTSs8ad+cNbhCf100VNJ1Ix8l5FnSRr9JgUYf42ttf8/Plg7x/8u7lfQjoMib0P5Zyg/hvK9sl9E+1fS8/dQ7q/ml+CWCmmDZuvwB66FBGKoJ2m0oYN4qwfDEuMDGx9EEIP62oGNSgkQQycb60IphmUkjTb8mCkGw+YPrJ8JMfRs/sD1cEEMy04abfhxIoYtSHTN4K4KqDpp9B/mftqw9rTBUQAAAABJRU5ErkJggg=="

# Warn/good color classes
q2_val_class = "val-red" if len(q2_zero) > q1_total * 0.3 else ("val-yellow" if len(q2_zero) > q1_total * 0.15 else "val-green")
q3_val_class = "val-green" if len(q3_high_usage) > q1_total * 0.4 else ("val-yellow" if len(q3_high_usage) > q1_total * 0.2 else "val-red")
q5_risk_total = category_counts["at_risk"] + category_counts["churned"]
q5_val_class = "val-green" if q5_risk_total < q1_total * 0.05 else ("val-yellow" if q5_risk_total < q1_total * 0.15 else "val-red")

# Pre-build detail HTML strings (avoids nested f-string escaping issues)
q2_detail_html = ""
if q2_zero:
    items = "".join(f'<li>{profile_lookup.get(uid, {}).get("name", "Unknown")} &mdash; activated {activation_dates[uid].strftime("%b %d")}</li>' for uid in sorted(q2_zero, key=lambda u: activation_dates[u]))
    q2_detail_html = f"<details><summary>See {len(q2_zero)} users with zero sessions</summary><div class='detail-content'><ul>{items}</ul></div></details>"

q5_detail_html = ""
if q5_churn:
    q5_detail_html = "<details><summary>See " + str(len(q5_churn)) + " potentially churned users</summary><div class='detail-content'><div class='table-wrap'><table><thead><tr><th>User</th><th>Last Seen</th><th>Inactive For</th></tr></thead><tbody>" + churn_rows + "</tbody></table></div></div></details>"

q7_detail_html = ""
if healthkit_users:
    hk_items = "".join(f'<li>{profile_lookup.get(uid, {}).get("name", "Unknown")}</li>' for uid in sorted(healthkit_users, key=lambda u: profile_lookup.get(u, {}).get("name", "Unknown")))
    q7_detail_html = f"<details><summary>See {len(healthkit_users)} users with HealthKit sync</summary><div class='detail-content'><ul>{hk_items}</ul></div></details>"

q3_table_rows = "".join(f'<tr><td>{d["name"]}</td><td>{d["activated"]}</td><td>{d["eligible_nights"]}</td><td>{d["sessions"]}</td><td class="current-val {"green" if d["rate"]>0.5 else "red" if d["rate"]<0.2 else ""}"">{"%.0f" % (d["rate"]*100)}%</td></tr>' for d in sorted(q3_details, key=lambda x: -x["rate"]))

html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Smartbuds User Report — {TODAY.strftime('%B %-d, %Y')}</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,300;0,9..40,400;0,9..40,500;0,9..40,600;1,9..40,400&family=Source+Serif+4:wght@400;600&display=swap" rel="stylesheet">
<style>
  :root {{
    --night: #2A2B3F;
    --white: #FFFFFF;
    --yellow: #FFFB6C;
    --lavender: #A1A0FF;
    --berry: #6C5CFF;
    --periwinkle: #8CC7F8;
    --coral: #FF6C6B;
    --green: #4DB86A;
    --night-light: #363752;
    --night-lighter: #42435c;
    --brand-blue: #C1CFF8;
    --link-blue: #7F9EF8;
    --lavender-faint: rgba(161,160,255,0.06);
    --lavender-tint: rgba(161,160,255,0.12);
    --radius: 16px;
  }}

  * {{ box-sizing: border-box; margin: 0; padding: 0; }}

  body {{
    font-family: 'DM Sans', -apple-system, BlinkMacSystemFont, sans-serif;
    color: var(--night);
    background: var(--night);
    padding: 0;
    line-height: 1.6;
    min-height: 100vh;
    overflow-x: hidden;
  }}

  body::before, body::after {{
    content: '';
    position: fixed;
    border-radius: 50%;
    pointer-events: none;
    z-index: 0;
  }}
  body::before {{
    width: 600px; height: 600px;
    top: -200px; left: -150px;
    background: radial-gradient(circle, rgba(108,92,255,0.12) 0%, transparent 70%);
  }}
  body::after {{
    width: 500px; height: 500px;
    bottom: -100px; right: -100px;
    background: radial-gradient(circle, rgba(140,199,248,0.10) 0%, transparent 70%);
  }}

  .accent-bar {{
    height: 3px;
    background: linear-gradient(90deg, var(--yellow), var(--lavender), var(--periwinkle));
    position: relative;
    z-index: 1;
  }}

  .page-nav {{
    display: flex;
    justify-content: center;
    gap: 4px;
    padding: 12px 16px 0;
    position: relative;
    z-index: 1;
  }}
  .page-nav a {{
    color: rgba(255,255,255,0.40);
    text-decoration: none;
    font-size: 13px;
    font-weight: 500;
    padding: 8px 20px;
    border-radius: 8px 8px 0 0;
    transition: color 0.2s, background 0.2s;
  }}
  .page-nav a:hover {{ color: rgba(255,255,255,0.70); background: rgba(255,255,255,0.04); }}
  .page-nav a.active {{ color: var(--white); background: rgba(255,255,255,0.08); }}

  .page-header {{
    background: transparent;
    padding: 44px 20px 40px;
    text-align: center;
    position: relative;
    z-index: 1;
  }}
  .page-header .brand-mark {{ display: inline-block; margin-bottom: 16px; }}
  .page-header .brand-mark img {{ display: block; height: 32px; width: auto; }}
  .page-header h1 {{
    font-family: 'DM Sans', sans-serif;
    font-size: 30px;
    font-weight: 300;
    color: var(--white);
    margin-bottom: 6px;
    letter-spacing: -0.01em;
  }}
  .page-header .subtitle {{
    color: rgba(255,255,255,0.50);
    font-size: 15px;
    font-weight: 400;
    margin-bottom: 4px;
  }}
  .page-header .generated {{
    color: rgba(255,255,255,0.30);
    font-size: 13px;
  }}

  /* ── Hero stat cards ───────────────────────────── */
  .hero-grid {{
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 14px;
    max-width: 880px;
    margin: 32px auto 0;
    padding: 0 16px;
  }}
  .hero-card {{
    background: var(--night-light);
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: var(--radius);
    padding: 28px 16px 22px;
    text-align: center;
    transition: transform 0.2s ease, box-shadow 0.2s ease, border-color 0.2s ease;
    position: relative;
    overflow: hidden;
  }}
  .hero-card::before {{
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
    background: linear-gradient(90deg, var(--yellow), var(--lavender));
    opacity: 0;
    transition: opacity 0.2s ease;
  }}
  .hero-card:hover {{
    transform: translateY(-2px);
    box-shadow: 0 12px 32px rgba(0,0,0,0.25);
    border-color: rgba(161,160,255,0.15);
  }}
  .hero-card:hover::before {{ opacity: 1; }}
  .hero-card .hero-val {{
    font-family: 'DM Sans', sans-serif;
    font-size: 44px;
    font-weight: 300;
    color: var(--yellow);
    line-height: 1.1;
    font-variant-numeric: tabular-nums;
  }}
  .hero-val.val-green {{ color: var(--green); }}
  .hero-val.val-yellow {{ color: #E8D44D; }}
  .hero-val.val-red {{ color: var(--coral); }}
  .hero-card .hero-label {{
    font-size: 11px;
    font-weight: 500;
    color: rgba(255,255,255,0.50);
    margin-top: 10px;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    line-height: 1.3;
  }}
  .hero-card .hero-sub {{
    font-size: 13px;
    color: rgba(255,255,255,0.30);
    margin-top: 4px;
    font-variant-numeric: tabular-nums;
  }}

  /* ── Content container ─────────────────────────── */
  .content {{
    max-width: 920px;
    margin: 0 auto;
    padding: 28px 16px 60px;
    position: relative;
    z-index: 1;
  }}

  /* ── Section cards ─────────────────────────────── */
  .section-card {{
    background: var(--white);
    border-radius: var(--radius);
    box-shadow: 0 2px 8px rgba(161,160,255,0.06), 0 0 0 1px rgba(161,160,255,0.04);
    padding: 36px 40px;
    margin-bottom: 20px;
    opacity: 0;
    transform: translateY(16px);
    transition: opacity 0.5s ease, transform 0.5s ease;
  }}
  .section-card.visible {{
    opacity: 1;
    transform: translateY(0);
  }}

  /* ── Typography ────────────────────────────────── */
  h2 {{
    font-family: 'DM Sans', sans-serif;
    font-size: 22px;
    font-weight: 300;
    color: var(--night);
    margin-bottom: 20px;
    letter-spacing: -0.01em;
  }}
  h3 {{
    font-family: 'DM Sans', sans-serif;
    font-size: 14px;
    font-weight: 500;
    color: var(--night);
    margin: 24px 0 12px;
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }}
  p {{
    margin-bottom: 12px;
    color: #555;
    font-size: 15px;
  }}

  /* ── Tables ────────────────────────────────────── */
  .table-wrap {{
    overflow-x: auto;
    -webkit-overflow-scrolling: touch;
    margin: 0 -8px 24px;
    padding: 0 8px;
  }}
  table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 14px;
    font-variant-numeric: tabular-nums;
    min-width: 580px;
    margin-bottom: 0;
  }}
  th {{
    background: var(--night);
    color: rgba(255,255,255,0.70);
    text-align: left;
    padding: 11px 14px;
    font-weight: 500;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    border: none;
    white-space: nowrap;
  }}
  th:first-child {{ border-radius: var(--radius) 0 0 0; }}
  th:last-child {{ border-radius: 0 var(--radius) 0 0; }}
  td {{
    padding: 11px 14px;
    border-bottom: 1px solid var(--lavender-faint);
    color: var(--night);
    white-space: nowrap;
  }}
  td:first-child {{ white-space: normal; }}
  tbody tr:nth-child(even) td {{ background: var(--lavender-faint); }}
  tbody tr:hover td {{ background: var(--lavender-tint); }}
  .current-val {{ font-weight: 600; color: var(--night); font-size: 15px; font-variant-numeric: tabular-nums; }}
  .green {{ color: var(--green); }}
  .red {{ color: var(--coral); }}
  .status-warn {{ color: #ca8a04; }}

  /* ── Method / note boxes ───────────────────────── */
  .method-note {{
    background: var(--lavender-faint);
    border-left: 3px solid var(--lavender);
    padding: 16px 20px;
    border-radius: 0 var(--radius) var(--radius) 0;
    margin: 16px 0 4px;
    font-size: 13px;
    color: #666;
  }}
  .method-note strong {{ color: var(--night); }}
  .method-note code {{ background: rgba(161,160,255,0.10); padding: 1px 5px; border-radius: 4px; font-size: 12px; }}

  .na-note {{
    background: rgba(255,108,107,0.06);
    border: 1px solid rgba(255,108,107,0.18);
    padding: 16px 20px;
    border-radius: var(--radius);
    margin: 16px 0 24px;
    font-size: 13px;
    color: var(--night);
  }}

  /* ── Stat cards (Q6 metrics) ───────────────────── */
  .stat-grid {{
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 14px;
    margin: 16px 0;
  }}
  .stat-card {{
    background: var(--lavender-faint);
    border-radius: var(--radius);
    padding: 20px;
  }}
  .stat-card .stat-label {{
    font-size: 11px;
    font-weight: 500;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: #888;
    margin-bottom: 8px;
  }}
  .stat-card .stat-main {{
    font-size: 28px;
    font-weight: 300;
    color: var(--night);
    line-height: 1.2;
    font-variant-numeric: tabular-nums;
  }}
  .stat-card .stat-main small {{
    font-size: 14px;
    color: #999;
    font-weight: 400;
  }}
  .stat-card .stat-detail {{
    font-size: 13px;
    color: #888;
    margin-top: 6px;
    font-variant-numeric: tabular-nums;
  }}

  /* ── Details / toggle ──────────────────────────── */
  details {{ margin: 16px 0; }}
  details summary {{
    cursor: pointer;
    font-size: 13px;
    font-weight: 500;
    color: var(--link-blue);
    padding: 10px 16px;
    background: var(--lavender-faint);
    border-radius: 10px;
    user-select: none;
    transition: background 0.2s ease, color 0.2s ease;
    list-style: none;
  }}
  details summary::-webkit-details-marker {{ display: none; }}
  details summary::before {{ content: '\\25B8\\00a0\\00a0'; }}
  details[open] summary::before {{ content: '\\25BE\\00a0\\00a0'; }}
  details summary:hover {{ background: var(--lavender-tint); color: var(--berry); }}
  details[open] summary {{ margin-bottom: 12px; border-radius: 10px 10px 0 0; }}
  details .detail-content {{
    animation: fadeIn 0.25s ease;
  }}
  details .detail-content ul {{
    list-style: none;
    padding: 0;
    columns: 2;
    column-gap: 24px;
  }}
  details .detail-content li {{
    padding: 6px 14px;
    font-size: 13px;
    color: #555;
    border-radius: 8px;
    margin-bottom: 2px;
    break-inside: avoid;
  }}
  details .detail-content li:hover {{ background: var(--lavender-faint); }}
  @keyframes fadeIn {{ from {{ opacity: 0; transform: translateY(-4px); }} to {{ opacity: 1; transform: translateY(0); }} }}

  /* ── Footer ────────────────────────────────────── */
  .report-footer {{
    text-align: center;
    padding: 24px 16px;
    font-size: 12px;
    color: rgba(255,255,255,0.25);
    position: relative;
    z-index: 1;
  }}

  /* ── Responsive ────────────────────────────────── */
  @media (max-width: 768px) {{
    .page-header {{ padding: 32px 16px 28px; }}
    .page-header h1 {{ font-size: 24px; }}
    .hero-grid {{ grid-template-columns: repeat(2, 1fr); gap: 10px; padding: 0 12px; }}
    .hero-card {{ padding: 20px 12px 16px; }}
    .hero-card .hero-val {{ font-size: 32px; }}
    .hero-card .hero-label {{ font-size: 10px; }}
    .content {{ padding: 20px 10px 40px; }}
    .section-card {{ padding: 24px 18px; border-radius: 14px; }}
    .stat-grid {{ grid-template-columns: 1fr; }}
    details .detail-content ul {{ columns: 1; }}
    h2 {{ font-size: 20px; }}
    h3 {{ font-size: 13px; }}
  }}
  @media (max-width: 420px) {{
    .hero-grid {{ grid-template-columns: repeat(2, 1fr); gap: 8px; }}
    .hero-card .hero-val {{ font-size: 28px; }}
    .hero-card .hero-label {{ font-size: 9px; }}
    .section-card {{ padding: 20px 14px; }}
    table {{ font-size: 13px; }}
  }}
</style>
</head>
<body>
<div class="accent-bar"></div>


<div class="page-header">
  <div class="brand-mark">
    <img src="{BRAND_LOGO}" alt="NextSense" />
  </div>
  <h1>Smartbuds User Report</h1>
  <div class="subtitle">Users activated since February 9, 2026</div>
  <div class="generated">Generated {TODAY.strftime('%B %-d, %Y')} &middot; Internal users excluded ({len(INTERNAL_IDS)} accounts)</div>

  <div class="hero-grid">
    <div class="hero-card">
      <div class="hero-val">{q1_total}</div>
      <div class="hero-label">Users Activated</div>
      <div class="hero-sub">Completed onboarding</div>
    </div>
    <div class="hero-card">
      <div class="hero-val {q2_val_class}">{len(q2_zero)}</div>
      <div class="hero-label">Users with Zero Sleep Sessions (&gt;90m)</div>
      <div class="hero-sub">{len(q2_zero)*100//q1_total if q1_total else 0}% of cohort</div>
    </div>
    <div class="hero-card">
      <div class="hero-val {q3_val_class}">{len(q3_high_usage)}</div>
      <div class="hero-label">Users Who Slept with Buds &gt;50% Eligible Nights</div>
      <div class="hero-sub">{len(q3_high_usage)*100//q1_total if q1_total else 0}% of cohort</div>
    </div>
    <div class="hero-card">
      <div class="hero-val {q5_val_class}">{q5_risk_total}</div>
      <div class="hero-label">At-Risk + Churned</div>
      <div class="hero-sub">{category_counts["at_risk"]} at-risk &middot; {category_counts["churned"]} churned</div>
    </div>
  </div>
</div>

<div class="content">

<!-- ──────────── Q1 ──────────── -->
<div class="section-card visible">
  <h2>Q1: How many users have activated since Feb 9?</h2>
  <p>
    <strong>{q1_total} users</strong> completed onboarding between Feb 9 and {TODAY.strftime('%b %-d')}, 2026.
  </p>
  <div class="method-note">
    <strong>How this was measured:</strong> Counted unique users whose first <code>change_onboarding_completed</code> event
    (with <code>isOnboardingCompleted = true</code>) occurred on or after Feb 9. Internal users excluded.
  </div>
</div>

<!-- ──────────── LIFECYCLE DISTRIBUTION ──────────── -->
<div class="section-card visible">
  <h2>User Lifecycle Distribution</h2>
  <p>Current status of all <strong>{total_users} activated users</strong> based on sleep session activity.</p>
  <div style="display:flex;align-items:center;gap:40px;flex-wrap:wrap;margin:24px 0;">
    <div>{donut_svg}</div>
    <div style="flex:1;min-width:220px;">{legend_html}</div>
  </div>
  <div class="method-note">
    <strong>Category definitions:</strong><br>
    <strong style="color:#00C2A8">Highly Active</strong> = avg 4+ sessions/week (rolling 4 weeks) &middot;
    <strong style="color:#71D688">Active</strong> = &ge;1 session in last 7 days &middot;
    <strong style="color:#B26CDD">Resurrected</strong> = returned after &gt;15 day gap &middot;
    <strong style="color:#D84516">At-Risk</strong> = 8&ndash;15 days since last session &middot;
    <strong style="color:#8B5E3C">Churned</strong> = &gt;15 days since last session &middot;
    <strong style="color:#949494">Never Active</strong> = onboarded but zero sessions.
  </div>
</div>

<!-- ──────────── Q2 ──────────── -->
<div class="section-card visible">
  <h2>Q2: How many have zero nights of sleep?</h2>
  <p>
    <strong>{len(q2_zero)} of {q1_total} users ({len(q2_zero)*100//q1_total if q1_total else 0}%)</strong> completed onboarding but have not logged a single qualifying sleep session (&gt;90 min).
    These users are classified as <strong style="color:#949494">&ldquo;Never Active.&rdquo;</strong>
  </p>
  {q2_detail_html}
  <div class="method-note">
    <strong>How this was measured:</strong> A "sleep session" = <code>session_statistics</code> event with
    <code>type = sleep</code> and <code>cumulativeSessionDuration &gt; 90 min</code>. Users with zero such sessions are counted here.
  </div>
</div>

<!-- ──────────── Q3 ──────────── -->
<div class="section-card visible">
  <h2>Q3: How many have used earbuds &gt;50% of eligible nights?</h2>
  <p>
    <strong>{len(q3_high_usage)} of {q1_total} users ({len(q3_high_usage)*100//q1_total if q1_total else 0}%)</strong> have used the earbuds on more than half their eligible nights since activation.
  </p>
  <details><summary>See usage breakdown by user ({q1_total} users)</summary><div class="detail-content">
  <div class="table-wrap"><table>
    <thead><tr><th>User</th><th>Activated</th><th>Eligible Nights</th><th>Nights Used</th><th>Rate</th></tr></thead>
    <tbody>{q3_table_rows}</tbody>
  </table></div></div></details>
  <div class="method-note">
    <strong>How this was measured:</strong> "Eligible nights" = days between activation and today.
    "Nights used" = unique calendar days with &ge;1 qualifying session. Rate = nights used / eligible nights.
  </div>
</div>

<!-- ──────────── Q4 ──────────── -->
<div class="section-card visible">
  <h2>Q4: Who likely needs replacement tips &amp; sleeves?</h2>
  <p>Two proxy metrics since Mixpanel doesn&rsquo;t track hardware wear directly:</p>
  <div class="stat-grid" style="grid-template-columns: repeat(2, 1fr);">
    <div class="stat-card">
      <div class="stat-label">Method A: Onboarded &ge;20 days ago</div>
      <div class="stat-main">{len(q4a_over_3wks)} <small>/ {q1_total}</small></div>
      <div class="stat-detail">{len(q4a_over_3wks)*100//q1_total if q1_total else 0}% of cohort</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Method B: &gt;21 unique nights</div>
      <div class="stat-main">{len(q4b_over_21)} <small>/ {q1_total}</small></div>
      <div class="stat-detail">{len(q4b_over_21)*100//q1_total if q1_total else 0}% of cohort</div>
    </div>
  </div>
  <div class="method-note">
    <strong>How this was measured:</strong>
    <strong>Method A</strong> counts users who onboarded 20+ days ago.
    <strong>Method B</strong> counts users with more than 21 unique nights that include a qualifying sleep session (&gt;90 min).
    Multiple sessions on the same night count as one night.
    Both are proxies &mdash; actual tip wear depends on usage intensity, fit, and individual factors.
  </div>
</div>

<!-- ──────────── Q5 ──────────── -->
<div class="section-card visible">
  <h2>Q5: Engagement Risk &amp; Recovery</h2>

  <div class="stat-grid" style="grid-template-columns: repeat(3, 1fr);">
    <div class="stat-card" style="border-left:4px solid #D84516;">
      <div class="stat-label">At-Risk</div>
      <div class="stat-main" style="color:#D84516;">{category_counts["at_risk"]}</div>
      <div class="stat-detail">8&ndash;15 days since last session</div>
    </div>
    <div class="stat-card" style="border-left:4px solid #8B5E3C;">
      <div class="stat-label">Churned</div>
      <div class="stat-main" style="color:#8B5E3C;">{category_counts["churned"]}</div>
      <div class="stat-detail">&gt;15 days since last session</div>
    </div>
    <div class="stat-card" style="border-left:4px solid #B26CDD;">
      <div class="stat-label">Resurrected</div>
      <div class="stat-main" style="color:#B26CDD;">{category_counts["resurrected"]}</div>
      <div class="stat-detail">Returned after &gt;15 day gap</div>
    </div>
  </div>

  {q5_at_risk_detail}
  {q5_churned_detail}
  {q5_resurrected_detail}

  <div class="method-note">
    <strong>How this was measured:</strong> Based on days since last completed sleep session (&gt;90 min).
    <strong style="color:#D84516">At-Risk</strong> = 8&ndash;15 days.
    <strong style="color:#8B5E3C">Churned</strong> = &gt;15 days.
    <strong style="color:#B26CDD">Resurrected</strong> = had a &gt;15 day gap but completed a session in the last 7 days.<br><br>
    <strong>Limitation:</strong> This does not tell us whether a user has <em>returned the product</em>. Return/refund data
    would need to come from Shopify or your CX system. A &ldquo;Returned&rdquo; category will be added once Shopify data is synced.
  </div>
</div>

<!-- ──────────── Q6 ──────────── -->
<div class="section-card visible">
  <h2>Q6: Sleep metrics across all activated users</h2>
  <p style="margin-bottom:20px">Across <strong>{sws_stats['n']:,} qualifying sessions</strong> from {len(user_sessions)} users:</p>

  <div class="stat-grid">
    <div class="stat-card">
      <div class="stat-label">Slow Wave Sleep</div>
      <div class="stat-main">{sws_stats['median']} <small>min median</small></div>
      <div class="stat-detail">Mean {sws_stats['mean']} &middot; IQR {sws_stats['p25']}&ndash;{sws_stats['p75']} &middot; Range {sws_stats['min']}&ndash;{sws_stats['max']}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Boost (Audio Stim)</div>
      <div class="stat-main">{stim_stats['median']} <small>median</small></div>
      <div class="stat-detail">Mean {stim_stats['mean']} &middot; IQR {stim_stats['p25']}&ndash;{stim_stats['p75']} &middot; Range {stim_stats['min']}&ndash;{stim_stats['max']}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Total Sleep</div>
      <div class="stat-main">{sleep_stats['median']} <small>min median</small></div>
      <div class="stat-detail">Mean {sleep_stats['mean']} &middot; IQR {sleep_stats['p25']}&ndash;{sleep_stats['p75']} &middot; Range {sleep_stats['min']}&ndash;{sleep_stats['max']}</div>
    </div>
  </div>

  <details><summary>See per-user averages ({q1_total} users)</summary><div class="detail-content">
  <div class="table-wrap"><table>
    <thead><tr><th>User</th><th>Sessions</th><th>Avg SWS (min)</th><th>Avg Boost</th><th>Avg Sleep (min)</th></tr></thead>
    <tbody>{metrics_rows}</tbody>
  </table></div></div></details>

  <div class="na-note">
    <strong>Note on Sleep Quality (SQ):</strong> No explicit SQ score was found in Mixpanel. The data includes
    <code>totalSleepMinutes</code>, <code>slowWaveMinutes</code>, <code>audioStimulation</code>, <code>wakeCount</code>,
    and <code>wasoMinutes</code> &mdash; but no composite SQ metric. Check Firebase or the backend pipeline.
  </div>

  <div class="method-note">
    <strong>How this was measured:</strong> Aggregated from <code>session_statistics</code> events with
    <code>type = sleep</code> and duration &gt;90 min. "Slow Wave Minutes" = <code>slowWaveMinutes</code>.
    "Boost" = <code>audioStimulation</code> count. "Total Sleep" = <code>totalSleepMinutes</code>.
  </div>
</div>

<!-- ──────────── Q7 ──────────── -->
<div class="section-card visible">
  <h2>Q7: How many have another wearable (benchmarking)?</h2>
  <p>
    <strong>{len(healthkit_users)} of {q1_total} users ({len(healthkit_users)*100//q1_total if q1_total else 0}%)</strong> have at least one HealthKit sync event,
    suggesting they use an Apple Watch or other HealthKit-connected device.
  </p>
  {q7_detail_html}
  <div class="method-note">
    <strong>How this was measured:</strong> Checked for <code>health_kit_successful_sync</code> events in this cohort.
    A HealthKit sync implies Apple Health access, which <em>often</em> (but not always) means an Apple Watch or similar wearable.<br><br>
    <strong>Limitation:</strong> Only captures Apple HealthKit. Users with Fitbit, Oura, Garmin, or other
    non-HealthKit wearables would not appear here.
  </div>
</div>

<!-- ──────────── MASTER TABLE ──────────── -->
<div class="section-card visible">
  <h2>Full User Detail</h2>
  <details><summary>See all {q1_total} users</summary><div class="detail-content">
  <div class="table-wrap"><table>
    <thead><tr><th>User</th><th>Activated</th><th>Sessions</th><th>Usage Rate</th><th>Avg SWS</th><th>Avg Boost</th><th>HealthKit</th><th>Last Seen</th><th>Lifecycle</th></tr></thead>
    <tbody>{user_rows_html}</tbody>
  </table></div></div></details>
</div>

</div><!-- /content -->

<div class="report-footer">
  Data source: Mixpanel (project 386573) &middot; Feb 9 &ndash; {TODAY.strftime('%b %-d')}, 2026 &middot;
  {len(INTERNAL_IDS)} internal users excluded &middot;
  Sleep session = session_statistics with type=sleep and duration &gt;90 min<br>
  Generated {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")} UTC
</div>

<script>
// Fade-in on scroll
document.querySelectorAll('.section-card').forEach(card => {{
  card.classList.add('visible');
}});
</script>

</body>
</html>"""

# Write output
output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
os.makedirs(output_dir, exist_ok=True)
output_path = os.path.join(output_dir, "index.html")
with open(output_path, "w") as f:
    f.write(html)

print(f"\n  Report written to {output_path}")
print("  Done!")
