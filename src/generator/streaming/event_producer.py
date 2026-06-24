"""
src/generator/streaming/event_producer.py

Chunk-based streaming event generator (1 chunk = 1 day).
Memory usage stays constant regardless of days_history.

Two event streams are combined per day:
  1. Background events — random activity at baseline/burst rates (bursty traffic, late
     arrivals, duplicates as described in design/01 Section 3.3).
  2. Transaction-anchored sessions — one correlated event sequence per transaction.
     Fraud transactions inject otp_failed (1–3) + transaction_declined probes (1–2) in
     the 30-minute window before the transaction attempt, giving the streaming feature
     store (f_stream_otp_failed_count_30m, f_stream_decline_count_30m) real ML signal.
"""

import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from tqdm import tqdm


EVENT_TYPES    = np.array(["login","card_check","transaction_attempt","transaction_approved",
                            "transaction_declined","otp_request","otp_failed"])
EVENT_WEIGHTS  = np.array([0.20, 0.08, 0.25, 0.22, 0.08, 0.12, 0.05])
DEVICE_TYPES   = np.array(["mobile","web","atm","pos"])
DEVICE_WEIGHTS = np.array([0.55, 0.30, 0.08, 0.07])
COUNTRIES      = np.array(["Vietnam","Singapore","Thailand","Malaysia","Philippines","USA"])
COUNTRY_WEIGHTS= np.array([0.70, 0.10, 0.07, 0.06, 0.04, 0.03])
OTP_FAIL_REASONS = np.array(["wrong_code","expired_otp","too_many_attempts"])
DECLINE_REASONS  = np.array(["insufficient_funds","card_blocked","fraud_suspect",
                              "invalid_card","limit_exceeded"])

PAYMENT_EVENT_TYPES = {"transaction_attempt","transaction_approved","transaction_declined",
                        "card_check","otp_request","otp_failed"}
AMOUNT_EVENT_TYPES  = {"transaction_attempt","transaction_approved"}


def _make_uuids(n: int, rng: np.random.Generator) -> np.ndarray:
    raw = rng.bytes(16 * n)
    out = []
    for i in range(n):
        b = bytearray(raw[i*16:(i+1)*16])
        b[6] = (b[6] & 0x0f) | 0x40
        b[8] = (b[8] & 0x3f) | 0x80
        h = b.hex()
        out.append(f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}")
    return np.array(out)


def _generate_day(
    day_start_ts: int,          # unix seconds
    customer_arr, merchant_arr, card_arr,
    base_events_per_min: int,
    burst_multiplier: int,
    burst_windows: list,
    late_arrival_rate: float,
    late_delay_min: int,
    late_delay_max: int,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """Generate background events for a single day. Returns a DataFrame."""
    MINUTES_PER_DAY = 1440

    # Per-minute counts
    minute_unix = day_start_ts + np.arange(MINUTES_PER_DAY, dtype=np.int64) * 60
    hour_of_day = (minute_unix % 86400) // 3600
    min_of_hour  = (minute_unix % 3600)  // 60

    in_burst = np.zeros(MINUTES_PER_DAY, dtype=bool)
    for w in burst_windows:
        sh, sm = map(int, w["start"].split(":"))
        eh, em = map(int, w["end"].split(":"))
        cur = hour_of_day * 60 + min_of_hour
        in_burst |= (cur >= sh * 60 + sm) & (cur <= eh * 60 + em)

    rates  = np.where(in_burst, base_events_per_min * burst_multiplier, base_events_per_min)
    counts = np.maximum(1, (rates * rng.uniform(0.8, 1.2, size=MINUTES_PER_DAY)).astype(int))
    n = int(counts.sum())

    # Expand to per-event
    min_idx      = np.repeat(np.arange(MINUTES_PER_DAY), counts)
    event_unix   = day_start_ts + min_idx * 60 + rng.integers(0, 60, size=n)

    # Columns
    event_types  = rng.choice(EVENT_TYPES, size=n, p=EVENT_WEIGHTS)
    pay_mask     = np.isin(event_types, list(PAYMENT_EVENT_TYPES))
    amt_mask     = np.isin(event_types, list(AMOUNT_EVENT_TYPES))
    otp_mask     = event_types == "otp_failed"
    dec_mask     = event_types == "transaction_declined"

    merchant_col = np.where(pay_mask, rng.choice(merchant_arr, size=n), None)
    card_col     = np.where(pay_mask, rng.choice(card_arr,     size=n), None)
    amounts      = np.where(amt_mask, np.round(rng.lognormal(13.0, 1.2, size=n), 2), np.nan)

    failure_col  = np.full(n, None, dtype=object)
    if otp_mask.any():
        failure_col[otp_mask] = rng.choice(OTP_FAIL_REASONS, size=otp_mask.sum())
    if dec_mask.any():
        failure_col[dec_mask] = rng.choice(DECLINE_REASONS,  size=dec_mask.sum())

    # Timestamps
    late_mask    = rng.random(size=n) < late_arrival_rate
    lag          = np.where(late_mask,
                            rng.integers(late_delay_min*60, late_delay_max*60, size=n),
                            rng.integers(1, 10, size=n))
    created_unix = event_unix + lag

    event_ids = _make_uuids(n, rng)
    session_nums = rng.integers(1, 9_999_999, size=n)

    df = pd.DataFrame({
        "event_id":        event_ids,
        "event_type":      event_types,
        "event_timestamp": pd.to_datetime(event_unix,   unit="s").strftime("%Y-%m-%dT%H:%M:%S"),
        "created_ts":      pd.to_datetime(created_unix, unit="s").strftime("%Y-%m-%dT%H:%M:%S"),
        "customer_id":     rng.choice(customer_arr, size=n),
        "session_id":      np.char.add("S", np.char.zfill(session_nums.astype(str), 7)),
        "device_type":     rng.choice(DEVICE_TYPES, size=n, p=DEVICE_WEIGHTS),
        "ip_country":      rng.choice(COUNTRIES,    size=n, p=COUNTRY_WEIGHTS),
        "merchant_id":     merchant_col,
        "card_id":         card_col,
        "amount":          amounts,
        "failure_reason":  failure_col,
    })
    df["amount"] = df["amount"].where(df["amount"].notna(), other=None)

    return df, int(late_mask.sum()), int(in_burst[min_idx].sum())


def _generate_transaction_sessions(
    day_txns: pd.DataFrame,
    rng: np.random.Generator,
    late_arrival_rate: float,
    late_delay_min: int,
    late_delay_max: int,
) -> pd.DataFrame:
    """
    Generate event sessions anchored to each transaction.

    Each transaction produces a mini session:
      Normal:  login → otp_request → transaction_attempt → approved/declined
      Fraud:   login → otp_failed (1–3) → transaction_declined probes (1–2)
                     → transaction_attempt → approved

    This directly correlates f_stream_otp_failed_count_30m and
    f_stream_decline_count_30m with fraud labels, matching the ecommerce
    example's approach of linking streaming events to offline records.
    """
    if len(day_txns) == 0:
        return pd.DataFrame()

    # De-dup by transaction_id: gateway retries don't generate extra sessions
    day_txns = day_txns.drop_duplicates(subset="transaction_id", keep="first")
    n = len(day_txns)

    # Pre-extract numpy arrays for vectorised batch generation
    # astype("datetime64[s]") normalises to second resolution before int cast,
    # avoiding the pandas 2→3 breaking change where Python datetimes store as
    # datetime64[us] instead of datetime64[ns] (wrong result with // 10**9).
    txn_unix = pd.to_datetime(day_txns["transaction_timestamp"]).astype("datetime64[s]").astype(np.int64).values
    is_fraud = day_txns["is_fraud"].values.astype(int)
    statuses = day_txns["transaction_status"].values
    cids     = day_txns["customer_id"].values.astype(str)
    kids     = day_txns["card_id"].values.astype(str)
    mids     = day_txns["merchant_id"].values.astype(str)
    amts     = day_txns["amount"].values.astype(float)
    ip_cs    = day_txns["ip_country"].fillna("Vietnam").values.astype(str)

    # One session ID and device per transaction (shared across all its events)
    sids = np.char.add("S", np.char.zfill(rng.integers(1, 9_999_999, n).astype(str), 7))
    devs = rng.choice(DEVICE_TYPES, size=n, p=DEVICE_WEIGHTS)

    all_dfs = []

    # ── 1. login (all transactions, 30–120 min before) ────────────────────────
    login_ts = txn_unix - (rng.uniform(30, 120, n) * 60).astype(np.int64)
    late = rng.random(n) < late_arrival_rate
    lag  = np.where(late, rng.integers(late_delay_min*60, late_delay_max*60, n),
                          rng.integers(1, 10, n))
    all_dfs.append(pd.DataFrame({
        "event_id":        _make_uuids(n, rng),
        "event_type":      "login",
        "event_timestamp": pd.to_datetime(login_ts,        unit="s").strftime("%Y-%m-%dT%H:%M:%S"),
        "created_ts":      pd.to_datetime(login_ts + lag,  unit="s").strftime("%Y-%m-%dT%H:%M:%S"),
        "customer_id": cids, "session_id": sids, "device_type": devs, "ip_country": ip_cs,
        "merchant_id": None, "card_id": None, "amount": None, "failure_reason": None,
    }))

    # ── 2. otp_request (non-fraud transactions, 2–10 min before) ─────────────
    nm = is_fraud == 0
    if nm.any():
        nn = int(nm.sum())
        otp_ts = txn_unix[nm] - (rng.uniform(2, 10, nn) * 60).astype(np.int64)
        late_n = rng.random(nn) < late_arrival_rate
        lag_n  = np.where(late_n, rng.integers(late_delay_min*60, late_delay_max*60, nn),
                                  rng.integers(1, 10, nn))
        all_dfs.append(pd.DataFrame({
            "event_id":        _make_uuids(nn, rng),
            "event_type":      "otp_request",
            "event_timestamp": pd.to_datetime(otp_ts,         unit="s").strftime("%Y-%m-%dT%H:%M:%S"),
            "created_ts":      pd.to_datetime(otp_ts + lag_n, unit="s").strftime("%Y-%m-%dT%H:%M:%S"),
            "customer_id": cids[nm], "session_id": sids[nm],
            "device_type": rng.choice(DEVICE_TYPES, nn, p=DEVICE_WEIGHTS), "ip_country": ip_cs[nm],
            "merchant_id": None, "card_id": None, "amount": None, "failure_reason": None,
        }))

    # ── 3. fraud signals (otp_failed + declined probes) — loop fraud txns ────
    fraud_idxs = np.where(is_fraud == 1)[0]
    if len(fraud_idxs) > 0:
        fraud_rows = []
        for i in fraud_idxs:
            t   = int(txn_unix[i])
            cid, kid, mid = cids[i], kids[i], mids[i]
            sid, dev, ip_c = sids[i], devs[i], ip_cs[i]

            # otp_failed: 1–3 events, 5–30 min before the transaction
            for _ in range(int(rng.integers(1, 4))):
                fraud_rows.append({
                    "event_type": "otp_failed",
                    "ts": t - int(rng.uniform(5, 30) * 60),
                    "cid": cid, "sid": sid, "dev": dev, "ip": ip_c,
                    "mid": None, "kid": None,
                    "failure_reason": str(rng.choice(OTP_FAIL_REASONS)),
                })

            # transaction_declined probe: 1–2 attempts, 10–30 min before
            for _ in range(int(rng.integers(1, 3))):
                fraud_rows.append({
                    "event_type": "transaction_declined",
                    "ts": t - int(rng.uniform(10, 30) * 60),
                    "cid": cid, "sid": sid, "dev": dev, "ip": ip_c,
                    "mid": mid, "kid": kid,
                    "failure_reason": str(rng.choice(DECLINE_REASONS)),
                })

        if fraud_rows:
            nf    = len(fraud_rows)
            fr_ts = np.array([r["ts"] for r in fraud_rows], dtype=np.int64)
            late_f = rng.random(nf) < late_arrival_rate
            lag_f  = np.where(late_f, rng.integers(late_delay_min*60, late_delay_max*60, nf),
                                      rng.integers(1, 10, nf))
            all_dfs.append(pd.DataFrame({
                "event_id":        _make_uuids(nf, rng),
                "event_type":      [r["event_type"]    for r in fraud_rows],
                "event_timestamp": pd.to_datetime(fr_ts,         unit="s").strftime("%Y-%m-%dT%H:%M:%S"),
                "created_ts":      pd.to_datetime(fr_ts + lag_f, unit="s").strftime("%Y-%m-%dT%H:%M:%S"),
                "customer_id":     [r["cid"]           for r in fraud_rows],
                "session_id":      [r["sid"]           for r in fraud_rows],
                "device_type":     [r["dev"]           for r in fraud_rows],
                "ip_country":      [r["ip"]            for r in fraud_rows],
                "merchant_id":     [r["mid"]           for r in fraud_rows],
                "card_id":         [r["kid"]           for r in fraud_rows],
                "amount":          None,
                "failure_reason":  [r["failure_reason"] for r in fraud_rows],
            }))

    # ── 4. transaction_attempt (all transactions, 1–5 min before) ────────────
    att_ts = txn_unix - rng.integers(60, 300, n)
    late_a = rng.random(n) < late_arrival_rate
    lag_a  = np.where(late_a, rng.integers(late_delay_min*60, late_delay_max*60, n),
                              rng.integers(1, 10, n))
    all_dfs.append(pd.DataFrame({
        "event_id":        _make_uuids(n, rng),
        "event_type":      "transaction_attempt",
        "event_timestamp": pd.to_datetime(att_ts,         unit="s").strftime("%Y-%m-%dT%H:%M:%S"),
        "created_ts":      pd.to_datetime(att_ts + lag_a, unit="s").strftime("%Y-%m-%dT%H:%M:%S"),
        "customer_id": cids, "session_id": sids, "device_type": devs, "ip_country": ip_cs,
        "merchant_id": mids, "card_id": kids, "amount": amts, "failure_reason": None,
    }))

    # ── 5. result event (transaction_approved or transaction_declined) ────────
    result_types  = np.where(
        np.isin(statuses, ["approved", "pending", "reversed"]),
        "transaction_approved", "transaction_declined",
    )
    approved_mask = result_types == "transaction_approved"
    result_amts   = np.where(approved_mask, amts, np.nan)
    fail_r        = np.full(n, None, dtype=object)
    if (~approved_mask).any():
        fail_r[~approved_mask] = rng.choice(DECLINE_REASONS, (~approved_mask).sum())
    late_r = rng.random(n) < late_arrival_rate
    lag_r  = np.where(late_r, rng.integers(late_delay_min*60, late_delay_max*60, n),
                              rng.integers(1, 10, n))
    df_res = pd.DataFrame({
        "event_id":        _make_uuids(n, rng),
        "event_type":      result_types,
        "event_timestamp": pd.to_datetime(txn_unix,          unit="s").strftime("%Y-%m-%dT%H:%M:%S"),
        "created_ts":      pd.to_datetime(txn_unix + lag_r,  unit="s").strftime("%Y-%m-%dT%H:%M:%S"),
        "customer_id": cids, "session_id": sids, "device_type": devs, "ip_country": ip_cs,
        "merchant_id": mids, "card_id": kids,
        "amount": result_amts, "failure_reason": fail_r,
    })
    df_res["amount"] = df_res["amount"].where(df_res["amount"].notna(), other=None)
    all_dfs.append(df_res)

    out = pd.concat(all_dfs, ignore_index=True)
    out["amount"] = out["amount"].where(out["amount"].notna(), other=None)
    return out


def generate_streaming_events(
    customer_ids, merchant_ids, card_ids, days_history,
    base_events_per_min, burst_multiplier, burst_windows,
    late_arrival_rate, late_delay_min, late_delay_max,
    duplicate_rate, output_path, rng,
    transaction_df=None,
) -> dict:
    """
    Generate streaming events day-by-day.

    If transaction_df is provided (recommended), transaction-anchored sessions are
    injected alongside the background stream, correlating streaming features with
    the fraud labels in the offline transaction table.

    Peak RAM = ~1 day of events instead of full dataset (chunk-based design).
    """
    customer_arr = np.array(customer_ids)
    merchant_arr = np.array(merchant_ids)
    card_arr     = np.array(card_ids)

    end_date   = datetime(2025, 10, 1)
    start_date = end_date - timedelta(days=days_history)

    # Pre-group transactions by date for O(1) daily lookups
    txn_by_date = {}
    if transaction_df is not None:
        for date_val, grp in transaction_df.groupby("transaction_date"):
            txn_by_date[date_val] = grp
        print(f"  [streaming] Transaction sessions enabled: {len(txn_by_date)} days, "
              f"{len(transaction_df):,} transactions")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    total_events        = 0
    total_late          = 0
    total_burst         = 0
    total_session_events = 0
    dupe_buffer         = []

    print(f"  [streaming] Generating {days_history} days chunk-by-chunk (1 chunk = 1 day)...")

    with open(output_path, "w") as f:
        for day_offset in tqdm(range(days_history), desc="Generating events", unit="day"):
            day_dt       = start_date + timedelta(days=day_offset)
            day_start_ts = int(day_dt.timestamp())

            # Background events
            df_day, n_late, n_burst = _generate_day(
                day_start_ts=day_start_ts,
                customer_arr=customer_arr,
                merchant_arr=merchant_arr,
                card_arr=card_arr,
                base_events_per_min=base_events_per_min,
                burst_multiplier=burst_multiplier,
                burst_windows=burst_windows,
                late_arrival_rate=late_arrival_rate,
                late_delay_min=late_delay_min,
                late_delay_max=late_delay_max,
                rng=rng,
            )

            # Transaction-anchored sessions
            day_date = day_dt.date()
            if day_date in txn_by_date:
                df_sessions = _generate_transaction_sessions(
                    txn_by_date[day_date], rng,
                    late_arrival_rate, late_delay_min, late_delay_max,
                )
                if len(df_sessions) > 0:
                    df_day = pd.concat([df_day, df_sessions], ignore_index=True)
                    total_session_events += len(df_sessions)

            total_events += len(df_day)
            total_late   += n_late
            total_burst  += n_burst

            # Collect ~duplicate_rate rows for later replay (reservoir sample)
            n_to_sample = max(0, int(len(df_day) * duplicate_rate))
            if n_to_sample > 0:
                sample_idx = rng.choice(len(df_day), size=n_to_sample, replace=False)
                dupe_buffer.append(df_day.iloc[sample_idx].copy())

            # Write day chunk
            records = df_day.to_dict(orient="records")
            f.write("\n".join(json.dumps(r, default=str) for r in records) + "\n")

        # Write duplicates at the end (out-of-order, realistic retry)
        if dupe_buffer:
            dupes_df = pd.concat(dupe_buffer, ignore_index=True)
            n_dupes  = len(dupes_df)
            retry_delays = rng.integers(60, 180, size=n_dupes)
            orig_unix    = pd.to_datetime(dupes_df["created_ts"]).astype("datetime64[s]").astype(np.int64)
            dupes_df["created_ts"] = pd.to_datetime(
                orig_unix.values + retry_delays, unit="s"
            ).strftime("%Y-%m-%dT%H:%M:%S")

            records = dupes_df.to_dict(orient="records")
            f.write("\n".join(json.dumps(r, default=str) for r in records) + "\n")
        else:
            n_dupes = 0

    total_with_dupes = total_events + n_dupes

    summary = {
        "total_events":              total_with_dupes,
        "base_events":               total_events,
        "session_events":            total_session_events,
        "duplicate_events":          n_dupes,
        "late_events":               total_late,
        "burst_events":              total_burst,
        "late_arrival_rate_actual":  round(total_late / max(total_events, 1), 4),
        "duplicate_rate_actual":     round(n_dupes    / max(total_events, 1), 4),
        "burst_event_share":         round(total_burst / max(total_events, 1), 4),
    }

    print(f"  [streaming] Done. Total: {total_with_dupes:,} "
          f"(background: {total_events - total_session_events:,}, "
          f"sessions: {total_session_events:,}, "
          f"dupes: {n_dupes:,}, late: {total_late:,})")
    return summary
