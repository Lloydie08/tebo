import os
import httpx
import traceback
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip().rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "").strip()
REST_URL     = f"{SUPABASE_URL}/rest/v1/cookie_sessions"
RPC_URL      = f"{SUPABASE_URL}/rest/v1/rpc/reset_id_sequence"

def get_headers() -> dict:
    return {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "return=representation",
    }

# ── Reset broken sequence (called auto on 409) ────────────────────────────────

def reset_sequence():
    try:
        r = httpx.post(RPC_URL, headers=get_headers(), json={}, timeout=10)
        print(f"[DB] Sequence reset → {r.status_code}")
    except Exception as e:
        print(f"[DB] Sequence reset error: {e}")

# ── Single INSERT — all columns at once, auto-retry on 409 ───────────────────

def save_cookie(cookies_list: list, is_premium: bool, result: dict) -> tuple[int | None, str | None]:
    payload = {
        "cookies":          cookies_list,
        "is_premium":       is_premium,
        "status":           result.get("status")           or "Valid",
        "description":      result.get("description")      or "",
        "email":            result.get("email"),
        "email_verified":   result.get("email_verified"),
        "plan":             result.get("plan"),
        "price":            result.get("price"),
        "country":          result.get("country"),
        "member_since":     result.get("member_since"),
        "payment_method":   result.get("payment_method"),
        "phone":            result.get("phone"),
        "phone_verified":   result.get("phone_verified"),
        "video_quality":    result.get("video_quality"),
        "max_streams":      result.get("max_streams"),
        "payment_hold":     result.get("payment_hold"),
        "extra_member":     result.get("extra_member"),
        "profiles":         result.get("profiles"),
        "billing":          result.get("billing"),
        "premium_detected": result.get("premium_detected"),
        "watch_link":       result.get("watch_link"),
    }
    payload = {k: v for k, v in payload.items() if v is not None}

    def _do_insert():
        return httpx.post(REST_URL, headers=get_headers(), json=payload, timeout=15)

    try:
        r = _do_insert()
        print(f"[DB] Insert → {r.status_code}")

        if r.status_code == 409:
            print("[DB] 409 — resetting sequence and retrying...")
            reset_sequence()
            r = _do_insert()
            print(f"[DB] Retry → {r.status_code}")

        if not r.is_success:
            print(f"[DB] Insert error: {r.text[:300]}")
            return None, f"HTTP {r.status_code}: {r.text[:200]}"

        data = r.json()
        row_id = data[0]["id"] if data else None
        print(f"[DB] Inserted ID: {row_id}")
        return row_id, None

    except httpx.TimeoutException:
        return None, "Request timed out"
    except Exception as e:
        traceback.print_exc()
        return None, str(e)

# ── Update existing row (Check All refresh) ───────────────────────────────────

def update_cookie_result(row_id: int, result: dict):
    try:
        update_data = {
            k: v for k, v in result.items()
            if k not in ("valid", "error", "country_code") and v is not None
        }
        r = httpx.patch(
            REST_URL,
            headers={**get_headers(), "Prefer": ""},
            params={"id": f"eq.{row_id}"},
            json=update_data,
            timeout=15,
        )
        print(f"[DB] Update row {row_id} → {r.status_code}")
        if not r.is_success:
            print(f"[DB] Update error: {r.text[:200]}")
    except Exception as e:
        print(f"[DB] Update error: {e}")
        traceback.print_exc()

# ── Delete single row ─────────────────────────────────────────────────────────

def delete_row(row_id: int) -> bool:
    try:
        r = httpx.delete(
            REST_URL,
            headers={**get_headers(), "Prefer": ""},
            params={"id": f"eq.{row_id}"},
            timeout=15,
        )
        r.raise_for_status()
        print(f"[DB] Deleted row {row_id}")
        return True
    except Exception as e:
        print(f"[DB] Delete error: {e}")
        traceback.print_exc()
        return False

# ── Bulk delete rows by IDs ───────────────────────────────────────────────────

def delete_rows(ids: list[int]) -> int:
    if not ids:
        return 0
    try:
        ids_str = ",".join(str(i) for i in ids)
        r = httpx.delete(
            REST_URL,
            headers={**get_headers(), "Prefer": ""},
            params={"id": f"in.({ids_str})"},
            timeout=20,
        )
        r.raise_for_status()
        print(f"[DB] Bulk deleted {len(ids)} rows")
        return len(ids)
    except Exception as e:
        print(f"[DB] Bulk delete error: {e}")
        traceback.print_exc()
        return 0

# ── Fetch all for checker ─────────────────────────────────────────────────────

def get_all_cookies() -> list:
    try:
        r = httpx.get(
            REST_URL,
            headers={**get_headers(), "Prefer": ""},
            params={"select": "id,cookies,is_premium"},
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
        print(f"[DB] get_all_cookies → {len(data)} rows")
        return data
    except Exception as e:
        print(f"[DB] Fetch error: {e}")
        traceback.print_exc()
        return []

# ── Fetch sorted: FREE first, PREMIUM last, display_id 1..N ──────────────────

def get_sorted_cookies() -> list:
    try:
        r = httpx.get(
            REST_URL,
            headers={**get_headers(), "Prefer": ""},
            params={"select": "*", "order": "is_premium.asc,id.asc"},
            timeout=15,
        )
        r.raise_for_status()
        rows = r.json()
        for idx, row in enumerate(rows, start=1):
            row["display_id"] = idx
        print(f"[DB] get_sorted_cookies → {len(rows)} rows")
        return rows
    except Exception as e:
        print(f"[DB] Fetch sorted error: {e}")
        traceback.print_exc()
        return []

# ── Counts ────────────────────────────────────────────────────────────────────

def get_row_count() -> int:
    try:
        r = httpx.get(
            REST_URL,
            headers={**get_headers(), "Prefer": "count=exact"},
            params={"select": "id"},
            timeout=15,
        )
        r.raise_for_status()
        content_range = r.headers.get("content-range", "")
        count = int(content_range.split("/")[-1]) if "/" in content_range else len(r.json())
        print(f"[DB] Row count: {count}")
        return count
    except Exception as e:
        print(f"[DB] Count error: {e}")
        traceback.print_exc()
        return -1

def get_free_count() -> int:
    try:
        r = httpx.get(
            REST_URL,
            headers={**get_headers(), "Prefer": "count=exact"},
            params={"select": "id", "is_premium": "eq.false"},
            timeout=15,
        )
        r.raise_for_status()
        content_range = r.headers.get("content-range", "")
        return int(content_range.split("/")[-1]) if "/" in content_range else len(r.json())
    except Exception:
        return 0

def get_sample_rows(limit: int = 5) -> list:
    try:
        r = httpx.get(
            REST_URL,
            headers={**get_headers(), "Prefer": ""},
            params={"select": "id,status,is_premium,description", "limit": str(limit)},
            timeout=15,
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[DB] Sample error: {e}")
        return []

# ── Duplicate check ───────────────────────────────────────────────────────────

def check_email_exists(email: str) -> int | None:
    try:
        if not email:
            return None
        r = httpx.get(
            REST_URL,
            headers={**get_headers(), "Prefer": ""},
            params={"select": "id,email", "email": f"eq.{email.strip().lower()}", "limit": "1"},
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
        if data:
            print(f"[DB] Duplicate email: {email} → ID {data[0]['id']}")
            return data[0]["id"]
        return None
    except Exception as e:
        print(f"[DB] Email check error: {e}")
        traceback.print_exc()
        return None

# ── Remove duplicate emails ───────────────────────────────────────────────────

def remove_duplicate_emails() -> int:
    try:
        r = httpx.get(
            REST_URL,
            headers={**get_headers(), "Prefer": ""},
            params={"select": "id,email", "email": "not.is.null", "order": "id.asc"},
            timeout=15,
        )
        r.raise_for_status()
        rows = r.json()

        email_groups: dict[str, list[int]] = defaultdict(list)
        for row in rows:
            raw_email = (row.get("email") or "").strip().lower()
            if raw_email:
                email_groups[raw_email].append(row["id"])

        to_delete = []
        for email, ids in email_groups.items():
            if len(ids) > 1:
                to_delete.extend(ids[1:])

        return delete_rows(to_delete)
    except Exception as e:
        print(f"[DB] Dedupe error: {e}")
        traceback.print_exc()
        return 0
