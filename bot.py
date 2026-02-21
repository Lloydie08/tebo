import os
import json
import csv
import io
import asyncio
import logging
import traceback
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, ConversationHandler, filters, ContextTypes,
)
from playwright.async_api import async_playwright
from checker  import validate_netflix_cookies, mask_email, parse_cookies_from_text
from database import (
    save_cookie, update_cookie_result,
    get_all_cookies, get_sorted_cookies,
    get_row_count, get_free_count, get_sample_rows,
    check_email_exists, delete_row, delete_rows,
    remove_duplicate_emails,
)

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()

PASTE_COOKIES = 1
BULK_TYPE     = 2

OUTPUT_FIELDS = [
    'id', 'cookies', 'description', 'is_premium',
    'status', 'email', 'email_verified',
    'plan', 'price', 'country', 'member_since',
    'payment_method', 'phone', 'phone_verified',
    'video_quality', 'max_streams', 'payment_hold',
    'extra_member', 'profiles', 'billing',
    'premium_detected', 'watch_link',
]

# ── Keyboards ──────────────────────────────────────────────────────────────────

def main_menu_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🆓 Add Free Cookie",    callback_data="add_free"),
            InlineKeyboardButton("💎 Add Premium Cookie", callback_data="add_premium"),
        ],
        [
            InlineKeyboardButton("📦 Bulk Import TXT",    callback_data="bulk_import"),
            InlineKeyboardButton("🔍 Check All Cookies",  callback_data="check_all"),
        ],
        [
            InlineKeyboardButton("📥 Export CSV",         callback_data="export_csv"),
            InlineKeyboardButton("🧹 Remove Duplicates",  callback_data="dedupe"),
        ],
    ])

def bulk_type_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🆓 Free",    callback_data="bulk_free"),
            InlineKeyboardButton("💎 Premium", callback_data="bulk_premium"),
        ],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_bulk")],
    ])

# ── /start ─────────────────────────────────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    total         = get_row_count()
    free_count    = get_free_count()
    premium_count = total - free_count

    await update.message.reply_text(
        f"🎬 *Netflix Cookie Manager*\n\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🆓 Free: `{free_count}` | 💎 Premium: `{premium_count}`\n"
        f"📋 Total: `{total}`\n"
        f"━━━━━━━━━━━━━━━━━━\n\n"
        f"What would you like to do?",
        reply_markup=main_menu_keyboard(),
        parse_mode="Markdown",
    )
    return ConversationHandler.END

# ── /debug ─────────────────────────────────────────────────────────────────────

async def debug(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        count = get_row_count()
        rows  = get_sample_rows(5)
        if count == -1:
            await update.message.reply_text(
                "❌ *DB Connection Failed*\n\nCheck Railway Variables. Run /env.",
                parse_mode="Markdown",
            )
            return
        lines = ["🛠 *Debug Info*\n", "✅ DB Connected", f"📋 Total rows: `{count}`\n"]
        if rows:
            lines.append("🔍 *Sample rows:*")
            for r in rows:
                label = "💎" if r.get("is_premium") else "🆓"
                lines.append(
                    f"  ID `{r['id']}` {label} — "
                    f"`{r.get('status','?')}` — "
                    f"{r.get('description','')[:40]}"
                )
        else:
            lines.append("⚠️ No rows found.")
        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"❌ *Debug Error:*\n`{e}`", parse_mode="Markdown")

# ── /env ───────────────────────────────────────────────────────────────────────

async def env_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    url   = os.getenv("SUPABASE_URL",        "❌ NOT SET")
    key   = os.getenv("SUPABASE_SERVICE_KEY", "❌ NOT SET")
    token = os.getenv("TELEGRAM_BOT_TOKEN",   "❌ NOT SET")
    await update.message.reply_text(
        f"🔧 *Environment Variables*\n\n"
        f"SUPABASE\\_URL: `{url[:35] + '...' if url != '❌ NOT SET' else '❌ NOT SET'}`\n"
        f"SUPABASE\\_SERVICE\\_KEY: `{'✅ SET' if key != '❌ NOT SET' else '❌ NOT SET'}`\n"
        f"TELEGRAM\\_BOT\\_TOKEN: `{'✅ SET' if token != '❌ NOT SET' else '❌ NOT SET'}`",
        parse_mode="Markdown",
    )

# ── Button Handler ─────────────────────────────────────────────────────────────

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data in ("add_free", "add_premium"):
        context.user_data["is_premium"] = (query.data == "add_premium")
        label = "💎 Premium" if context.user_data["is_premium"] else "🆓 Free"
        await query.edit_message_text(
            f"Selected: *{label}*\n\n"
            f"Paste your cookies in JSON array format:\n\n"
            f"`[{{\"name\": \"NetflixId\", \"value\": \"...\"}}]`\n\n"
            f"Send /cancel to go back.",
            parse_mode="Markdown",
        )
        return PASTE_COOKIES

    elif query.data == "bulk_import":
        await query.edit_message_text(
            "📦 *Bulk Import*\n\nSelect account type for this batch:",
            reply_markup=bulk_type_keyboard(),
            parse_mode="Markdown",
        )
        return BULK_TYPE

    elif query.data in ("bulk_free", "bulk_premium"):
        context.user_data["bulk_is_premium"] = (query.data == "bulk_premium")
        label = "💎 Premium" if context.user_data["bulk_is_premium"] else "🆓 Free"
        await query.edit_message_text(
            f"📦 *Bulk Import — {label}*\n\n"
            f"Send a `.txt` file containing one or more cookie arrays.\n\n"
            f"*Supported formats:*\n"
            f"• One JSON array per line\n"
            f"• Multiple arrays separated by blank lines\n"
            f"• Array of arrays `[[...],[...]]`\n"
            f"• Netscape tab-separated format\n\n"
            f"Send /cancel to go back.",
            parse_mode="Markdown",
        )
        return BULK_TYPE

    elif query.data == "cancel_bulk":
        await query.edit_message_text("❌ Cancelled.", reply_markup=main_menu_keyboard())
        return ConversationHandler.END

    elif query.data == "check_all":
        await query.edit_message_text("🔄 Starting validation... please wait.")
        await run_check_all(query.message, context)

    elif query.data == "export_csv":
        await query.edit_message_text("📥 Generating CSV...")
        await export_csv(query.message, context)

    elif query.data == "dedupe":
        await query.edit_message_text("🧹 Scanning for duplicate emails...")
        await run_dedupe(query.message, context)

    return ConversationHandler.END

# ── Receive Single Cookie ──────────────────────────────────────────────────────

async def receive_cookies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text       = update.message.text.strip()
    is_premium = context.user_data.get("is_premium", False)

    try:
        cookies_list = json.loads(text)
        if not isinstance(cookies_list, list) or len(cookies_list) == 0:
            raise ValueError("Must be a non-empty JSON array")
    except Exception as e:
        await update.message.reply_text(
            f"❌ *Invalid JSON format*\n`{e}`\n\nTry again or /cancel",
            parse_mode="Markdown",
        )
        return PASTE_COOKIES

    await _check_then_save(update.message, cookies_list, is_premium)
    return ConversationHandler.END

# ── Core: Check → Dedupe → Save ───────────────────────────────────────────────

async def _check_then_save(message, cookies_list: list, is_premium: bool):
    label = "💎 Premium" if is_premium else "🆓 Free"

    # Step 1: Validate
    msg = await message.reply_text(
        f"🔍 *Step 1/3 — Validating Cookie*\n\n⏳ Connecting to Netflix...",
        parse_mode="Markdown",
    )
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            result  = await validate_netflix_cookies(browser, cookies_list)
            await browser.close()
    except Exception as e:
        await msg.edit_text(
            f"❌ *Checker Error*\n\n`{e}`\n\nCookie not saved.",
            parse_mode="Markdown",
            reply_markup=main_menu_keyboard(),
        )
        return

    if not result["valid"]:
        await msg.edit_text(
            f"❌ *Cookie is Dead*\n\n"
            f"Reason: `{result.get('error', 'Redirected to login')}`\n\n"
            f"Cookie was *not saved* to the database.",
            parse_mode="Markdown",
            reply_markup=main_menu_keyboard(),
        )
        return

    # Step 2: Duplicate check
    masked_email = mask_email(result.get("email") or "N/A")
    await msg.edit_text(
        f"✅ *Step 1/3 — Cookie is Valid!*\n\n"
        f"📧 Email: `{masked_email}`\n"
        f"🌍 {result.get('description', 'N/A')}\n"
        f"💳 Plan: `{result.get('plan', 'N/A')}`\n\n"
        f"🔍 *Step 2/3 — Checking for duplicates...*",
        parse_mode="Markdown",
    )

    email = (result.get("email") or "").strip().lower()
    if email:
        existing_id = check_email_exists(email)
        if existing_id:
            await msg.edit_text(
                f"⚠️ *Step 2/3 — Duplicate Detected*\n\n"
                f"📧 `{masked_email}` already exists\n"
                f"🆔 Existing ID: `{existing_id}`\n\n"
                f"Cookie was *not saved*.",
                parse_mode="Markdown",
                reply_markup=main_menu_keyboard(),
            )
            return

    # Step 3: Save
    await msg.edit_text(
        f"✅ *Step 2/3 — No Duplicate Found*\n\n"
        f"💾 *Step 3/3 — Saving to database...*",
        parse_mode="Markdown",
    )

    row_id, db_error = save_cookie(cookies_list, is_premium, result)

    if not row_id:
        await msg.edit_text(
            f"❌ *Database Save Failed*\n\n"
            f"Error: `{db_error}`\n\n"
            f"Run /debug or /env to diagnose.",
            parse_mode="Markdown",
        )
        return

    free_count    = get_free_count()
    total         = get_row_count()
    premium_count = total - free_count
    display_id    = free_count if not is_premium else total

    await msg.edit_text(
        f"✅ *Cookie Saved Successfully!*\n\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🔢 Position: `#{display_id}` | {label}\n"
        f"📧 Email: `{masked_email}`\n"
        f"🌍 {result.get('description', 'N/A')}\n"
        f"💳 Plan: `{result.get('plan', 'N/A')}`\n"
        f"💰 Price: `{result.get('price', 'N/A')}`\n"
        f"📺 Quality: `{result.get('video_quality', 'N/A')}`\n"
        f"👥 Streams: `{result.get('max_streams', 'N/A')}`\n"
        f"📅 Billing: `{result.get('billing', 'N/A')}`\n"
        f"📊 Status: `{result.get('status', 'Valid')}`\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🆓 Free: `{free_count}` | 💎 Premium: `{premium_count}`\n"
        f"📋 Total in DB: `{total}`",
        parse_mode="Markdown",
        reply_markup=main_menu_keyboard(),
    )

# ── /cancel ────────────────────────────────────────────────────────────────────

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ Cancelled.", reply_markup=main_menu_keyboard())
    return ConversationHandler.END

# ── Bulk TXT Import ────────────────────────────────────────────────────────────

async def receive_bulk_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    doc = update.message.document
    if not doc or not doc.file_name.lower().endswith(".txt"):
        await update.message.reply_text("❌ Please send a `.txt` file.")
        return BULK_TYPE

    is_premium = context.user_data.get("bulk_is_premium", False)
    label      = "💎 Premium" if is_premium else "🆓 Free"
    msg        = await update.message.reply_text("📖 Reading file...")

    try:
        tg_file = await doc.get_file()
        raw     = await tg_file.download_as_bytearray()
        text    = raw.decode("utf-8", errors="ignore")
    except Exception as e:
        await msg.edit_text(f"❌ Failed to read file:\n`{e}`", parse_mode="Markdown")
        return ConversationHandler.END

    cookie_sets = parse_cookies_from_text(text)
    if not cookie_sets:
        await msg.edit_text(
            "❌ *No valid cookie arrays found.*\n\n"
            "Make sure the file contains JSON arrays `[{...}]`.",
            parse_mode="Markdown",
        )
        return ConversationHandler.END

    total    = len(cookie_sets)
    counters = {"saved": 0, "dead": 0, "duplicate": 0, "errors": 0, "done": 0}

    await msg.edit_text(
        f"📦 *Bulk Import — {label}*\n\n"
        f"📂 Found *{total}* cookie sets\n"
        f"⏳ Validating & saving... 0/{total}",
        parse_mode="Markdown",
    )

    async with async_playwright() as p:
        browser   = await p.chromium.launch(headless=True)
        semaphore = asyncio.Semaphore(3)
        lock      = asyncio.Lock()

        async def process_set(idx: int, cookies_list: list):
            async with semaphore:
                try:
                    result = await validate_netflix_cookies(browser, cookies_list)

                    async with lock:
                        counters["done"] += 1
                        done = counters["done"]

                    if not result["valid"]:
                        async with lock:
                            counters["dead"] += 1
                    else:
                        email  = (result.get("email") or "").strip().lower()
                        dup_id = check_email_exists(email) if email else None

                        if dup_id:
                            async with lock:
                                counters["duplicate"] += 1
                        else:
                            row_id, db_error = save_cookie(cookies_list, is_premium, result)
                            if row_id:
                                async with lock:
                                    counters["saved"] += 1
                            else:
                                print(f"[BULK] DB error on set {idx}: {db_error}")
                                async with lock:
                                    counters["errors"] += 1

                    if done % 3 == 0 or done == total:
                        try:
                            async with lock:
                                s, d, dup, err = (
                                    counters["saved"], counters["dead"],
                                    counters["duplicate"], counters["errors"],
                                )
                            await msg.edit_text(
                                f"📦 *Bulk Import — {label}*\n\n"
                                f"⏳ Progress: {done}/{total}\n\n"
                                f"✅ Saved: {s}\n"
                                f"❌ Dead: {d}\n"
                                f"⚠️ Duplicate: {dup}\n"
                                f"💥 Errors: {err}",
                                parse_mode="Markdown",
                            )
                        except Exception:
                            pass

                except Exception as e:
                    print(f"[BULK] Exception on set {idx}: {e}")
                    traceback.print_exc()
                    async with lock:
                        counters["errors"] += 1
                        counters["done"]   += 1

        await asyncio.gather(*(process_set(i, cs) for i, cs in enumerate(cookie_sets)))
        await browser.close()

    free_count    = get_free_count()
    total_db      = get_row_count()
    premium_count = total_db - free_count

    await msg.edit_text(
        f"✅ *Bulk Import Complete!*\n\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📂 Processed: {total}\n"
        f"✅ Saved: {counters['saved']}\n"
        f"❌ Dead: {counters['dead']}\n"
        f"⚠️ Duplicates Skipped: {counters['duplicate']}\n"
        f"💥 Errors: {counters['errors']}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🆓 Free: `{free_count}` | 💎 Premium: `{premium_count}`\n"
        f"📋 Total in DB: `{total_db}`",
        parse_mode="Markdown",
        reply_markup=main_menu_keyboard(),
    )
    return ConversationHandler.END

# ── Check All — validate, delete dead, renumber ───────────────────────────────

async def run_check_all(message, context: ContextTypes.DEFAULT_TYPE):
    rows = get_all_cookies()
    if not rows:
        await message.reply_text(
            f"⚠️ *No cookies found.*\n\nDB row count: `{get_row_count()}`\nRun /debug.",
            parse_mode="Markdown",
            reply_markup=main_menu_keyboard(),
        )
        return

    total    = len(rows)
    live     = 0
    dead     = 0
    checked  = 0
    dead_ids = []

    status_msg = await message.reply_text(
        f"🔄 *Checking All Cookies*\n\n"
        f"📋 Total: {total}\n\n"
        f"✅ Live: 0 | ❌ Dead: 0 | ⏳ Remaining: {total}",
        parse_mode="Markdown",
    )

    async with async_playwright() as p:
        browser   = await p.chromium.launch(headless=True)
        semaphore = asyncio.Semaphore(3)

        async def check_row(row):
            nonlocal live, dead, checked
            async with semaphore:
                row_id       = row["id"]
                cookies_data = row["cookies"]

                if isinstance(cookies_data, str):
                    try:
                        cookies_data = json.loads(cookies_data)
                    except Exception:
                        dead_ids.append(row_id)
                        dead    += 1
                        checked += 1
                        return

                result = await validate_netflix_cookies(browser, cookies_data)

                if result["valid"]:
                    live += 1
                    update_cookie_result(row_id, result)
                else:
                    dead += 1
                    dead_ids.append(row_id)

                checked += 1
                if checked % 5 == 0 or checked == total:
                    try:
                        await status_msg.edit_text(
                            f"🔄 *Checking All Cookies*\n\n"
                            f"📋 Total: {total}\n\n"
                            f"✅ Live: {live} | ❌ Dead: {dead} | "
                            f"⏳ Remaining: {total - checked}",
                            parse_mode="Markdown",
                        )
                    except Exception:
                        pass

        await asyncio.gather(*(check_row(row) for row in rows))
        await browser.close()

    # Delete all dead rows in one call
    await status_msg.edit_text(
        f"🗑 *Deleting {len(dead_ids)} dead cookies...*",
        parse_mode="Markdown",
    )
    removed = delete_rows(dead_ids)

    # Reload sorted list — display_id is reassigned 1..N automatically
    sorted_rows   = get_sorted_cookies()
    total_after   = len(sorted_rows)
    free_count    = sum(1 for r in sorted_rows if not r.get("is_premium"))
    premium_count = total_after - free_count

    await status_msg.edit_text(
        f"✅ *Check All Complete!*\n\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📊 Checked: {total}\n"
        f"✅ Live kept: {live}\n"
        f"❌ Dead deleted: {removed}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🆓 Free (IDs 1–{free_count}): `{free_count}`\n"
        f"💎 Premium (IDs {free_count + 1}–{total_after}): `{premium_count}`\n"
        f"📋 Total remaining: `{total_after}`\n\n"
        f"_IDs renumbered 1…{total_after} — Free first, Premium last_",
        parse_mode="Markdown",
        reply_markup=main_menu_keyboard(),
    )

# ── Remove Duplicates ──────────────────────────────────────────────────────────

async def run_dedupe(message, context: ContextTypes.DEFAULT_TYPE):
    removed       = remove_duplicate_emails()
    free_count    = get_free_count()
    total         = get_row_count()
    premium_count = total - free_count

    if removed == 0:
        await message.reply_text(
            f"✅ *No duplicates found!*\n\n"
            f"🆓 Free: `{free_count}` | 💎 Premium: `{premium_count}`\n"
            f"📋 Total: `{total}`",
            parse_mode="Markdown",
            reply_markup=main_menu_keyboard(),
        )
    else:
        await message.reply_text(
            f"🧹 *Duplicates Removed!*\n\n"
            f"🗑 Removed: `{removed}` rows\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🆓 Free: `{free_count}` | 💎 Premium: `{premium_count}`\n"
            f"📋 Remaining: `{total}`",
            parse_mode="Markdown",
            reply_markup=main_menu_keyboard(),
        )

# ── Export CSV ─────────────────────────────────────────────────────────────────

async def export_csv(message, context: ContextTypes.DEFAULT_TYPE):
    rows = get_sorted_cookies()  # free first, premium last, display_id 1..N
    if not rows:
        await message.reply_text(
            "⚠️ *No cookies to export.*",
            parse_mode="Markdown",
            reply_markup=main_menu_keyboard(),
        )
        return

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=OUTPUT_FIELDS, extrasaction='ignore')
    writer.writeheader()

    for row in rows:
        row_out = {k: row.get(k, '') for k in OUTPUT_FIELDS}
        row_out['id'] = row['display_id']  # sequential: 1,2,3...
        if isinstance(row_out.get('cookies'), (list, dict)):
            row_out['cookies'] = json.dumps(row_out['cookies'])
        writer.writerow(row_out)

    output.seek(0)
    bio      = io.BytesIO(output.getvalue().encode('utf-8'))
    bio.name = "netflix_cookies.csv"

    free_count    = sum(1 for r in rows if not r.get("is_premium"))
    premium_count = sum(1 for r in rows if r.get("is_premium"))
    live_count    = sum(1 for r in rows if r.get("status") == "Valid")
    dead_count    = sum(1 for r in rows if r.get("status") == "Dead")

    await message.reply_document(
        document=bio,
        filename="netflix_cookies.csv",
        caption=(
            f"📥 *Netflix Cookies Export*\n\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🆓 Free (IDs 1–{free_count}): `{free_count}`\n"
            f"💎 Premium (IDs {free_count + 1}–{len(rows)}): `{premium_count}`\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"✅ Live: {live_count} | ❌ Dead: {dead_count}\n"
            f"📋 Total: {len(rows)} rows\n\n"
            f"_Sorted: Free first → Premium last_"
        ),
        parse_mode="Markdown",
    )

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    if not BOT_TOKEN:
        raise ValueError("❌ TELEGRAM_BOT_TOKEN is not set!")

    app = Application.builder().token(BOT_TOKEN).build()

    conv = ConversationHandler(
        entry_points=[
            CommandHandler("start", start),
            CallbackQueryHandler(button_handler),
        ],
        states={
            PASTE_COOKIES: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_cookies),
                CallbackQueryHandler(button_handler),
            ],
            BULK_TYPE: [
                CallbackQueryHandler(button_handler),
                MessageHandler(filters.Document.ALL, receive_bulk_file),
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        per_message=False,
    )

    app.add_handler(CommandHandler("debug", debug))
    app.add_handler(CommandHandler("env",   env_check))
    app.add_handler(conv)

    logger.info("🤖 Bot is running...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
