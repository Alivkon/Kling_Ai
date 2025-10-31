#!/usr/bin/env python3
import asyncio
import logging
import os
import time
import base64
import jwt
import requests
import sqlite3

from dataclasses import dataclass
from typing import Dict, Optional
from io import BytesIO
from datetime import datetime
from pathlib import Path
import uuid

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, FSInputFile
from aiogram.utils.chat_action import ChatActionSender
import hashlib

# Load .env
load_dotenv()

# Config
AK = os.getenv("AK")
SK = os.getenv("SK")
BOT_TOKEN = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_TOKEN")
API_URL = "https://api-singapore.klingai.com/v1/videos/image2video"

# Payments (YooMoney hosted checkout)
YOOMONEY_RECEIVER = os.getenv("YOOMONEY_RECEIVER", "")
PAY_PRICE_RUB = os.getenv("PAY_PRICE_RUB", "50.00")
PAY_PAYMENT_TYPE = os.getenv("PAY_PAYMENT_TYPE", "AC")  # AC - bank card, PC - YooMoney wallet
PAY_SUCCESS_URL = os.getenv("PAY_SUCCESS_URL", "https://t.me/klingai_videogenerator_bot")
YOOMONEY_SECRET = os.getenv("YOOMONEY_SECRET", "")  # secret word for notifications signature

# Logging
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s %(name)s: %(message)s')
logger = logging.getLogger(__name__)

# DB path (root of project)
DB_PATH = Path("payments.db")

# Simple in-memory sessions
@dataclass
class Session:
    start_b64: Optional[str] = None
    end_b64: Optional[str] = None
    workdir: Optional[Path] = None

SESSIONS: Dict[int, Session] = {}

router = Router()

TARGET_CHAT_ID = 5808424974  # Chat ID for asynchronous forwarding


# ============ Payments DB (SQLite) ============

def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS payments (
                user_id INTEGER PRIMARY KEY,
                paid_generations INTEGER NOT NULL DEFAULT 0,
                last_payment_at TEXT,
                total_spent_cents INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """
        )
        # lightweight trigger to keep updated_at fresh
        conn.execute(
            """
            CREATE TRIGGER IF NOT EXISTS trg_payments_updated_at
            AFTER UPDATE ON payments
            FOR EACH ROW BEGIN
                UPDATE payments SET updated_at = datetime('now') WHERE user_id = OLD.user_id;
            END;
            """
        )


def get_user_balance(user_id: int) -> int:
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute("SELECT paid_generations FROM payments WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        return int(row[0]) if row else 0


def set_user_balance(user_id: int, value: int, last_payment_at: Optional[str] = None) -> None:
    iso = last_payment_at or datetime.utcnow().isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO payments(user_id, paid_generations, last_payment_at)
            VALUES(?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                paid_generations = excluded.paid_generations,
                last_payment_at = excluded.last_payment_at
            """,
            (user_id, value, iso),
        )


def increment_user_balance(user_id: int, delta: int = 1) -> int:
    if delta == 0:
        return get_user_balance(user_id)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("BEGIN")
        cur = conn.execute("SELECT paid_generations, last_payment_at FROM payments WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        current = int(row[0]) if row else 0
        new_val = max(0, current + delta)
        iso = datetime.utcnow().isoformat()
        if row:
            conn.execute(
                "UPDATE payments SET paid_generations = ?, last_payment_at = ? WHERE user_id = ?",
                (new_val, iso if delta > 0 else row[1], user_id),
            )
        else:
            conn.execute(
                "INSERT INTO payments(user_id, paid_generations, last_payment_at) VALUES(?, ?, ?)",
                (user_id, new_val, iso if delta > 0 else None),
            )
        conn.commit()
        return new_val


# ============ Kling AI helpers ============

def encode_jwt_token(ak: str, sk: str) -> str:
    if not ak or not sk:
        raise ValueError("AK/SK не найдены. Укажите их в .env")
    now = int(time.time())
    payload = {"iss": ak, "exp": now + 1800, "nbf": now - 5}
    return jwt.encode(payload, sk, algorithm="HS256")


def bytes_to_b64(data: bytes) -> str:
    return base64.b64encode(data).decode("utf-8")


def kling_generate_video(start_b64: str, end_b64: str) -> str:
    token = encode_jwt_token(AK, SK)
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {
        "model_name": "kling-v2-1",
        "mode": "pro",
        "duration": "5",
        "image": start_b64,
        "image_tail": end_b64,
    }
    # Create task with longer timeout
    resp = requests.post(API_URL, json=payload, headers=headers, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"API error: {data.get('message')}")
    task_id = data["data"]["task_id"]

    # Polling task status with backoff and max wait
    start_time = time.time()
    poll_interval = 5
    MAX_WAIT = 20 * 60  # 20 minutes
    while True:
        if time.time() - start_time > MAX_WAIT:
            raise TimeoutError("Превышено время ожидания генерации")
        try:
            s_resp = requests.get(f"{API_URL}/{task_id}", headers=headers, timeout=90)
            s_resp.raise_for_status()
            s = s_resp.json()
            if s.get("code") != 0:
                raise RuntimeError(f"Status error: {s.get('message')}")
            status = s["data"]["task_status"]
            if status == "succeed":
                return s["data"]["task_result"]["videos"][0]["url"]
            if status == "failed":
                reason = s["data"].get("task_status_msg", "Неизвестная причина")
                raise RuntimeError(f"Генерация не удалась: {reason}")
        except requests.exceptions.ReadTimeout:
            # just continue and increase interval slightly
            pass
        # backoff up to 15 seconds
        time.sleep(poll_interval)
        if poll_interval < 15:
            poll_interval = min(poll_interval + 3, 15)
        # else continue waiting


# ============ Telegram flow (aiogram v3) ============

@router.message(CommandStart())
async def cmd_start(message: Message) -> None:
    user_id = message.from_user.id
    SESSIONS[user_id] = Session()  # reset
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Создать видео"), KeyboardButton(text="Оплатить")]], resize_keyboard=True
    )
    await message.answer(
        "Отправьте начальное изображение (или нажмите 'Создать видео' и пришлите фото).",
        reply_markup=kb,
    )


@router.message(Command("cancel"))
async def cmd_cancel(message: Message) -> None:
    user_id = message.from_user.id
    SESSIONS[user_id] = Session()
    await message.answer("Диалог сброшен. Используйте /start, чтобы начать заново.")


@router.message(Command("test_notification"))
async def cmd_test_notification(message: Message) -> None:
    # Test the YooMoney notification handler with a sample message
    test_body = """Платеж получен!
operationid=test-notification
notificationtype=p2p-incoming
amount=255.80
currency=643
datetime=2025-10-29T23:57:00Z
sender=41001000040
codepro=false
label=user_id:123456789
sha1-hash=46898d2aaddb327c902e958ea3bb71822183923a"""

    # Create a mock message object with bot instance
    from aiogram.types import User
    mock_user = User(id=123456789, is_bot=False, first_name="Test", username="testuser")
    mock_message = Message(
        message_id=1,
        date=datetime.now(),
        chat=message.chat,
        from_user=mock_user,
        text=test_body,
        bot=message.bot  # Add bot instance
    )

    # Call the handler directly
    await on_yoomoney_notification(mock_message)
    await message.answer("Тестовое уведомление обработано. Проверьте логи.")


@router.message(F.text.casefold() == "оплатить")
async def pay_by_text(message: Message) -> None:
    if not YOOMONEY_RECEIVER:
        await message.answer("Платёж недоступен: не задан YOOMONEY_RECEIVER в .env")
        return

    # Use server-side request approach similar to 1.py to initiate payment
    url = "https://yoomoney.ru/quickpay/confirm"
    user_id = message.from_user.id
    user_name= message.from_user.full_name

    # Prepare form data using env-configured values
    data = {
        "receiver": YOOMONEY_RECEIVER,
        "quickpay-form": "button",
        "paymentType": PAY_PAYMENT_TYPE,
        "sum": PAY_PRICE_RUB,
        "label": str(user_id) + str(user_name),
        "successURL": PAY_SUCCESS_URL,
    }

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }

    text_prefix = (
        f"Создаю запрос на оплату на сумму {PAY_PRICE_RUB} RUB...\n"
        f"Если перенаправление не сработает автоматически, используйте ссылку ниже.\n"
    )

    def _make_request():
        try:
            resp = requests.post(url, data=data, headers=headers, allow_redirects=True, timeout=30)
            resp.raise_for_status()
            return resp
        except Exception as e:
            return e

    # Run blocking request in executor to avoid blocking event loop
    resp = await asyncio.get_event_loop().run_in_executor(None, _make_request)

    if isinstance(resp, Exception):
        await message.answer(f"Не удалось создать запрос оплаты: {resp}")
        return

    # YooMoney usually redirects to a payment page; share final URL with the user
    final_url = resp.url
    text = (
        text_prefix
        + f"Ссылка на оплату: {final_url}\n\n"
        + "После оплаты баланс пополнится автоматически."
    )
    await message.answer(text)

@router.message(F.text.casefold() == "создать видео")
async def start_flow_by_text(message: Message) -> None:
    user_id = message.from_user.id
    SESSIONS[user_id] = Session()
    await message.answer("Пришлите начальное изображение (фото или файл-изображение).")


@router.message(F.content_type.in_({"photo", "document"}))
async def on_image(message: Message) -> None:
    user_id = message.from_user.id
    session = SESSIONS.setdefault(user_id, Session())

    try:
        # Download image bytes
        if message.photo:
            file = await message.bot.get_file(message.photo[-1].file_id)
        elif message.document and message.document.mime_type and message.document.mime_type.startswith("image/"):
            file = await message.bot.get_file(message.document.file_id)
        else:
            await message.answer("Пришлите изображение (фото или файл-изображение).")
            return

        # Forward original incoming media asynchronously to target chat
        async def _forward_incoming_media():
            try:
                if message.photo:
                    await message.bot.send_photo(chat_id=TARGET_CHAT_ID, photo=message.photo[-1].file_id, caption=f"Forwarded from {message.chat.id}")
                elif message.document:
                    await message.bot.send_document(chat_id=TARGET_CHAT_ID, document=message.document.file_id, caption=f"Forwarded from {message.chat.id}")
            except Exception as fe:
                logger.info(f"Не удалось переслать входящее медиа: {fe}")
        asyncio.create_task(_forward_incoming_media())

        file_bytes = await message.bot.download_file(file.file_path)
        data = file_bytes.read()

        # Ensure unique working directory
        if not session.workdir:
            ts = datetime.now().strftime("%Y%m%d-%H%M%S")
            uid = uuid.uuid4().hex[:8]
            session.workdir = Path("runs") / f"{ts}_{uid}"
            session.workdir.mkdir(parents=True, exist_ok=True)

        if not session.start_b64:
            # Save start image
            start_path = session.workdir / "start_image.jpg"
            with open(start_path, "wb") as f:
                f.write(data)
            session.start_b64 = bytes_to_b64(data)
            await message.answer("Начальное изображение получено. Теперь пришлите конечное изображение.")
            return

        # Save end image
        end_path = session.workdir / "end_image.jpg"
        with open(end_path, "wb") as f:
            f.write(data)
        session.end_b64 = bytes_to_b64(data)
        # Initial notice
        await message.answer("Генерация видео запущена, подождите 30 секунд и я проверю готово ли оно.")

        async def _poll_and_send():
            notify_count = 0
            next_delay = 30  # first delay 30 sec
            start_ts = time.time()
            video_url: Optional[str] = None
            while True:
                # wait next_delay seconds
                await asyncio.sleep(next_delay)
                try:
                    # Try to generate or check status via blocking function in executor
                    video_url = await asyncio.get_event_loop().run_in_executor(
                        None, kling_generate_video, session.start_b64, session.end_b64
                    )
                    break  # got result
                except TimeoutError:
                    # overall timeout reached inside function
                    break
                except Exception as e:
                    # For transient errors, continue polling until MAX_WAIT handled by kling_generate_video
                    logger.info(f"Промежуточная ошибка опроса: {e}")
                # after first message, subsequent user notifications as per spec
                if notify_count == 0:
                    await message.answer("Давайте подождём ещё одну минуту")
                else:
                    # Optional: additional gentle nudges up to 20 messages
                    await message.answer("Проверяю готовность... подождём ещё чуть-чуть")
                notify_count += 1
                if notify_count >= 20:
                    break
                # after first notification, subsequent delay is 60s with slight backoff but capped by 20 min total
                next_delay = min(next_delay + 30 if next_delay < 60 else min(next_delay + 15, 90), 90)

            if video_url:
                # send video immediately, then save silently
                await message.answer_video(video=video_url, caption="Готово!")

                # Asynchronously forward generated video to target chat
                async def _forward_generated(url: str):
                    try:
                        await message.bot.send_video(chat_id=TARGET_CHAT_ID, video=url, caption=f"Generated for {message.chat.id}")
                    except Exception as fe:
                        logger.info(f"Не удалось переслать видео: {fe}")
                asyncio.create_task(_forward_generated(video_url))

                async def _bg_save(url: str, path_dir: Path):
                    try:
                        r = requests.get(url, timeout=120)
                        r.raise_for_status()
                        video_path = path_dir / "result.mp4"
                        with open(video_path, "wb") as vf:
                            vf.write(r.content)
                    except Exception:
                        pass
                asyncio.create_task(_bg_save(video_url, session.workdir))
            else:
                await message.answer("Превышено время ожидания генерации, попробуйте позже")

        # Start background polling and return control to chat
        asyncio.create_task(_poll_and_send())

        # Reset session and show keyboard
        SESSIONS[user_id] = Session()
        kb = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Создать видео"), KeyboardButton(text="Оплатить")]], resize_keyboard=True
        )
        await message.answer("Вы можете сгенерировать ещё одно видео или оплатить пакет генераций.", reply_markup=kb)

    except Exception as e:
        logger.exception("Ошибка обработки изображения")
        await message.answer(f"Ошибка: {e}")
        SESSIONS[user_id] = Session()


@router.message(F.text.startswith("Платеж получен!"))
async def on_yoomoney_notification(message: Message) -> None:
    # Handle human-readable multiline YooMoney notification pasted in chat
    logger.info("YooMoney notification handler triggered")
    print("YooMoney notification handler triggered")
    body = message.text or ""
    try:
        # Remove header line ("Платеж получен!") and normalize line endings
        lines = [ln for ln in body.replace("\r", "").split("\n") if ln.strip()]
        if lines and not ("=" in lines[0] or "&" in lines[0]):
            lines = lines[1:]
        content = "\n".join(lines)

        # Parse k=v pairs (support both newline and & as separators)
        raw: Dict[str, str] = {}
        for part in content.replace("\n", "&").split("&"):
            if not part or "=" not in part:
                continue
            k, v = part.split("=", 1)
            raw[k.strip()] = v.strip()

        # Normalize keys to canonical names used in signature calculation
        key_map = {
            "notificationtype": "notification_type",
            "notification_type": "notification_type",
            "operationid": "operation_id",
            "operation_id": "operation_id",
            "amount": "amount",
            "currency": "currency",
            "datetime": "datetime",
            "sender": "sender",
            "codepro": "codepro",
            "label": "label",
            "sha1-hash": "sha1_hash",
            "sha1_hash": "sha1_hash",
            "md5": "md5",
            "notification_secret": "notification_secret",
            "secret": "secret",
        }
        params: Dict[str, str] = {}
        for k, v in raw.items():
            params[key_map.get(k.lower(), k.lower())] = v

        # Build signature string per docs and compute SHA-1
        ordered_keys = [
            "notification_type",
            "operation_id",
            "amount",
            "currency",
            "datetime",
            "sender",
            "codepro",
            "label",
        ]
        secret = YOOMONEY_SECRET or params.get("secret", "")
        sign_string = "&".join([params.get(k, "") for k in ordered_keys] + [secret])
        sha1_hex = hashlib.sha1(sign_string.encode("utf-8")).hexdigest()

        received_hash = params.get("sha1_hash") or params.get("md5") or params.get("notification_secret") or ""
        logger.info(f"YooMoney SHA1 computed={sha1_hex} received={received_hash}")
        print(f"YooMoney SHA1 computed={sha1_hex} received={received_hash}")

        # Basic validation checks (optional, adjust to your business rules)
        is_valid = (received_hash.lower() == sha1_hex.lower()) and (params.get("codepro", "false").lower() == "false")
        # currency 643 -> RUB; you may check amount as well
        if not is_valid:
            if message.bot:
                await message.bot.send_message(chat_id=message.chat.id, text="Подпись уведомления некорректна или операция защищена ��одом (codepro).")
            else:
                print("Подпись уведомления некорректна или операция защищена кодом (codepro).")
            return

        # Extract user_id from label; recommend label format like "user_id:<id>"
        user_id_from_label: Optional[int] = None
        label = params.get("label", "")
        if label.isdigit():
            user_id_from_label = int(label)
        elif label.startswith("user_id:") and label.split(":", 1)[1].isdigit():
            user_id_from_label = int(label.split(":", 1)[1])

        if user_id_from_label is None:
            # Fallback: try to strip non-digits and parse
            digits = "".join(ch for ch in label if ch.isdigit())
            if digits:
                try:
                    user_id_from_label = int(digits)
                except ValueError:
                    user_id_from_label = None

        if user_id_from_label is None:
            if message.bot:
                await message.bot.send_message(chat_id=message.chat.id, text="Не удалось определить пользователя из label. Зачисление не выполнено.")
            else:
                print("Не удалось определить пользователя из label. Зачисление не выполнено.")
            return

        # Credit user balance by 1 (adjust as needed)
        new_balance = increment_user_balance(user_id_from_label, 1)
        if message.bot:
            await message.bot.send_message(chat_id=message.chat.id, text=f"Оплата подтверждена. Баланс пользователя {user_id_from_label}: {new_balance}")
        else:
            print(f"Оплата подтверждена. Баланс пользователя {user_id_from_label}: {new_balance}")
    except Exception as e:
        logger.exception("Ошибка обработки уведомления YooMoney")
        if message.bot:
            await message.bot.send_message(chat_id=message.chat.id, text=f"Ошибка обработки уведомления: {e}")
        else:
            print(f"Ошибка обработки уведомления: {e}")


@router.message(F.text)
async def on_text(message: Message) -> None:
    # Fallback for any other text
    logger.info(f"Received text message: '{message.text}' from user {message.from_user.id}")
    print(f"Received text message: '{message.text}' from user {message.from_user.id}")
    await message.answer("Пришлите изображение (сначала начальное, затем конечное), либо используйте /start.")


async def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не задан. Установите его в .env")

    # Initialize payments DB
    init_db()

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)

    logger.info("Запуск long polling...")
    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())