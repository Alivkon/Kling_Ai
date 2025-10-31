#!/usr/bin/env python3
import hashlib
import logging
import os
from typing import Dict, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, Request, HTTPException
from aiogram import Bot

# Load .env
load_dotenv()

# Config
BOT_TOKEN = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_TOKEN")
YOOMONEY_SECRET = os.getenv("YOOMONEY_SECRET", "")

# Logging
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s %(name)s: %(message)s')
logger = logging.getLogger(__name__)

# Import payment functions from kling_bot.py (or payments.py if separated)
# Assuming payments.py has the functions; if not, import from kling_bot
try:
    from payments import increment_user_balance
except ImportError:
    from kling_bot import increment_user_balance

# Initialize bot
bot = Bot(token=BOT_TOKEN)

app = FastAPI()

@app.post("/webhook/yoomoney")
async def yoomoney_webhook(request: Request):
    """
    Handle YooMoney webhook notifications.
    Expects POST data as form-encoded or JSON with payment details.
    """
    logger.info("YooMoney webhook received")

    # Get data from request
    if request.headers.get("content-type") == "application/json":
        data = await request.json()
        params = {k: str(v) for k, v in data.items()}
    else:
        form_data = await request.form()
        params = {k: v for k, v in form_data.items()}

    logger.info(f"Webhook params: {params}")

    # Normalize keys to canonical names
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
    normalized_params: Dict[str, str] = {}
    for k, v in params.items():
        normalized_params[key_map.get(k.lower(), k.lower())] = str(v)

    # Build signature string and compute SHA-1
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
    secret = YOOMONEY_SECRET or normalized_params.get("secret", "")
    sign_string = "&".join([normalized_params.get(k, "") for k in ordered_keys] + [secret])
    sha1_hex = hashlib.sha1(sign_string.encode("utf-8")).hexdigest()

    received_hash = normalized_params.get("sha1_hash") or normalized_params.get("md5") or normalized_params.get("notification_secret") or ""
    logger.info(f"SHA1 computed={sha1_hex}, received={received_hash}")

    # Validate signature and other checks
    is_valid = (
        received_hash.lower() == sha1_hex.lower() and
        normalized_params.get("codepro", "false").lower() == "false" and
        normalized_params.get("currency") == "643"  # RUB
    )
    if not is_valid:
        logger.warning("Invalid webhook signature or parameters")
        raise HTTPException(status_code=400, detail="Invalid signature or parameters")

    # Extract user_id from label
    label = normalized_params.get("label", "")
    user_id: Optional[int] = None
    if label.isdigit():
        user_id = int(label)
    elif label.startswith("user_id:") and label.split(":", 1)[1].isdigit():
        user_id = int(label.split(":", 1)[1])
    else:
        digits = "".join(ch for ch in label if ch.isdigit())
        if digits:
            user_id = int(digits)

    if user_id is None:
        logger.warning("Could not extract user_id from label")
        raise HTTPException(status_code=400, detail="Invalid user_id in label")

    # Process payment: increment balance
    try:
        new_balance = increment_user_balance(user_id, 1)  # Adjust delta as needed
        logger.info(f"Payment processed for user {user_id}, new balance: {new_balance}")

        # Notify user via Telegram
        await bot.send_message(
            chat_id=user_id,
            text=f"Платеж получен! Ваш баланс: {new_balance} генераций."
        )
    except Exception as e:
        logger.exception(f"Error processing payment for user {user_id}")
        raise HTTPException(status_code=500, detail="Internal error processing payment")

    return {"status": "ok"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)