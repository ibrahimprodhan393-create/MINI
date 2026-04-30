import httpx

from .config import settings


class TelegramNotifier:
    def __init__(self) -> None:
        self.enabled = bool(settings.bot_token)

    def web_app_menu_button(self) -> dict:
        return {
            "type": "web_app",
            "text": settings.telegram_menu_button_text,
            "web_app": {"url": settings.public_app_url},
        }

    def web_app_inline_keyboard(self) -> dict:
        return {
            "inline_keyboard": [
                [
                    {
                        "text": settings.telegram_menu_button_text,
                        "web_app": {"url": settings.public_app_url},
                    }
                ]
            ]
        }

    async def call_api(self, method: str, payload: dict) -> dict:
        if not self.enabled:
            return {"ok": False, "description": "BOT_TOKEN is not configured."}
        url = f"https://api.telegram.org/bot{settings.bot_token}/{method}"
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(url, json=payload)
            response.raise_for_status()
            return response.json()

    async def configure_menu_button(self, chat_id: int | None = None) -> None:
        if not self.enabled or not settings.public_app_url:
            return
        payload: dict = {"menu_button": self.web_app_menu_button()}
        if chat_id:
            payload["chat_id"] = chat_id
        try:
            await self.call_api("setChatMenuButton", payload)
        except httpx.HTTPError:
            return

    async def remove_reply_keyboard(self, chat_id: int) -> None:
        if not self.enabled or not chat_id:
            return
        try:
            response = await self.call_api(
                "sendMessage",
                {
                    "chat_id": chat_id,
                    "text": "Menu updated.",
                    "reply_markup": {"remove_keyboard": True},
                },
            )
            message_id = response.get("result", {}).get("message_id")
            if message_id:
                await self.call_api("deleteMessage", {"chat_id": chat_id, "message_id": message_id})
        except httpx.HTTPError:
            return

    async def send_message(self, chat_id: int, text: str) -> None:
        if not self.enabled or not chat_id:
            return
        try:
            await self.call_api(
                "sendMessage",
                {
                    "chat_id": chat_id,
                    "text": text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
            )
        except httpx.HTTPError:
            return

    async def send_web_app_button(self, chat_id: int) -> None:
        if not self.enabled or not chat_id:
            return
        if not settings.public_app_url:
            await self.send_message(chat_id, "Mini App URL is not configured yet.")
            return
        payload = {
            "chat_id": chat_id,
            "text": "Welcome to the store. Tap the button below to open the Mini App.",
            "reply_markup": self.web_app_inline_keyboard(),
        }
        try:
            await self.configure_menu_button(chat_id)
            await self.remove_reply_keyboard(chat_id)
            await self.call_api("sendMessage", payload)
        except httpx.HTTPError:
            return

    async def notify_admins(self, text: str) -> None:
        for admin_id in settings.admin_telegram_ids:
            await self.send_message(admin_id, text)


notifier = TelegramNotifier()
