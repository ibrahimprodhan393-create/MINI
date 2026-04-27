import httpx

from .config import settings


class TelegramNotifier:
    def __init__(self) -> None:
        self.enabled = bool(settings.bot_token)

    async def send_message(self, chat_id: int, text: str) -> None:
        if not self.enabled or not chat_id:
            return
        url = f"https://api.telegram.org/bot{settings.bot_token}/sendMessage"
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(
                    url,
                    json={
                        "chat_id": chat_id,
                        "text": text,
                        "parse_mode": "HTML",
                        "disable_web_page_preview": True,
                    },
                )
        except httpx.HTTPError:
            return

    async def notify_admins(self, text: str) -> None:
        for admin_id in settings.admin_telegram_ids:
            await self.send_message(admin_id, text)


notifier = TelegramNotifier()
