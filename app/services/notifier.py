import asyncio
import logging
import requests
import orjson
from typing import List, Dict, Any
from app.models.service import WebhookConfig

logger = logging.getLogger(__name__)

async def trigger_webhooks(webhooks: List[WebhookConfig], logs: List[Dict[str, Any]]):
    if not webhooks or not logs:
        return

    for webhook in webhooks:
        if not webhook.enabled:
            continue

        matching_logs = []
        for log in logs:
            # Check level match
            level_match = log.get("level") in webhook.levels
            
            # Check keyword match
            keyword_match = False
            if webhook.keywords:
                msg = log.get("message", "").lower()
                keyword_match = any(kw.lower() in msg for kw in webhook.keywords)
            
            if level_match or keyword_match:
                matching_logs.append(log)

        if matching_logs:
            # Run webhook delivery in a separate thread to not block the event loop
            asyncio.create_task(send_webhook_request(webhook.url, matching_logs))

async def send_webhook_request(url: str, logs: List[Dict[str, Any]]):
    try:
        service_name = logs[0].get("service_name", "Unknown")
        count = len(logs)
        
        # Default generic payload
        payload: Dict[str, Any] = {
            "event": "log_alert",
            "service": service_name,
            "count": count,
            "logs": logs[:10]
        }

        # Discord Specific Formatting
        if "discord.com/api/webhooks" in url:
            embeds = []
            for log in logs[:5]: # Limit to 5 for Discord embeds
                color = 15548997 if log.get("level") in ["ERROR", "FATAL"] else 15105570
                embeds.append({
                    "title": f"[{log.get('level')}] {service_name}",
                    "description": log.get("message"),
                    "color": color,
                    "footer": {"text": f"Timestamp: {log.get('timestamp')}"}
                })
            payload = {
                "content": f"🚨 **Velicor Alert**: {count} critical events detected in `{service_name}`",
                "embeds": embeds
            }

        # Slack Specific Formatting
        elif "hooks.slack.com/services" in url:
            payload = {
                "text": f"🚨 *Velicor Alert*: {count} critical events detected in `{service_name}`",
                "blocks": [
                    {
                        "type": "section",
                        "text": {"type": "mrkdwn", "text": f"🚨 *Velicor Alert*: {count} critical events detected in `{service_name}`"}
                    },
                    {
                        "type": "divider"
                    }
                ]
            }
            for log in logs[:5]:
                payload["blocks"].append({
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"*[{log.get('level')}]* {log.get('message')}"}
                })

        def do_post():
            return requests.post(
                url, 
                data=orjson.dumps(payload),
                headers={"Content-Type": "application/json"},
                timeout=5
            )
        
        response = await asyncio.to_thread(do_post)
        if response.status_code >= 400:
            logger.error(f"Webhook delivery failed to {url}: {response.status_code} - {response.text}")
        else:
            logger.info(f"Successfully delivered webhook alert to {url}")
    except Exception as e:
        logger.error(f"Error sending webhook to {url}: {e}")
