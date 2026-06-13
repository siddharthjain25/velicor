import asyncio
import logging
import requests
import orjson
from typing import List, Dict, Any
from datetime import datetime
from app.models.service import WebhookConfig
from app.core.config import settings

logger = logging.getLogger(__name__)

async def trigger_webhooks(webhooks: List[WebhookConfig], logs: List[Dict[str, Any]]):
    if not webhooks or not logs:
        return

    for webhook in webhooks:
        if not webhook.enabled:
            continue

        # Check service filter
        if webhook.services:
            service_name = logs[0].get("service_name")
            if service_name and service_name not in webhook.services:
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
            # Run webhook delivery
            if settings.is_serverless:
                await send_webhook_request(webhook.url, matching_logs)
            else:
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

async def trigger_retention_webhooks(webhooks: List[WebhookConfig], service_name: str, retention_days: int, deleted_count: int):
    if not webhooks:
        return

    for webhook in webhooks:
        if not webhook.enabled:
            continue
        
        # Check service filter
        if webhook.services and service_name not in webhook.services:
            continue

        # Trigger retention webhook delivery
        if settings.is_serverless:
            await send_retention_webhook_request(webhook.url, service_name, retention_days, deleted_count)
        else:
            asyncio.create_task(send_retention_webhook_request(webhook.url, service_name, retention_days, deleted_count))

async def send_retention_webhook_request(url: str, service_name: str, retention_days: int, deleted_count: int):
    try:
        # Default generic payload
        payload: Dict[str, Any] = {
            "event": "log_retention",
            "service": service_name,
            "retention_days": retention_days,
            "deleted_count": deleted_count,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }

        # Discord Specific Formatting
        if "discord.com/api/webhooks" in url:
            payload = {
                "content": f"🧹 **Velicor Retention**: Purged old logs for service `{service_name}`",
                "embeds": [{
                    "title": f"Log Retention Completed: {service_name}",
                    "color": 3447003,
                    "fields": [
                        {"name": "Retention Policy", "value": f"{retention_days} days", "inline": True},
                        {"name": "Logs Purged", "value": f"{deleted_count:,}", "inline": True}
                    ],
                    "footer": {"text": f"Completed at: {datetime.utcnow().isoformat()}Z"}
                }]
            }

        # Slack Specific Formatting
        elif "hooks.slack.com/services" in url:
            payload = {
                "text": f"🧹 *Velicor Retention*: Purged {deleted_count:,} logs for service `{service_name}` (older than {retention_days} days)",
                "blocks": [
                    {
                        "type": "section",
                        "text": {"type": "mrkdwn", "text": f"🧹 *Velicor Retention*: Purged logs for service `{service_name}`"}
                    },
                    {
                        "type": "section",
                        "fields": [
                            {"type": "mrkdwn", "text": f"*Retention Policy:* {retention_days} days"},
                            {"type": "mrkdwn", "text": f"*Logs Purged:* {deleted_count:,}"}
                        ]
                    }
                ]
            }

        def do_post():
            return requests.post(
                url, 
                data=orjson.dumps(payload),
                headers={"Content-Type": "application/json"},
                timeout=5
            )
        
        response = await asyncio.to_thread(do_post)
        if response.status_code >= 400:
            logger.error(f"Retention webhook delivery failed to {url}: {response.status_code} - {response.text}")
        else:
            logger.info(f"Successfully delivered retention webhook to {url}")
    except Exception as e:
        logger.error(f"Error sending retention webhook to {url}: {e}")

