"""
Notification module for sending alerts via Telegram and creating GitHub Issues.
"""

import os
import requests
import json
from datetime import datetime


def load_env():
    """Load environment variables from .env file if it exists."""
    env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ.setdefault(key.strip(), value.strip())


def send_telegram_message(message, parse_mode="Markdown"):
    """
    Send a message to Telegram.
    
    Args:
        message: Text message to send
        parse_mode: "Markdown" or "HTML"
    
    Returns:
        bool: True if sent successfully
    """
    load_env()
    
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    
    if not bot_token or not chat_id:
        print("⚠️ Telegram credentials not found in environment")
        return False
    
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": parse_mode
    }
    
    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code == 200:
            print("✅ Telegram notification sent")
            return True
        elif response.status_code == 400 and parse_mode:
            print(f"⚠️ Telegram Markdown failed (400), retrying without formatting...")
            payload["parse_mode"] = None
            response = requests.post(url, json=payload, timeout=10)
            if response.status_code == 200:
                print("✅ Telegram notification sent (plain text fallback)")
                return True
        
        print(f"⚠️ Telegram API error: {response.status_code}")
        return False
    except Exception as e:
        print(f"⚠️ Failed to send Telegram message: {e}")
        return False


def create_github_issue(title, body, labels=None):
    """
    Create a GitHub Issue for error reporting.
    
    Args:
        title: Issue title
        body: Issue body (markdown)
        labels: List of labels
    
    Returns:
        str: Issue URL if created, None otherwise
    """
    load_env()
    
    token = os.environ.get("GITHUB_TOKEN")
    repo = os.environ.get("GITHUB_REPOSITORY", "Volodymyr75/petitions")
    
    if not token:
        print("⚠️ GITHUB_TOKEN not found in environment")
        return None
    
    url = f"https://api.github.com/repos/{repo}/issues"
    
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json"
    }
    
    payload = {
        "title": title,
        "body": body,
        "labels": labels or ["bug", "automation"]
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        if response.status_code == 201:
            issue_url = response.json().get("html_url")
            print(f"✅ GitHub Issue created: {issue_url}")
            return issue_url
        else:
            print(f"⚠️ GitHub API error: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"⚠️ Failed to create GitHub Issue: {e}")
        return None


def notify_sync_failure(stage, errors, details=None):
    """
    Send failure notification via all channels.
    
    Args:
        stage: "Pre-flight" or "Post-sync"
        errors: List of error messages
        details: Additional details dict
    """
    date_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    # Telegram message (short)
    tg_message = f"""🚨 *Petition Sync FAILED*
📅 {date_str}
📍 Stage: {stage}

*Errors:*
"""
    for error in errors[:3]:  # Limit to 3 errors for Telegram
        tg_message += f"• {error}\n"
    
    if len(errors) > 3:
        tg_message += f"... and {len(errors) - 3} more\n"
    
    tg_message += "\n🔧 Check GitHub Issue for details"
    
    send_telegram_message(tg_message)
    
    # GitHub Issue (detailed)
    issue_title = f"[Sync Failed] {stage} - {datetime.now().strftime('%Y-%m-%d')}"
    
    issue_body = f"""## Sync Failure Report

**Date:** {date_str}
**Stage:** {stage}

### Errors

"""
    for error in errors:
        issue_body += f"- {error}\n"
    
    if details:
        issue_body += f"""
### Details

```json
{json.dumps(details, indent=2, ensure_ascii=False)}
```
"""
    
    issue_body += """
### Possible Causes

- Website structure may have changed
- Network issues
- Rate limiting

### Action Required

1. Check the website manually
2. Review scraper selectors
3. Re-run sync after fixing
"""
    
    create_github_issue(issue_title, issue_body)


def notify_sync_success(stats):
    """
    Optionally notify about successful sync.
    
    Args:
        stats: Dictionary with sync statistics
    """
    date_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    message = f"""✅ *Petition Sync Completed*
📅 {date_str}

📊 *Stats:*
• New petitions: {stats.get('new_petitions', 0)}
• Vote delta: +{stats.get('vote_delta', 0):,}
• Status changes: {stats.get('status_changes', 0)}
"""
    
    send_telegram_message(message)


if __name__ == "__main__":
    # Test Telegram notification
    print("Testing Telegram notification...")
    send_telegram_message("🧪 Test message from Petition Analytics")
