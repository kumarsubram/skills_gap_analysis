"""
GitHub Utils - Clean & Simple
============================

Uses SAME keyword detection as historical DAG.
Place at: /usr/local/airflow/include/utils/github_utils.py
"""

from datetime import datetime, timezone
from typing import Dict, List, Any
import re
import json
import os

def load_keywords():
    """Load keywords using same approach as historical DAG"""
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    keyword_path = os.path.join(base_dir, "include", "config", "keywords.json")
    
    with open(keyword_path, "r", encoding="utf-8") as f:
        return json.load(f)

def extract_technologies_from_event(event: Dict[str, Any], keywords: Dict[str, Any]) -> List[str]:
    """Extract technologies using SAME logic as historical DAG"""
    found_technologies = []
    messages = []
    event_type = event.get("type")
    
    # Collect text to search (same as historical)
    if event_type == "PushEvent":
        commits = event.get("payload", {}).get("commits", [])
        for commit in commits:
            if commit.get("message"):
                messages.append(commit["message"].lower())
    
    elif event_type == "PullRequestEvent":
        pr = event.get("payload", {}).get("pull_request", {})
        if pr.get("title"):
            messages.append(pr["title"].lower())
        if pr.get("body"):
            messages.append(pr.get("body", "")[:500].lower())
    
    elif event_type == "ReleaseEvent":
        release = event.get("payload", {}).get("release", {})
        if release.get("name"):
            messages.append(release["name"].lower())
        if release.get("body"):
            messages.append(release.get("body", "")[:500].lower())
    
    elif event_type == "CreateEvent":
        if event.get("payload", {}).get("description"):
            messages.append(event["payload"]["description"].lower())
    
    # Add repo name
    repo_name = event.get("repo", {}).get("name", "")
    if repo_name:
        messages.append(repo_name.lower())
    
    # Search for keywords (same logic as historical)
    for message in messages:
        for keyword in keywords:
            pattern = rf"\b{re.escape(keyword.lower())}\b"
            if re.search(pattern, message):
                found_technologies.append(keyword)
                break
    
    return list(set(found_technologies))

def process_github_event_detailed(event: Dict[str, Any]) -> Dict[str, Any]:
    """Simple event processing with payload details"""
    processed_event = {
        'id': event.get('id'),
        'type': event.get('type'),
        'actor_login': event.get('actor', {}).get('login'),
        'repo_name': event.get('repo', {}).get('name'),
        'repo_id': event.get('repo', {}).get('id'),
        'created_at': event.get('created_at'),
        'processed_at': datetime.now(timezone.utc).isoformat(),
    }
    
    # Add event-specific details
    payload = event.get('payload', {})
    
    if event.get('type') == 'PushEvent':
        commits = payload.get('commits', [])
        processed_event.update({
            'push_size': payload.get('size', 0),
            'commits_count': len(commits),
            'commit_messages': [commit.get('message', '') for commit in commits[:3]]
        })
    
    elif event.get('type') == 'ReleaseEvent':
        release = payload.get('release', {})
        processed_event.update({
            'release_tag': release.get('tag_name'),
            'release_name': release.get('name'),
        })
    
    elif event.get('type') == 'PullRequestEvent':
        pr = payload.get('pull_request', {})
        processed_event.update({
            'pr_title': pr.get('title', ''),
            'pr_action': payload.get('action', ''),
        })
    
    return processed_event