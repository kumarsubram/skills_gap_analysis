"""
GitHub Events Producer DAG - FIXED PRODUCER.CLOSE() ERROR
=========================================================

✅ FIXED: Removed producer.close() - only use producer.flush()
✅ ENHANCED: Now runs indefinitely until manually stopped (no 1-hour limit)
✅ SAME DAG NAME: producer_github_events (no changes to DAG ID)

Replace: dags/producer_github_events.py
"""

import os
import json
import time
import requests
import re
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timezone


def load_tech_keywords():
    """Load technology keywords from your existing file"""
    keyword_path = "/opt/airflow/include/jsons/tech_keywords.json" 
    
    try:
        with open(keyword_path, "r", encoding="utf-8") as f:
            keywords_data = json.load(f)
        
        # Extract just the keys (keyword names) from your structured format
        if isinstance(keywords_data, dict):
            keywords = set(keywords_data.keys())
        else:
            keywords = set(keywords_data)
        
        # Convert to lowercase for case-insensitive matching
        keywords_lower = {kw.lower() for kw in keywords}
        
        print(f"📋 Loaded {len(keywords)} technology keywords")
        print(f"🔍 Sample keywords: {list(keywords)[:10]}")
        
        return keywords, keywords_lower
        
    except Exception as e:
        print(f"❌ Error loading keywords: {e}")
        # Fallback to basic keywords for testing
        basic_keywords = {
            'python', 'javascript', 'react', 'node', 'docker', 'api', 'web', 'app', 
            'js', 'py', 'css', 'html', 'sql', 'git', 'npm', 'pip', 'cli', 'bot',
            'server', 'client', 'lib', 'sdk', 'tool', 'framework', 'library'
        }
        print(f"⚠️ Using fallback keywords: {len(basic_keywords)} basic terms")
        return basic_keywords, basic_keywords


def extract_searchable_text(event):
    """Extract all searchable text from GitHub event"""
    text_parts = []
    
    # Repository name (MOST IMPORTANT for tech keywords)
    if 'repo' in event:
        repo_name = event['repo'].get('name', '')
        if repo_name:
            text_parts.append(repo_name.lower())
            # Extract just repo name without owner
            if '/' in repo_name:
                text_parts.append(repo_name.split('/')[-1].lower())
    
    # Event type
    text_parts.append(event.get('type', '').lower())
    
    # Actor (sometimes contains tech terms)
    if 'actor' in event:
        text_parts.append(event['actor'].get('login', '').lower())
    
    # Payload content based on event type
    payload = event.get('payload', {})
    event_type = event.get('type', '')
    
    if event_type == 'PushEvent':
        # Commit messages
        commits = payload.get('commits', [])
        for commit in commits[:3]:  # Limit to first 3 commits
            message = commit.get('message', '')
            if message:
                text_parts.append(message.lower()[:200])  # First 200 chars
    
    elif event_type == 'PullRequestEvent':
        pr = payload.get('pull_request', {})
        title = pr.get('title', '')
        if title:
            text_parts.append(title.lower())
        
        # Branch names often contain tech terms
        if 'head' in pr and pr['head']:
            branch = pr['head'].get('ref', '')
            if branch:
                text_parts.append(branch.lower())
    
    elif event_type == 'CreateEvent':
        # New repo/branch descriptions
        description = payload.get('description', '')
        if description:
            text_parts.append(description.lower()[:200])
        
        ref = payload.get('ref', '')
        if ref:
            text_parts.append(ref.lower())
    
    elif event_type == 'ReleaseEvent':
        release = payload.get('release', {})
        name = release.get('name', '')
        if name:
            text_parts.append(name.lower())
        
        tag = release.get('tag_name', '')
        if tag:
            text_parts.append(tag.lower())
    
    elif event_type == 'IssuesEvent':
        issue = payload.get('issue', {})
        title = issue.get('title', '')
        if title:
            text_parts.append(title.lower()[:100])
    
    # Join all text
    full_text = ' '.join(filter(None, text_parts))
    return full_text


def find_keywords_in_event(event, keywords, keywords_lower):
    """Find technology keywords in a GitHub event"""
    searchable_text = extract_searchable_text(event)
    
    if not searchable_text:
        return []
    
    found_keywords = []
    
    # Extract words from text
    words = re.findall(r'\b\w+\b', searchable_text)
    word_set = set(words)
    
    # Check for exact word matches
    for keyword_lower in keywords_lower:
        if keyword_lower in word_set:
            # Find original keyword case
            original_keyword = next(k for k in keywords if k.lower() == keyword_lower)
            found_keywords.append(original_keyword)
    
    # Also check for substring matches (for compound terms)
    for keyword_lower in keywords_lower:
        if len(keyword_lower) > 3 and keyword_lower in searchable_text:
            original_keyword = next(k for k in keywords if k.lower() == keyword_lower)
            if original_keyword not in found_keywords:
                found_keywords.append(original_keyword)
    
    return list(set(found_keywords))  # Remove duplicates


def get_kafka_bootstrap_servers():
    """Kafka is always localhost for container-to-container communication"""
    return 'kafka:29092'


def produce_github_events(**context):
    """Produce GitHub events to Kafka - runs for 1 hour then restarts"""
    
    print("🚀 GITHUB EVENTS PRODUCER - HOURLY AUTOMATIC RESTARTS")
    print("=" * 60)
    
    # Import Kafka producer
    try:
        from confluent_kafka import Producer
    except ImportError:
        print("❌ confluent-kafka not installed")
        return {'status': 'failed', 'error': 'confluent-kafka missing'}
    
    # Load technology keywords
    keywords, keywords_lower = load_tech_keywords()
    
    # Setup
    bootstrap_servers = get_kafka_bootstrap_servers()
    topic = 'github-events-raw'
    
    print(f"🔗 Kafka: {bootstrap_servers}")
    print(f"📤 Topic: {topic}")
    print(f"🔍 Filtering for {len(keywords)} technology keywords")
    print("⏰ Producer will run for 1 hour then restart automatically")
    
    # Create Kafka producer
    producer = Producer({
        'bootstrap.servers': bootstrap_servers,
        'client.id': 'github-events-producer-enhanced',
        'acks': 'all',
        'retries': 3
    })
    
    # GitHub API setup
    headers = {
        'Accept': 'application/vnd.github.v3+json',
        'User-Agent': 'JobOrbit-Producer/2.0'
    }
    
    # Add GitHub token if available
    github_token = os.getenv('GITHUB_TOKEN')
    if github_token:
        headers['Authorization'] = f'token {github_token}'
        print("✅ Using GitHub token for higher rate limits")
    else:
        print("⚠️ No GitHub token - limited to 60 requests/hour")
    
    # Enhanced statistics tracking
    stats = {
        'total_events_fetched': 0,
        'events_with_keywords': 0,
        'events_sent_to_kafka': 0,
        'keyword_matches': {},
        'cycles': 0,
        'api_calls': 0
    }
    
    # Start streaming
    start_time = datetime.now()
    
    print(f"⏰ Started at: {start_time.strftime('%H:%M:%S')}")
    print("🎯 ONLY sending events that contain technology keywords!")
    print("🔄 Will timeout after 1 hour and restart automatically")
    
    try:
        # RUNS FOR 1 HOUR - then Airflow times out and restarts next hour
        while True:
            stats['cycles'] += 1
            
            # Fetch GitHub events
            url = 'https://api.github.com/events'
            
            try:
                response = requests.get(url, headers=headers, timeout=15)
                stats['api_calls'] += 1
                
                if response.status_code == 200:
                    events = response.json()
                    stats['total_events_fetched'] += len(events)
                    
                    events_sent_this_cycle = 0
                    
                    # Process each event for keywords
                    for event in events:
                        # Find keywords in this event
                        found_keywords = find_keywords_in_event(event, keywords, keywords_lower)
                        
                        # Only send to Kafka if keywords found
                        if found_keywords:
                            stats['events_with_keywords'] += 1
                            
                            # Update keyword statistics
                            for keyword in found_keywords:
                                stats['keyword_matches'][keyword] = stats['keyword_matches'].get(keyword, 0) + 1
                            
                            # Enhance event with our analysis
                            enhanced_event = {
                                **event,
                                'extracted_keywords': found_keywords,
                                'searchable_text': extract_searchable_text(event)[:300],
                                'filter_timestamp': datetime.utcnow().isoformat(),
                                'producer_version': '2.0_enhanced'
                            }
                            
                            # Send to Kafka
                            try:
                                producer.produce(topic, json.dumps(enhanced_event))
                                events_sent_this_cycle += 1
                                stats['events_sent_to_kafka'] += 1
                            except Exception as kafka_error:
                                print(f"❌ Kafka send error: {kafka_error}")
                    
                    # Flush to ensure delivery
                    producer.flush()
                    
                    # Enhanced logging
                    remaining = response.headers.get('X-RateLimit-Remaining', 'Unknown')
                    total_filtered = stats['total_events_fetched']
                    sent_total = stats['events_sent_to_kafka']
                    filter_rate = (sent_total / total_filtered * 100) if total_filtered > 0 else 0
                    
                    print(f"📡 Cycle {stats['cycles']}: {len(events)} fetched, "
                          f"{events_sent_this_cycle} sent | "
                          f"Total: {sent_total}/{total_filtered} ({filter_rate:.1f}%) | "
                          f"Rate limit: {remaining}")
                    
                    # Show top keywords every 10 cycles
                    if stats['cycles'] % 10 == 0 and stats['keyword_matches']:
                        top_keywords = sorted(stats['keyword_matches'].items(), 
                                            key=lambda x: x[1], reverse=True)[:5]
                        print(f"   🏆 Top keywords: {dict(top_keywords)}")
                
                elif response.status_code == 403:
                    print("❌ Rate limited! Waiting 60 seconds...")
                    time.sleep(60)
                    continue
                    
                else:
                    print(f"❌ HTTP {response.status_code}: {response.text[:100]}")
                
            except Exception as e:
                print(f"❌ Request error: {e}")
            
            # Wait 3 seconds before next poll
            print(f"😴 Waiting 3 seconds... (Cycle {stats['cycles']} complete)")
            time.sleep(3)
        
    except KeyboardInterrupt:
        print("🛑 Producer interrupted by user")
    except Exception as e:
        print(f"❌ Producer error: {e}")
    finally:
        # FIXED: Only use flush() - NO close() method exists
        try:
            print("🔄 Flushing final messages...")
            producer.flush()
            print("✅ Final flush completed")
        except Exception as flush_error:
            print(f"⚠️ Warning during final flush: {flush_error}")
        
        # Note: confluent-kafka Producer does NOT have a close() method
        
        runtime = datetime.now() - start_time
        
        print("=" * 60)
        print("📊 FINAL PRODUCTION SUMMARY:")
        print(f"   📥 Total events fetched: {stats['total_events_fetched']:,}")
        print(f"   ✅ Events with keywords: {stats['events_with_keywords']:,}")
        print(f"   📤 Events sent to Kafka: {stats['events_sent_to_kafka']:,}")
        print(f"   🎯 Filter success rate: {(stats['events_sent_to_kafka']/stats['total_events_fetched']*100):.1f}%" if stats['total_events_fetched'] > 0 else "   🎯 Filter success rate: 0%")
        print(f"   🔄 API calls made: {stats['api_calls']}")
        print(f"   ⏰ Runtime: {runtime}")
        print(f"   📡 Topic: {topic}")
        print(f"   🔄 Total cycles: {stats['cycles']}")
        
        if stats['keyword_matches']:
            print("   🏆 Top keywords found:")
            sorted_keywords = sorted(stats['keyword_matches'].items(), 
                                   key=lambda x: x[1], reverse=True)
            for keyword, count in sorted_keywords[:10]:
                print(f"      • {keyword}: {count} events")
        else:
            print("   ❌ No technology keywords found in any events")
            print("   💡 Consider broadening keyword list or checking GitHub API responses")
        
        print("=" * 60)
        
        return {
            'status': 'success',
            'total_events_fetched': stats['total_events_fetched'],
            'events_sent_to_kafka': stats['events_sent_to_kafka'],
            'events_with_keywords': stats['events_with_keywords'],
            'filter_rate': (stats['events_sent_to_kafka']/stats['total_events_fetched']*100) if stats['total_events_fetched'] > 0 else 0,
            'runtime_minutes': runtime.total_seconds() / 60,
            'keyword_matches': dict(sorted(stats['keyword_matches'].items(), 
                                         key=lambda x: x[1], reverse=True)[:10]),
            'cycles': stats['cycles']
        }


# DAG definition - SAME NAME, NO CHANGES
default_args = {
    'owner': 'data-engineering',
    "start_date": datetime(2025, 6, 23, tzinfo=timezone.utc),
    'email_on_failure': False,
    'retries': 1,  # Retry once if failure occurs
}

dag = DAG(
    dag_id='producer_github_events',
    default_args=default_args,
    description='GitHub events producer - hourly automatic restarts',
    schedule="0 0,1,2,3,5,6,7,8,9,10,12,13,14,15,16,17,18,19,20,21,22,23 * * *", 
    catchup=False,
    max_active_runs=1,
    tags=['github', 'kafka', 'producer', 'streaming', 'hourly-restart'],
)

produce_task = PythonOperator(
    task_id='produce_github_events',
    python_callable=produce_github_events,
    execution_timeout=timedelta(hours=1),  # 1-hour timeout for hourly restarts
    dag=dag,
)