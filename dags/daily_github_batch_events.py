"""
GitHub Batch Events Daily DAG

One task processes one date: Download → Extract → Aggregate → Cleanup
Simple, idempotent, with detailed logging.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone

# Import GitHub processing components
from include.github.github_downloader import download_github_date
from include.github.github_keyword_extractor import extract_keywords_for_date
from include.github.github_aggregator import aggregate_and_save_to_silver
from include.config.env_detection import ENV
from include.delta_lake.file_manager import delete_files_from_layer, file_exists

# CONFIGURATION
START_DATE = "2024-09-01"
SKIP_START = "2024-12-28"  # Optional: "2024-12-31" or None
SKIP_END = "2025-06-07"    # Optional: "2025-06-06" or None


def get_next_unprocessed_date():
    """Find the next date that needs processing"""
    start_date = datetime.strptime(START_DATE, "%Y-%m-%d")
    yesterday = datetime.now() - timedelta(days=1)

    skip_start = datetime.strptime(SKIP_START, "%Y-%m-%d") if SKIP_START else None
    skip_end = datetime.strptime(SKIP_END, "%Y-%m-%d") if SKIP_END else None

    current_date = start_date
    while current_date <= yesterday:
        if skip_start and skip_end and skip_start <= current_date <= skip_end:
            current_date += timedelta(days=1)
            continue

        date_str = current_date.strftime("%Y-%m-%d")

        silver_checks = [
            file_exists(f"tech_trends_summary_{date_str}.parquet", 'github', 'silver/tech_trends_summary'),
            file_exists(f"top_repos_{date_str}.parquet", 'github', 'silver/top_repos'),
            file_exists(f"event_summary_{date_str}.parquet", 'github', 'silver/event_summary'),
            file_exists(f"hourly_trends_{date_str}.parquet", 'github', 'silver/hourly_trends')
        ]

        existing_count = sum(1 for exists in silver_checks if exists)

        if existing_count < 4:
            return date_str

        current_date += timedelta(days=1)

    return None


def cleanup_bronze_files(date_str):
    """Clean up Bronze files for a specific date"""
    gz_files = [f"{date_str}-{hour:02d}.json.gz" for hour in range(24)]
    delete_files_from_layer(gz_files, 'github', 'bronze', force_cleanup=True)

    parquet_files = [f"keywords-{date_str}-hour-{hour:02d}.parquet" for hour in range(24)]
    delete_files_from_layer(parquet_files, 'github', 'bronze', force_cleanup=True)

    print(f"🗑️  Cleaned 48 Bronze files for {date_str}")


def process_next_date(**context):
    """Process the next unprocessed date completely"""
    print("🚀 GITHUB BATCH PROCESSING")
    print("=" * 60)
    print(f"Environment: {ENV.environment.upper()}")
    print(f"Start Date: {START_DATE}")

    if SKIP_START and SKIP_END:
        print(f"Skip Range: {SKIP_START} to {SKIP_END}")
    else:
        print("Skip Range: None")

    next_date = get_next_unprocessed_date()

    if not next_date:
        print("\n✅ ALL DATES COMPLETE!")
        print("🏁 No more dates to process.")
        return {'status': 'all_complete'}

    print(f"\n🎯 Target Date: {next_date}")

    silver_checks = [
        file_exists(f"tech_trends_summary_{next_date}.parquet", 'github', 'silver/tech_trends_summary'),
        file_exists(f"top_repos_{next_date}.parquet", 'github', 'silver/top_repos'),
        file_exists(f"event_summary_{next_date}.parquet", 'github', 'silver/event_summary'),
        file_exists(f"hourly_trends_{next_date}.parquet", 'github', 'silver/hourly_trends')
    ]
    existing_count = sum(1 for exists in silver_checks if exists)

    if existing_count > 0:
        print(f"⚠️  {next_date} PARTIALLY COMPLETE: {existing_count}/4 Silver files")
        print("🔄 Will complete processing")
    else:
        print(f"📝 {next_date} READY FOR PROCESSING: {existing_count}/4 Silver files")

    try:
        print("\n" + "=" * 60)
        print(f"📥 STEP 1: DOWNLOADING {next_date}")
        print("=" * 60)

        download_result = download_github_date(next_date)
        print("📊 Download Results:")
        print(f"   ✅ Successful: {download_result['successful_downloads']}")
        print(f"   ♻️  Cached: {download_result['cached_files']}")
        print(f"   ❌ Failed: {download_result['failed_downloads']}")
        if download_result['failed_downloads'] > 0:
            print(f"   🔴 Failed hours: {download_result.get('failed_hours', [])}")

        print("\n" + "=" * 60)
        print(f"🔍 STEP 2: EXTRACTING KEYWORDS FOR {next_date}")
        print("=" * 60)

        extract_result = extract_keywords_for_date(next_date)
        print("📊 Extract Results:")
        print(f"   ✅ Successful: {extract_result['successful_extractions']}")
        print(f"   ❌ Failed: {extract_result['failed_extractions']}")
        if extract_result['failed_extractions'] > 0:
            print(f"   🔴 Failed hours: {extract_result.get('failed_hours', [])}")

        print("\n" + "=" * 60)
        print(f"📊 STEP 3: AGGREGATING TO SILVER FOR {next_date}")
        print("=" * 60)

        aggregate_result = aggregate_and_save_to_silver(next_date)
        if not aggregate_result['success']:
            reason = aggregate_result.get('reason', 'unknown')
            print(f"❌ AGGREGATION FAILED: {reason}")
            print("🔒 Bronze files preserved for retry")
            return {'status': 'failed', 'date': next_date, 'stage': 'aggregate', 'reason': reason}

        print("📊 Aggregate Results:")
        print(f"   ✅ Files saved: {aggregate_result['files_saved']}/4")
        print(f"   📈 Keywords: {aggregate_result['total_keywords']}")
        print(f"   💬 Mentions: {aggregate_result['total_mentions']:,}")

        print("\n" + "=" * 60)
        print(f"🗑️  STEP 4: CLEANING BRONZE FOR {next_date}")
        print("=" * 60)

        cleanup_bronze_files(next_date)

        print("\n" + "=" * 60)
        print(f"✅ FINAL VALIDATION FOR {next_date}")
        print("=" * 60)

        final_silver_checks = [
            file_exists(f"tech_trends_summary_{next_date}.parquet", 'github', 'silver/tech_trends_summary'),
            file_exists(f"top_repos_{next_date}.parquet", 'github', 'silver/top_repos'),
            file_exists(f"event_summary_{next_date}.parquet", 'github', 'silver/event_summary'),
            file_exists(f"hourly_trends_{next_date}.parquet", 'github', 'silver/hourly_trends')
        ]
        final_silver_count = sum(1 for exists in final_silver_checks if exists)

        gz_files = [f"{next_date}-{hour:02d}.json.gz" for hour in range(24)]
        parquet_files = [f"keywords-{next_date}-hour-{hour:02d}.parquet" for hour in range(24)]
        remaining_gz = sum(1 for f in gz_files if file_exists(f, 'github', 'bronze'))
        remaining_parquet = sum(1 for f in parquet_files if file_exists(f, 'github', 'bronze'))

        print("📊 Validation Results:")
        print(f"   📁 Silver files: {final_silver_count}/4")
        print(f"   🗑️  Remaining GZ: {remaining_gz}/24")
        print(f"   🗑️  Remaining Parquet: {remaining_parquet}/24")

        success = (final_silver_count == 4) and (remaining_gz == 0) and (remaining_parquet == 0)

        if success:
            print(f"\n🎉 SUCCESS: {next_date} PROCESSED COMPLETELY!")
            print("💾 Freed ~1-2GB disk space")
            next_next_date = get_next_unprocessed_date()
            if next_next_date:
                print(f"🎯 Next run will process: {next_next_date}")
            else:
                print("🏁 ALL DATES WILL BE COMPLETE!")
            print("=" * 60)
            return {'status': 'success', 'date': next_date, 'silver_files': final_silver_count, 'next_date': next_next_date}
        else:
            raise Exception(f"Validation failed for {next_date}")

    except Exception as e:
        print(f"\n❌ ERROR PROCESSING {next_date}: {str(e)}")
        print("🔒 Bronze files preserved for retry")
        print("=" * 60)
        return {'status': 'error', 'date': next_date, 'error': str(e)}


def process_all_pending_dates(**context):
    """Loop over and process all unprocessed dates in one DAG run"""
    while True:
        result = process_next_date(**context)
        if result.get("status") in ("all_complete", "error", "failed"):
            return result


# DAG Definition
default_args = {
    "owner": "data-engineering",
    "start_date": datetime(2024, 10, 1, tzinfo=timezone.utc),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="daily_github_batch_events",
    default_args=default_args,
    description=f"Heavy GitHub processing from {START_DATE}",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["github", "batch", "heavy"],
) as dag:

    process_all = PythonOperator(
        task_id="process_all_pending_dates",
        python_callable=process_all_pending_dates,
        pool='batch_heavy',        # 🎯 Gets 64 dedicated slots
        priority_weight=1,         # LOW priority (runs when resources available)
        execution_timeout=timedelta(hours=6),
    )
