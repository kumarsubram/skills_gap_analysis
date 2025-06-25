"""
FIXED GitHub Processing DAG - Single Run Sequential Processing
==============================================================

FIXES:
1. Single DAG run processes Silver → Gold → Analytics sequentially
2. Enhanced assessment integration for single-run workflow
3. Better debugging output and next steps guidance

Replace: dags/daily_github_processing_analytics.py
"""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Airflow imports
from airflow import DAG
from airflow.operators.python import PythonOperator

# Project imports
from include.utils.github_medallion_processing import (
    process_bronze_to_silver,
    process_silver_to_gold,
    assess_processing_status 
)
from include.utils.github_analytics_processing import (
    process_analytics_trends
)
from include.utils.github_table_initialization import ensure_all_github_tables_exist


def create_tables(**context):
    """Step 1: Create all required tables"""
    print("🏗️ CREATING TABLES FOR SINGLE RUN PROCESSING")
    print("=" * 45)
    
    try:
        result = ensure_all_github_tables_exist(include_analytics=True)
        
        if result['success']:
            print("✅ All tables created successfully")
            print(f"📊 Created {result['successful']}/{result['total']} tables")
            print("🚀 Ready for single-run Silver → Gold → Analytics processing")
            return {'status': 'success', 'message': 'Tables ready', 'tables_created': result['successful']}
        else:
            print(f"❌ Table creation partial: {result['successful']}/{result['total']}")
            return {'status': 'failed', 'message': 'Table creation incomplete', 'tables_created': result['successful']}
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return {'status': 'failed', 'error': str(e)}


def assess_what_to_process(**context):
    """Step 2: Single-run assessment for complete pipeline processing"""
    print("🧠 SINGLE RUN ASSESSMENT")
    print("=" * 30)
    
    try:
        # Use the enhanced assessment logic for single-run processing
        status = assess_processing_status(**context)
        
        dates_to_process = status.get('dates_to_process', [])
        strategy = status.get('strategy', 'unknown')
        
        print("\n📋 SINGLE RUN ASSESSMENT RESULTS:")
        print(f"   Strategy: {strategy}")
        print(f"   Dates to process: {len(dates_to_process)}")
        print(f"   Silver needed: {status.get('process_silver', False)}")
        print(f"   Gold needed: {status.get('process_gold', False)}")
        print(f"   Analytics needed: {status.get('process_analytics', False)}")
        
        if not dates_to_process and not status.get('process_analytics', False):
            print("⏩ No processing needed")
            return {
                'status': 'success',
                'dates_to_process': [],
                'process_silver': False,
                'process_gold': False,
                'process_analytics': False,
                'strategy': strategy,
                'message': 'No processing needed - all layers current'
            }
        
        # Show the single-run processing plan
        print("\n🎯 SINGLE RUN PROCESSING PLAN:")
        if status.get('process_silver', False):
            assessment_details = status.get('processing_info', {}).get('assessment_details', {})
            silver_dates = assessment_details.get('silver_dates', [])
            print(f"   🥈 Silver: Process {len(silver_dates)} dates")
        
        if status.get('process_gold', False):
            print("   🥇 Gold: Dynamic detection after Silver completion")
        
        if status.get('process_analytics', False):
            print("   📊 Analytics: Process all timeframes after Gold completion")
        
        print("🚀 Single execution will complete entire pipeline!")
        
        return {
            'status': 'success',
            'dates_to_process': dates_to_process,
            'process_silver': status['process_silver'],
            'process_gold': status['process_gold'], 
            'process_analytics': status['process_analytics'],
            'strategy': strategy,
            'total_dates': len(dates_to_process),
            'processing_info': status.get('processing_info', {}),
            'message': f'Single run: {strategy} - will process {len(dates_to_process)} dates'
        }
        
    except Exception as e:
        print(f"❌ Assessment failed: {e}")
        import traceback
        traceback.print_exc()
        
        # Return safe defaults that allow manual processing
        return {
            'status': 'failed', 
            'error': str(e),
            'dates_to_process': [],
            'process_silver': False,
            'process_gold': False,
            'process_analytics': False,
            'strategy': 'error',
            'message': f'Assessment error: {str(e)}'
        }


def run_silver_processing(**context):
    """Step 3: Silver processing for single-run pipeline"""
    print("🥈 SINGLE RUN SILVER PROCESSING")
    print("=" * 40)
    
    try:
        # Get assessment from previous task
        ti = context['task_instance']
        assessment = ti.xcom_pull(task_ids='assess_what_to_process')
        
        if not assessment:
            print("❌ No assessment data from previous task")
            return {'status': 'failed', 'error': 'No assessment data'}
        
        if not assessment.get('process_silver', False):
            print("⏩ Silver processing skipped")
            print(f"   Reason: {assessment.get('message', 'Not needed per assessment')}")
            return {'status': 'skipped', 'reason': 'not_needed_per_assessment'}
        
        strategy = assessment.get('strategy', 'unknown')
        print(f"🔄 Single-run strategy: {strategy}")
        print("📂 Processing Silver gaps for complete pipeline")
        
        # Use enhanced Silver processing
        result = process_bronze_to_silver(**context)
        
        print("\n📊 SILVER PROCESSING RESULT:")
        print(f"   Status: {result['status']}")
        
        if result['status'] == 'success':
            dates_processed = result.get('dates_processed', [])
            technologies = result.get('total_technologies', 0)
            mentions = result.get('total_mentions', 0)
            print(f"   ✅ Silver complete: {len(dates_processed)} dates, {technologies} technologies")
            print(f"   📈 Total mentions: {mentions:,}")
            print("   🔄 Gold will now detect and process gaps including these new dates")
        elif result['status'] == 'skipped':
            print(f"   ⏩ Silver skipped: {result.get('reason', 'unknown')}")
        else:
            print(f"   ❌ Silver failed: {result.get('error', 'unknown')}")
        
        return result
        
    except Exception as e:
        print(f"❌ Silver processing failed: {e}")
        import traceback
        traceback.print_exc()
        return {'status': 'failed', 'error': str(e)}


def run_gold_processing(**context):
    """Step 4: Gold processing with dynamic detection for single-run pipeline"""
    print("🥇 SINGLE RUN GOLD PROCESSING")
    print("=" * 35)
    
    try:
        # Get results from previous tasks
        ti = context['task_instance']
        assessment = ti.xcom_pull(task_ids='assess_what_to_process')
        silver_result = ti.xcom_pull(task_ids='run_silver_processing')
        
        print("🧠 SINGLE RUN COORDINATION:")
        print(f"   Assessment strategy: {assessment.get('strategy', 'unknown') if assessment else 'none'}")
        print(f"   Silver result: {silver_result.get('status', 'unknown') if silver_result else 'none'}")
        
        if not assessment:
            print("❌ No assessment data - cannot proceed")
            return {'status': 'failed', 'error': 'No assessment data'}
        
        if not assessment.get('process_gold', False):
            print("⏩ Gold processing skipped per assessment")
            return {'status': 'skipped', 'reason': 'not_needed_per_assessment'}
        
        print("\n🔄 DYNAMIC GOLD PROCESSING")
        print("📋 Will detect all Gold gaps after Silver completion")
        
        # The Gold processing function will dynamically detect what needs processing
        result = process_silver_to_gold(**context)
        
        print(f"\n📊 GOLD RESULT: {result.get('status')}")
        if result.get('status') == 'success':
            processed = len(result.get('dates_processed', []))
            technologies = result.get('technologies_classified', 0)
            print(f"   ✅ {processed} dates, {technologies} technologies processed")
            print("   🔄 Analytics will now process with complete Gold data")
        elif result.get('status') == 'skipped':
            print(f"   ⏩ Gold skipped: {result.get('reason', 'unknown')}")
        
        return result
        
    except Exception as e:
        print(f"❌ Gold processing failed: {e}")
        import traceback
        traceback.print_exc()
        return {'status': 'failed', 'error': str(e)}


def run_analytics_processing(**context):
    """Step 5: Analytics processing for complete single-run pipeline"""
    print("📊 SINGLE RUN ANALYTICS PROCESSING")
    print("=" * 40)
    
    try:
        # Get results from previous tasks
        ti = context['task_instance']
        assessment = ti.xcom_pull(task_ids='assess_what_to_process')
        gold_result = ti.xcom_pull(task_ids='run_gold_processing')
        
        print("🧠 ANALYTICS COORDINATION:")
        print(f"   Assessment strategy: {assessment.get('strategy', 'unknown') if assessment else 'none'}")
        print(f"   Gold result: {gold_result.get('status', 'unknown') if gold_result else 'none'}")
        
        if not assessment:
            print("❌ No assessment data - cannot proceed")
            return {'status': 'failed', 'error': 'No assessment data'}
        
        if not assessment.get('process_analytics', False):
            print("⏩ Analytics processing skipped per assessment")
            return {'status': 'skipped', 'reason': 'not_needed_per_assessment'}
        
        print("\n🧠 RUNNING ANALYTICS with complete medallion data")
        print("📊 Processing all timeframes with latest Gold data")
        
        # Use the existing Analytics processing
        result = process_analytics_trends(**context)
        
        print(f"\n📊 ANALYTICS RESULT: {result.get('status')}")
        if result.get('status') in ['success', 'partial_success']:
            timeframes = result.get('timeframes_processed', 0)
            total = result.get('total_timeframes', 0)
            print(f"   ✅ {timeframes}/{total} analytics timeframes processed")
            print("   🎉 Single-run pipeline complete!")
        elif result.get('status') == 'skipped':
            print(f"   ⏩ Analytics skipped: {result.get('reason', 'unknown')}")
        
        return result
        
    except Exception as e:
        print(f"❌ Analytics processing failed: {e}")
        import traceback
        traceback.print_exc()
        return {'status': 'failed', 'error': str(e)}


def show_final_summary(**context):
    """Step 6: Final summary for single-run processing"""
    print("📊 SINGLE RUN FINAL SUMMARY")
    print("=" * 35)
    
    try:
        # Get results from all previous tasks
        ti = context['task_instance']
        
        assessment = ti.xcom_pull(task_ids='assess_what_to_process')
        silver_result = ti.xcom_pull(task_ids='run_silver_processing')
        gold_result = ti.xcom_pull(task_ids='run_gold_processing')
        analytics_result = ti.xcom_pull(task_ids='run_analytics_processing')
        
        # Handle case where assessment failed
        if not assessment:
            print("❌ No assessment data - cannot generate summary")
            return {'status': 'failed', 'message': 'No assessment data available'}
        
        strategy = assessment.get('strategy', 'unknown')
        print(f"🎯 Single-run strategy: {strategy}")
        
        # Determine what was supposed to happen
        should_process_silver = assessment.get('process_silver', False)
        should_process_gold = assessment.get('process_gold', False)
        should_process_analytics = assessment.get('process_analytics', False)
        
        if not should_process_silver and not should_process_gold and not should_process_analytics:
            print("\n⏩ NO PROCESSING OCCURRED - System up to date")
            print("✅ All medallion layers are current")
            print("📊 No gaps detected in Bronze → Silver → Gold → Analytics")
            return {'status': 'success', 'message': 'No processing needed - system current'}
        
        print("\n🔄 SINGLE RUN PROCESSING RESULTS:")
        
        # Show processing results
        results = {
            '🥈 Silver': (silver_result, should_process_silver),
            '🥇 Gold': (gold_result, should_process_gold),
            '📊 Analytics': (analytics_result, should_process_analytics)
        }
        
        successful_layers = 0
        total_expected_layers = sum(1 for _, should_process in results.values() if should_process)
        
        for layer_name, (result, should_process) in results.items():
            if should_process:
                if result and result.get('status') in ['success', 'partial_success']:
                    if layer_name == '🥈 Silver':
                        count = len(result.get('dates_processed', []))
                        print(f"   {layer_name}: ✅ {count} dates processed")
                    elif layer_name == '🥇 Gold':
                        count = len(result.get('dates_processed', []))
                        print(f"   {layer_name}: ✅ {count} dates processed")
                    elif layer_name == '📊 Analytics':
                        timeframes = result.get('timeframes_processed', 0)
                        total = result.get('total_timeframes', 0)
                        if result.get('status') == 'partial_success':
                            print(f"   {layer_name}: 🔄 {timeframes}/{total} timeframes (partial)")
                        else:
                            print(f"   {layer_name}: ✅ {timeframes}/{total} timeframes")
                    successful_layers += 1
                elif result and result.get('status') == 'skipped':
                    reason = result.get('reason', 'unknown')
                    print(f"   {layer_name}: ⏩ Skipped ({reason})")
                elif result and result.get('status') == 'blocked':
                    reason = result.get('reason', 'unknown')
                    print(f"   {layer_name}: 🚫 Blocked ({reason})")
                else:
                    error = result.get('error', 'unknown') if result else 'no result'
                    print(f"   {layer_name}: ❌ Failed ({error})")
            else:
                print(f"   {layer_name}: ⏭️ Not needed")
        
        # Determine overall status
        if successful_layers == total_expected_layers and total_expected_layers > 0:
            overall_status = "🎉 SINGLE RUN SUCCESS"
        elif successful_layers >= 1:
            overall_status = f"🔄 PARTIAL SUCCESS ({successful_layers}/{total_expected_layers} layers)"
        else:
            overall_status = "❌ PROCESSING ISSUES"
        
        print(f"\n🎯 OVERALL STATUS: {overall_status}")
        print(f"⏰ Completed: {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC")
        print("🚀 Single-run architecture: Silver → Gold → Analytics in one execution")
        
        # Next steps guidance
        print("\n💡 NEXT STEPS:")
        if strategy == 'single_run_complete_pipeline':
            if successful_layers == total_expected_layers:
                print("   🎉 Single-run pipeline complete!")
                print("   📊 All medallion layers processed successfully")
                print("   🔄 Run Bronze pipeline to generate new data")
            else:
                print("   🔧 Check failed layers and re-run DAG")
                print("   📋 Review error logs for specific issues")
        elif strategy == 'single_run_analytics_only':
            if analytics_result and analytics_result.get('status') in ['success', 'partial_success']:
                print("   🎉 Analytics refresh complete!")
                print("   📊 All analytics timeframes up to date")
            else:
                print("   🔧 Check analytics processing and re-run")
        elif strategy == 'manual_full_pipeline':
            print("   ✅ Manual processing complete")
            print("   🔄 Run without config for automatic processing")
        else:
            print("   🔄 Re-run DAG for continued processing")
        
        return {
            'status': 'success',
            'strategy': strategy,
            'overall_status': overall_status,
            'successful_layers': successful_layers,
            'expected_layers': total_expected_layers,
            'processing_complete': successful_layers == total_expected_layers,
            'architecture': 'single_run_sequential',
            'layer_results': {k: v[0] for k, v in results.items()},
            'assessment': assessment
        }
        
    except Exception as e:
        print(f"❌ Summary failed: {e}")
        import traceback
        traceback.print_exc()
        return {'status': 'failed', 'error': str(e)}


# DAG Definition - SAME NAME: daily_github_processing_analytics
default_args = {
    "owner": "data-engineering",
    "start_date": datetime(2025, 6, 20, tzinfo=timezone.utc),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

dag = DAG(
    dag_id="daily_github_processing_analytics",  # SAME NAME - NEVER CHANGE
    default_args=default_args,
    description="GitHub Processing: Single-Run Sequential (Silver → Gold → Analytics)",
    schedule=None,  # Manual trigger
    catchup=False,
    max_active_runs=1,
    tags=["github", "medallion", "analytics", "single-run", "sequential"],
    doc_md="""
    # GitHub Processing Pipeline - Single Run Sequential Processing
    
    **FIXED for single-run execution: Silver → Gold → Analytics**
    
    ## Processing Flow:
    1. **Tables**: Create all required tables
    2. **Assessment**: Find ALL missing data across layers
    3. **Silver**: Process ALL missing Silver dates  
    4. **Gold**: Dynamically detect and process ALL Gold gaps (including new Silver data)
    5. **Analytics**: Process all timeframes with complete Gold data
    6. **Summary**: Show complete pipeline results
    
    ## Key Features:
    - ✅ Single DAG run completes entire pipeline
    - ✅ Dynamic detection of Gold gaps after Silver completion
    - ✅ No multi-run stupidity - everything in one execution
    - ✅ Clear progress tracking and next steps
    
    ## Usage:
    - **Automatic**: Finds and processes all missing data in one run
    - **Manual date**: `{"date": "2025-01-03"}` processes that date through entire pipeline
    
    ## Architecture:
    Bronze (existing) → Silver (clean) → Gold (classified) → Analytics (insights)
    All processed sequentially in single DAG execution!
    """,
)

# Define Tasks - SAME NAMES, SINGLE-RUN FUNCTIONALITY
with dag:
    
    # Step 1: Create tables
    create_tables_task = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables
    )
    
    # Step 2: Single-run assessment
    assess_task = PythonOperator(
        task_id="assess_what_to_process",
        python_callable=assess_what_to_process
    )
    
    # Step 3: Silver processing (first phase)
    silver_task = PythonOperator(
        task_id="run_silver_processing",
        python_callable=run_silver_processing
    )
    
    # Step 4: Gold processing (dynamic detection after Silver)
    gold_task = PythonOperator(
        task_id="run_gold_processing",
        python_callable=run_gold_processing
    )
    
    # Step 5: Analytics processing (after complete medallion)
    analytics_task = PythonOperator(
        task_id="run_analytics_processing",
        python_callable=run_analytics_processing
    )
    
    # Step 6: Single-run final summary
    summary_task = PythonOperator(
        task_id="show_final_summary",
        python_callable=show_final_summary
    )

# Task Dependencies - SAME STRUCTURE, SINGLE RUN FLOW
create_tables_task >> assess_task >> silver_task >> gold_task >> analytics_task >> summary_task