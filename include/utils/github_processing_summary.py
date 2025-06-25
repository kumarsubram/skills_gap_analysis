"""
Smart Processing Summary and Reporting Functions for GitHub Medallion Pipeline

🧠 FIXED: Table naming consistency - uses same Gold table name as all other files
No manual configuration needed - truly idempotent behavior.

Place at: include/utils/github_processing_summary.py
"""

from datetime import datetime, timezone
from typing import Dict
from deltalake import DeltaTable

from include.utils.delta_table_utils import (
    get_enterprise_table_path,
    get_minio_storage_options
)
from include.utils.github_medallion_processing import assess_processing_status
from include.utils.github_date_processing import get_processing_dates
from include.utils.github_table_initialization import ensure_github_silver_gold_tables_exist

# FIXED: Use SAME table name as all other files
GOLD_TABLE_NAME = "technology_daily_activity"


def ensure_tables_ready(**context) -> Dict:
    """Ensure Silver and Gold tables exist with proper schemas"""
    print("🏗️ ENSURING MEDALLION TABLES READY")
    print("=" * 40)
    print("✅ Using corrected schemas with business domain focus")
    print("✅ Gold layer: Business classifications (not statistics)")
    
    try:
        result = ensure_github_silver_gold_tables_exist()
        
        if result['success']:
            print("✅ All medallion tables ready with corrected schemas")
            print("🎯 Ready for Bronze → Silver → Gold processing")
            return {
                'status': 'success',
                'tables_ready': True,
                'message': 'Medallion architecture tables ready'
            }
        else:
            print("❌ Table initialization failed")
            print("💡 Check MinIO connection and table creation permissions")
            return {
                'status': 'failed',
                'tables_ready': False,
                'message': 'Could not initialize medallion tables'
            }
            
    except Exception as e:
        print(f"❌ Table readiness check failed: {e}")
        return {
            'status': 'failed',
            'tables_ready': False,
            'error': str(e),
            'message': f'Table check error: {str(e)}'
        }


def generate_business_intelligence_insights(gold_df, date_str: str) -> None:
    """Generate detailed business intelligence insights from Gold data"""
    if len(gold_df) == 0:
        print("   ⚠️  No Gold data available for insights")
        return
    
    print("\n🏛️ BUSINESS INTELLIGENCE INSIGHTS:")
    
    # Category breakdown
    top_categories = gold_df['technology_category'].value_counts().head(5)
    print("   📂 Top Categories:")
    for category, count in top_categories.items():
        print(f"      • {category}: {count} technologies")
    
    # FIXED: Use simplified Gold schema fields (no complex business fields)
    # Volume breakdown
    if 'mention_volume' in gold_df.columns:
        volume_counts = gold_df['mention_volume'].value_counts()
        print("   📊 Mention Volumes:")
        for volume, count in volume_counts.items():
            print(f"      • {volume}: {count} technologies")
    
    # Data completeness
    if 'data_completeness' in gold_df.columns:
        avg_completeness = gold_df['data_completeness'].apply(
            lambda x: 'high' if x == 'complete' else x
        ).value_counts()
        print("   📋 Data Quality:")
        for quality, count in avg_completeness.items():
            print(f"      • {quality}: {count} technologies")
    
    # Top technologies by mentions
    top_techs = gold_df.nlargest(5, 'daily_mentions')
    print("   🔥 Top Technologies by Mentions:")
    for _, row in top_techs.iterrows():
        category_emoji = {
            'ai_model': '🤖', 'ai_framework': '🧠', 'language': '💻',
            'framework': '🌐', 'database': '🗄️', 'cloud': '☁️'
        }.get(row['technology_category'], '🔧')
        print(f"      {category_emoji} {row['technology']}: {row['daily_mentions']} mentions ({row['technology_category']})")
    
    # AI focus (hot category)
    ai_techs = gold_df[gold_df['technology_category'].str.startswith('ai')].nlargest(3, 'daily_mentions')
    if len(ai_techs) > 0:
        print("   🤖 AI/ML Technologies:")
        for _, row in ai_techs.iterrows():
            print(f"      • {row['technology']}: {row['daily_mentions']} mentions → {row['mention_volume']} volume")


def generate_smart_next_steps(processing_info: Dict) -> None:
    """🧠 Generate intelligent next steps based on what was processed"""
    strategy = processing_info.get('strategy', 'unknown')
    
    if strategy == 'auto_silver':
        remaining_silver = len(processing_info.get('missing_silver_dates', [])) - 1
        remaining_gold = len(processing_info.get('missing_gold_dates', []))
        
        print("\n🔄 NEXT STEPS AFTER SILVER PROCESSING:")
        if remaining_silver > 0:
            print(f"   📅 Run again to process {remaining_silver} more Silver dates")
        if remaining_gold > 0:
            print(f"   🥇 Then process {remaining_gold} Gold dates")
        if remaining_silver == 0 and remaining_gold == 0:
            print("   🎉 All Silver processing complete! Gold will process next run.")
            
    elif strategy == 'auto_gold':
        remaining_gold = len(processing_info.get('missing_gold_dates', [])) - 1
        
        print("\n🔄 NEXT STEPS AFTER GOLD PROCESSING:")
        if remaining_gold > 0:
            print(f"   📅 Run again to process {remaining_gold} more Gold dates")
        else:
            print("   🎉 All Gold processing complete! Medallion architecture current.")
            
    elif strategy == 'up_to_date':
        print("\n✅ SYSTEM STATUS: UP TO DATE")
        print("   🎯 All medallion layers are current")
        print("   📊 Ready for analytics and reporting")
        print("   🔄 Run Bronze pipeline to generate new data")
        
    elif strategy == 'manual':
        print("\n📅 MANUAL PROCESSING COMPLETE")
        print("   🔄 Run without config for automatic processing")
        print("   📊 Or specify another date for manual processing")
        
    else:
        print("\n💡 GENERAL SUGGESTIONS:")
        print("   🔄 Re-run DAG for automatic processing")
        print("   📊 Check data quality and completeness")


def generate_processing_overview(processing_info: Dict) -> None:
    """Generate overview of what processing occurred"""
    strategy = processing_info.get('strategy', 'unknown')
    
    strategy_explanations = {
        'auto_silver': '🥈 Automatically detected Bronze→Silver gaps and processed most recent',
        'auto_gold': '🥇 Automatically detected Silver→Gold gaps and processed most recent', 
        'up_to_date': '✅ All layers current - no processing needed',
        'manual': '📅 Manual date processing',
        'no_data': '❌ No Bronze data available'
    }
    
    explanation = strategy_explanations.get(strategy, f'🔧 Strategy: {strategy}')
    print("\n🧠 SMART PROCESSING OVERVIEW:")
    print(f"   {explanation}")
    
    if 'bronze_dates' in processing_info:
        bronze_count = len(processing_info['bronze_dates'])
        silver_count = len(processing_info.get('silver_dates', []))
        gold_count = len(processing_info.get('gold_dates', []))
        
        print(f"   📊 Data Landscape: {bronze_count} Bronze → {silver_count} Silver → {gold_count} Gold")


def generate_final_summary(**context) -> Dict:
    """🧠 SMART Final Summary: Automatically detects what was processed and what's next"""
    try:
        status = assess_processing_status(**context)
        date_str = status['date']
        processing_info = status.get('processing_info', {})
        
        print("\n📊 SMART PROCESSING SUMMARY")
        print("=" * 55)
        print("🧠 Mode: INTELLIGENT AUTO-DISCOVERY")
        print(f"🎯 Date: {date_str or 'None (no processing needed)'}")
        print(f"⏰ Completed: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
        print("✅ Enhanced medallion architecture with business intelligence")
        
        # Show what the smart system detected and did
        generate_processing_overview(processing_info)
        
        # Handle case where no processing was needed
        if date_str is None:
            strategy = processing_info.get('strategy', 'unknown')
            if strategy == 'up_to_date':
                print("\n🎉 RESULT: ALL MEDALLION LAYERS CURRENT")
                print("📋 Bronze, Silver, and Gold are all up to date")
                print("\n💡 SYSTEM STATUS:")
                print("   ✅ Medallion architecture complete")
                print("   📊 Ready for analytics and dashboards")
                print("   🔄 Run Bronze pipeline to process new data")
                print("   📈 Consider setting up automated scheduling")
            else:
                print("\n⏩ RESULT: NO PROCESSING NEEDED")
                print(f"📋 {processing_info.get('reason', 'Unknown reason')}")
                
            return {
                'status': 'success',
                'date': None,
                'pipeline_status': '✅ UP TO DATE' if strategy == 'up_to_date' else '⏩ NO ACTION NEEDED',
                'message': processing_info.get('reason', 'No processing required'),
                'processing_info': processing_info,
                'completed_at': datetime.now(timezone.utc).isoformat()
            }
        
        # Medallion status overview
        print("\n🏗️ MEDALLION ARCHITECTURE STATUS:")
        layers_status = [
            ("🥉 Bronze", status['bronze_count'], status['bronze_valid']),
            ("🥈 Silver", status['silver_count'], status['silver_complete']), 
            ("🥇 Gold", status['gold_count'], status['gold_complete'])
        ]
        
        for layer_name, count, complete in layers_status:
            status_icon = "✅" if complete else "❌"
            print(f"   {layer_name}: {count:,} records {status_icon}")
        
        # Enhanced business insights for Gold
        if status['gold_complete'] and status['gold_count'] > 0:
            try:
                # FIXED: Use consistent Gold table name
                gold_path = get_enterprise_table_path(GOLD_TABLE_NAME, "gold")
                storage_options = get_minio_storage_options()
                gold_dt = DeltaTable(gold_path, storage_options=storage_options)
                gold_df = gold_dt.to_pandas(filters=[('date', '=', date_str)])
                
                generate_business_intelligence_insights(gold_df, date_str)
                        
            except Exception as e:
                print(f"   ⚠️  Could not load detailed Gold insights: {e}")
        
        # Overall pipeline status
        if status['bronze_valid'] and status['silver_complete'] and status['gold_complete']:
            pipeline_status = "✅ COMPLETE"
            message = "Full medallion processing successful"
        elif status['bronze_valid'] and status['silver_complete']:
            pipeline_status = "🔄 SILVER COMPLETE"
            message = "Silver complete, Gold in progress"
        elif status['bronze_valid']:
            pipeline_status = "🔄 BRONZE READY"
            message = "Bronze ready, processing needed"
        else:
            pipeline_status = "❌ BLOCKED"
            message = "Bronze data issues blocking pipeline"
        
        print(f"\n🎯 PIPELINE STATUS: {pipeline_status}")
        print(f"📋 Message: {message}")
        print("🏛️ Architecture: Bronze (raw) → Silver (clean) → Gold (business intelligence)")
        
        # Intelligent next steps
        generate_smart_next_steps(processing_info)
        
        print("\n🚀 Ready for: Analytics layer with statistical analysis + streaming integration")
        
        return {
            'status': 'success',
            'date': date_str,
            'pipeline_status': pipeline_status,
            'bronze_valid': status['bronze_valid'],
            'bronze_count': status['bronze_count'],
            'silver_complete': status['silver_complete'],
            'silver_count': status['silver_count'],
            'gold_complete': status['gold_complete'],
            'gold_count': status['gold_count'],
            'processing_info': processing_info,
            'message': message,
            'completed_at': datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        print(f"❌ Summary generation failed: {e}")
        processing_info = get_processing_dates(**context)
        return {
            'status': 'failed',
            'error': str(e),
            'date': processing_info.get('target_date'),
            'processing_info': processing_info,
            'message': f'Summary error: {str(e)}'
        }