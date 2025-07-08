"""
FIXED GitHub Date Processing - Single Run Sequential Processing
===============================================================

FIXES:
1. Single DAG run processes Silver → Gold → Analytics sequentially
2. Finds ALL missing dates and processes them in one execution
3. Maintains all existing functionality but removes multi-run stupidity
4. Assessment returns ALL dates that need processing across layers

Replace: include/utils/github_date_processing.py
"""

from typing import Dict, List
from deltalake import DeltaTable

from include.utils.delta_table_utils import (
    get_enterprise_table_path,
    get_minio_storage_options,
    check_table_exists
)

# FIXED: Consistent table naming (must match across ALL files)
BRONZE_TABLE = "keyword_extractions"
SILVER_TABLE = "keyword_trends"
GOLD_TABLE = "technology_daily_activity"  # ← This MUST be same everywhere


def get_available_dates_for_layer(table_name: str, layer: str) -> List[str]:
    """Get all available dates from a specific layer"""
    try:
        if not check_table_exists(table_name, layer, "github"):
            print(f"📅 {layer.title()} table '{table_name}' doesn't exist")
            return []
        
        table_path = get_enterprise_table_path(table_name, layer)
        storage_options = get_minio_storage_options()
        
        dt = DeltaTable(table_path, storage_options=storage_options)
        df = dt.to_pandas()
        
        if 'date' not in df.columns:
            print(f"❌ No date column in {layer.title()} table")
            return []
        
        available_dates = sorted(df['date'].unique().tolist())
        date_range = f"from {available_dates[0]} to {available_dates[-1]}" if available_dates else "None"
        print(f"📊 {layer.title()} dates: {len(available_dates)} ({date_range})")
        return available_dates
        
    except Exception as e:
        print(f"❌ Error getting {layer.title()} dates: {e}")
        return []


def find_missing_dates_between_layers(source_layer: str, target_layer: str, 
                                    source_table: str, target_table: str) -> List[str]:
    """Find dates that exist in source but are missing in target - TRUE IDEMPOTENCY"""
    source_dates = set(get_available_dates_for_layer(source_table, source_layer))
    target_dates = set(get_available_dates_for_layer(target_table, target_layer))
    
    missing_dates = sorted(source_dates - target_dates)
    
    if missing_dates:
        print(f"🔍 Missing {target_layer.title()} dates: {len(missing_dates)}")
        print(f"   Examples: {missing_dates[:3]}{'...' if len(missing_dates) > 3 else ''}")
        if len(missing_dates) <= 10:
            print(f"   All missing: {missing_dates}")
    else:
        print(f"✅ {target_layer.title()} is up to date with {source_layer.title()}")
    
    return missing_dates


def assess_processing_needs_single_run(**context) -> Dict:
    """SINGLE RUN: Find all missing data and process Silver → Gold → Analytics in ONE execution"""
    print("🚀 SINGLE RUN ASSESSMENT - Silver → Gold → Analytics")
    print("=" * 55)
    
    # Check for manual date override
    dag_run = context.get("dag_run", {})
    conf = dag_run.conf or {}
    
    if "date" in conf:
        manual_date = conf["date"]
        print(f"📅 MANUAL MODE: {manual_date}")
        
        # For manual dates, process all layers for that date
        return {
            'mode': 'manual',
            'strategy': 'manual_full_pipeline',
            'silver_dates': [manual_date],      # Process this date in Silver
            'gold_dates': [manual_date],        # Process this date in Gold  
            'analytics_needed': True,           # Always refresh Analytics
            'reason': f'Manual full pipeline processing for {manual_date}'
        }
    
    # AUTO MODE: Find ALL missing data across layers
    print("🤖 AUTO MODE: Finding ALL gaps for complete single-run processing")
    
    try:
        bronze_dates = set(get_available_dates_for_layer(BRONZE_TABLE, 'bronze'))
        silver_dates = set(get_available_dates_for_layer(SILVER_TABLE, 'silver'))
        gold_dates = set(get_available_dates_for_layer(GOLD_TABLE, 'gold'))
    except Exception as e:
        print(f"❌ Error getting layer dates: {e}")
        return {
            'mode': 'error',
            'strategy': 'data_access_error',
            'silver_dates': [],
            'gold_dates': [],
            'analytics_needed': False,
            'reason': f'Could not access layer data: {str(e)}'
        }
    
    print("\n📊 LAYER STATUS:")
    print(f"   🥉 Bronze: {len(bronze_dates)} dates")
    print(f"   🥈 Silver: {len(silver_dates)} dates")
    print(f"   🥇 Gold: {len(gold_dates)} dates")
    
    # Find ALL missing dates
    missing_silver = sorted(bronze_dates - silver_dates)
    missing_gold = sorted(silver_dates - gold_dates)
    
    print("\n🔍 GAP ANALYSIS:")
    print(f"   Missing Silver: {len(missing_silver)} dates")
    print(f"   Missing Gold: {len(missing_gold)} dates")
    
    # Debug output for missing dates
    if missing_silver:
        print(f"   🥈 Silver gaps: {missing_silver[:5]}{'...' if len(missing_silver) > 5 else ''}")
    if missing_gold:
        print(f"   🥇 Gold gaps: {missing_gold[:5]}{'...' if len(missing_gold) > 5 else ''}")
    
    # SINGLE RUN STRATEGY: Process everything that needs processing
    if missing_silver or missing_gold:
        strategy = 'single_run_complete_pipeline'
        
        # After Silver processes missing_silver, Gold will need to process those PLUS existing missing_gold
        # But Gold task will dynamically detect what needs processing at runtime
        
        print(f"\n🎯 STRATEGY: {strategy}")
        print(f"📋 SILVER: Will process {len(missing_silver)} missing dates")
        print("📋 GOLD: Will process gaps after Silver completion")
        print("📋 ANALYTICS: Will run after Gold completion")
        
        return {
            'mode': 'auto',
            'strategy': strategy,
            'silver_dates': missing_silver,     # Silver processes its gaps
            'gold_dates': missing_gold,         # Gold knows about current gaps
            'analytics_needed': True,           # Analytics always runs at end
            'total_silver_missing': len(missing_silver),
            'total_gold_missing': len(missing_gold),
            'reason': f'Single run: {len(missing_silver)} Silver + {len(missing_gold)} Gold gaps + Analytics'
        }
    else:
        # Everything is current - just refresh analytics
        strategy = 'single_run_analytics_only'
        reason = 'All medallion layers current - refreshing Analytics only'
        
        print(f"\n🎯 STRATEGY: {strategy}")
        print(f"📋 REASON: {reason}")
        
        return {
            'mode': 'auto',
            'strategy': strategy,
            'silver_dates': [],
            'gold_dates': [],
            'analytics_needed': True,           # Just refresh Analytics
            'total_silver_missing': 0,
            'total_gold_missing': 0,
            'reason': reason
        }


def get_processing_dates(**context) -> Dict:
    """
    Main function called by DAG - returns what needs processing for SINGLE RUN
    
    FIXED: Uses single-run assessment logic
    """
    print("🎯 SINGLE RUN PROCESSING DATE ASSESSMENT")
    print("=" * 45)
    
    # Use the single-run assessment
    assessment = assess_processing_needs_single_run(**context)
    
    # Create consolidated response for DAG
    silver_dates = assessment['silver_dates']
    gold_dates = assessment['gold_dates']
    all_dates = list(set(silver_dates + gold_dates))  # Unique dates
    
    print("\n📋 SINGLE RUN ASSESSMENT:")
    print(f"   Strategy: {assessment['strategy']}")
    print(f"   Silver dates: {len(silver_dates)}")
    print(f"   Gold dates: {len(gold_dates)} (current gaps)")
    print(f"   Total unique dates: {len(all_dates)}")
    print(f"   Analytics needed: {assessment['analytics_needed']}")
    
    return {
        'target_date': all_dates[-1] if all_dates else None,
        'dates_to_process': all_dates,
        'process_count': len(all_dates),
        'strategy': assessment['strategy'],
        'reason': assessment['reason'],
        'process_silver': len(silver_dates) > 0,
        'process_gold': len(gold_dates) > 0 or len(silver_dates) > 0,  # Gold runs if Silver creates new data
        'process_analytics': assessment['analytics_needed'],
        'assessment_details': assessment
    }


def debug_pipeline_status():
    """ENHANCED: Print comprehensive pipeline status with better debugging"""
    print("📊 DETAILED PIPELINE STATUS")
    print("=" * 40)
    
    try:
        bronze_dates = get_available_dates_for_layer(BRONZE_TABLE, 'bronze')
        silver_dates = get_available_dates_for_layer(SILVER_TABLE, 'silver')
        gold_dates = get_available_dates_for_layer(GOLD_TABLE, 'gold')
        
        print("\n📈 Layer Counts:")
        print(f"   🥉 Bronze: {len(bronze_dates)} dates")
        print(f"   🥈 Silver: {len(silver_dates)} dates")
        print(f"   🥇 Gold: {len(gold_dates)} dates")
        
        if bronze_dates:
            print("\n📅 Latest Available Data:")
            print(f"   🥉 Latest Bronze: {bronze_dates[-1]}")
        if silver_dates:
            print(f"   🥈 Latest Silver: {silver_dates[-1]}")
        if gold_dates:
            print(f"   🥇 Latest Gold: {gold_dates[-1]}")
        
        # Show gaps
        if bronze_dates and silver_dates:
            missing_silver = len(set(bronze_dates) - set(silver_dates))
            print("\n⚠️  Gap Analysis:")
            print(f"   Missing Silver: {missing_silver} dates")
            if missing_silver > 0:
                gaps = sorted(set(bronze_dates) - set(silver_dates))
                print(f"   Silver gaps: {gaps[:3]}{'...' if len(gaps) > 3 else ''}")
        
        if silver_dates and gold_dates:
            missing_gold = len(set(silver_dates) - set(gold_dates))
            print(f"   Missing Gold: {missing_gold} dates")
            if missing_gold > 0:
                gaps = sorted(set(silver_dates) - set(gold_dates))
                print(f"   Gold gaps: {gaps[:3]}{'...' if len(gaps) > 3 else ''}")
        
        # Table existence check
        print("\n🏗️  Table Existence:")
        print(f"   🥉 Bronze exists: {check_table_exists(BRONZE_TABLE, 'bronze', 'github')}")
        print(f"   🥈 Silver exists: {check_table_exists(SILVER_TABLE, 'silver', 'github')}")
        print(f"   🥇 Gold exists: {check_table_exists(GOLD_TABLE, 'gold', 'github')}")
        
    except Exception as e:
        print(f"❌ Status check failed: {e}")
        import traceback
        traceback.print_exc()


# Keep original function name for compatibility
def print_pipeline_status():
    """Wrapper for backward compatibility"""
    debug_pipeline_status()


if __name__ == "__main__":
    # Test the single-run processing with enhanced debugging
    print("🧪 TESTING SINGLE RUN DATE PROCESSING")
    print("=" * 50)
    
    # Show current status with detailed debugging
    debug_pipeline_status()
    
    # Test assessment
    mock_context = {}
    result = get_processing_dates(**mock_context)
    
    print("\n🎯 Single Run Processing Assessment Results:")
    print(f"   Strategy: {result['strategy']}")
    print(f"   Target date: {result['target_date']}")
    print(f"   Dates to process: {len(result['dates_to_process'])}")
    print(f"   Silver needed: {result['process_silver']}")
    print(f"   Gold needed: {result['process_gold']}")
    print(f"   Analytics ready: {result['process_analytics']}")
    print(f"   Reason: {result['reason']}")
    
    if result['dates_to_process']:
        print(f"   First few dates: {result['dates_to_process'][:3]}")