"""
GitHub Analytics Processing - Separate from Medallion Processing
================================================================

CLEAN SEPARATION:
- Medallion file: Silver + Gold (data processing)
- Analytics file: Business intelligence + insights (this file)

CREATE NEW FILE: include/utils/github_analytics_processing.py
"""

from datetime import datetime, timedelta
from typing import Dict
import pandas as pd
import pyarrow as pa
from deltalake import write_deltalake, DeltaTable

from include.utils.delta_table_utils import (
    get_minio_storage_options,
    check_table_exists,
    get_enterprise_table_path
)
from include.schemas.github_schemas import (
    get_analytics_technology_trends_7d_schema,
    get_analytics_technology_trends_30d_schema,
    get_analytics_technology_trends_90d_schema,
    get_analytics_technology_trends_alltime_schema,
    validate_dataframe_against_schema
)

# FIXED: Consistent table naming
GOLD_TABLE = "technology_daily_activity"

# Analytics table configurations
ANALYTICS_TABLES = {
    '7d': {
        'table_name': 'technology_trends_7d',
        'min_days': 7,
        'table_path': 's3://delta-lake/analytics/analytics_github_technology_trends_7d'
    },
    '30d': {
        'table_name': 'technology_trends_30d', 
        'min_days': 30,
        'table_path': 's3://delta-lake/analytics/analytics_github_technology_trends_30d'
    },
    '90d': {
        'table_name': 'technology_trends_90d',
        'min_days': 90,
        'table_path': 's3://delta-lake/analytics/analytics_github_technology_trends_90d'
    },
    'alltime': {
        'table_name': 'technology_trends_alltime',
        'min_days': 1,
        'table_path': 's3://delta-lake/analytics/analytics_github_technology_trends_alltime'
    }
}


def backup_existing_analytics_data(timeframe: str, analysis_date: str, storage_options: Dict) -> Dict:
    """
    Create backup of existing analytics data before processing
    Returns backup info for potential rollback
    """
    table_path = ANALYTICS_TABLES[timeframe]['table_path']
    
    try:
        dt = DeltaTable(table_path, storage_options=storage_options)
        
        # Get existing data for this analysis date
        existing_df = dt.to_pandas(filters=[('analysis_date', '=', analysis_date)])
        
        if len(existing_df) > 0:
            backup_info = {
                'has_backup': True,
                'record_count': len(existing_df),
                'backup_data': existing_df.copy(),  # Keep in memory for rollback
                'table_version': dt.version()
            }
            print(f"   📋 Backed up {len(existing_df)} existing {timeframe} records")
        else:
            backup_info = {
                'has_backup': False,
                'record_count': 0,
                'backup_data': None,
                'table_version': dt.version()
            }
            print(f"   📋 No existing {timeframe} data to backup")
        
        return backup_info
        
    except Exception as e:
        print(f"   ⚠️ Could not backup {timeframe} data: {e}")
        return {
            'has_backup': False,
            'record_count': 0,
            'backup_data': None,
            'table_version': None,
            'backup_error': str(e)
        }


def safe_delete_analytics_data(timeframe: str, analysis_date: str, storage_options: Dict) -> Dict:
    """
    Safely delete existing analytics data with validation
    """
    table_path = ANALYTICS_TABLES[timeframe]['table_path']
    
    try:
        dt = DeltaTable(table_path, storage_options=storage_options)
        
        # Check what we're about to delete
        existing_count = len(dt.to_pandas(filters=[('analysis_date', '=', analysis_date)]))
        
        if existing_count > 0:
            # Perform the delete
            dt.delete(f"analysis_date == '{analysis_date}'")
            
            # Verify deletion worked
            remaining_count = len(dt.to_pandas(filters=[('analysis_date', '=', analysis_date)]))
            
            if remaining_count == 0:
                print(f"   🗑️ Deleted {existing_count} existing {timeframe} records")
                return {'success': True, 'deleted_count': existing_count}
            else:
                return {
                    'success': False, 
                    'error': f'Delete incomplete: {remaining_count} records remain',
                    'deleted_count': existing_count - remaining_count
                }
        else:
            print(f"   📝 No existing {timeframe} data to delete")
            return {'success': True, 'deleted_count': 0}
            
    except Exception as e:
        print(f"   ❌ Delete failed for {timeframe}: {e}")
        return {'success': False, 'error': str(e), 'deleted_count': 0}


def rollback_analytics_data(timeframe: str, backup_info: Dict, storage_options: Dict) -> bool:
    """
    Rollback analytics data using backup if processing fails
    """
    if not backup_info.get('has_backup', False):
        print(f"   📝 No backup to rollback for {timeframe}")
        return True
    
    try:
        table_path = ANALYTICS_TABLES[timeframe]['table_path']
        backup_data = backup_info['backup_data']
        
        if backup_data is not None and len(backup_data) > 0:
            # Get schema for this timeframe
            schema_funcs = {
                '7d': get_analytics_technology_trends_7d_schema,
                '30d': get_analytics_technology_trends_30d_schema,
                '90d': get_analytics_technology_trends_90d_schema,
                'alltime': get_analytics_technology_trends_alltime_schema
            }
            
            schema = schema_funcs[timeframe]()
            backup_table = pa.table(backup_data, schema=schema)
            
            # Write backup data back
            write_deltalake(
                table_or_uri=table_path,
                data=backup_table,
                storage_options=storage_options,
                mode="append",
                partition_by=["analysis_date"]
            )
            
            print(f"   🔄 Rolled back {len(backup_data)} {timeframe} records")
            return True
        else:
            print(f"   📝 No backup data to restore for {timeframe}")
            return True
            
    except Exception as e:
        print(f"   ❌ Rollback failed for {timeframe}: {e}")
        return False


def process_analytics_timeframe_safe(all_gold_df: pd.DataFrame, analysis_date: str, 
                                   timeframe: str, config: Dict, storage_options: Dict) -> Dict:
    """
    Process analytics timeframe with robust error handling and rollback capability
    """
    
    print(f"   🔄 Processing {timeframe} analytics with error handling...")
    
    # Step 1: Create backup of existing data
    backup_info = backup_existing_analytics_data(timeframe, analysis_date, storage_options)
    
    try:
        # Step 2: Calculate date range and filter data
        if timeframe == 'alltime':
            filtered_df = all_gold_df[all_gold_df['date'] <= analysis_date].copy()
            window_start = filtered_df['date'].min() if len(filtered_df) > 0 else analysis_date
            window_end = analysis_date
            days_in_window = len(filtered_df['date'].unique()) if len(filtered_df) > 0 else 0
        else:
            days_back = int(timeframe.replace('d', ''))
            end_date = datetime.strptime(analysis_date, '%Y-%m-%d')
            start_date = end_date - timedelta(days=days_back-1)
            
            window_start = start_date.strftime('%Y-%m-%d')
            window_end = analysis_date
            
            filtered_df = all_gold_df[
                (all_gold_df['date'] >= window_start) & 
                (all_gold_df['date'] <= window_end)
            ].copy()
            days_in_window = days_back
        
        # Step 3: Validate we have enough data
        if len(filtered_df) == 0:
            print(f"   ⚠️ No data for {timeframe} analysis")
            return {'status': 'skipped', 'reason': 'no_data'}
        
        if days_in_window < config['min_days']:
            print(f"   ⚠️ Only {days_in_window} days, need {config['min_days']} for {timeframe}")
            return {'status': 'skipped', 'reason': 'insufficient_data'}
        
        # Step 4: Aggregate by technology
        agg_df = filtered_df.groupby(['technology', 'technology_category']).agg({
            'daily_mentions': ['sum', 'mean', 'max'],
            'date': 'count'
        }).reset_index()
        
        # Flatten column names
        agg_df.columns = ['technology', 'technology_category', 'total_mentions', 
                        'avg_daily_mentions', 'peak_day_mentions', 'days_with_activity']
        
        # 🔧 COMPREHENSIVE NOISE FILTER
        noise_keywords = [
            'github', 'repository', 'repo', 'commit', 'branch', 
            'pull', 'push', 'clone', 'fork', 'star', 'issue', 'pr'
        ]
        agg_df = agg_df[~agg_df['technology'].isin(noise_keywords)]
        
        # 🔧 FIXED: Calculate rankings based on CURRENT timeframe data, not all-time
        print(f"   📊 Calculating {timeframe}-specific rankings...")
        
        # Overall ranking for this timeframe
        agg_df['mention_rank'] = agg_df['total_mentions'].rank(method='dense', ascending=False).astype('int32')
        
        # Category ranking for this timeframe  
        agg_df['category_rank'] = (
            agg_df.groupby('technology_category')['total_mentions']
            .transform(lambda x: x.rank(method='dense', ascending=False) if len(x) > 1 else 1)
            .fillna(1)
            .astype('int32')
        )
        
        # Debug output for rankings
        print(f"   🔍 Sample rankings for {timeframe}:")
        sample = agg_df.nlargest(3, 'total_mentions')[['technology', 'technology_category', 'total_mentions', 'mention_rank', 'category_rank']]
        for _, row in sample.iterrows():
            print(f"      #{row['mention_rank']} {row['technology']} ({row['technology_category']}) - {row['total_mentions']} mentions, category rank #{row['category_rank']}")
        
        # Step 5: Create analytics DataFrame based on timeframe
        if timeframe == 'alltime':
            analytics_df = pd.DataFrame({
                'analysis_date': analysis_date,
                'data_start_date': window_start,
                'data_end_date': window_end,
                'total_days_analyzed': days_in_window,
                'technology': agg_df['technology'],
                'technology_category': agg_df['technology_category'],
                'total_mentions_alltime': agg_df['total_mentions'].astype('int64'),
                'avg_daily_mentions_alltime': agg_df['avg_daily_mentions'].astype('float64'),
                'peak_day_mentions_alltime': agg_df['peak_day_mentions'].astype('int64'),
                'days_with_activity_alltime': agg_df['days_with_activity'].astype('int32'),
                # 🔧 FIXED: Use alltime-specific rankings (this makes sense for alltime table)
                'mention_rank_alltime': agg_df['mention_rank'],
                'category_rank_alltime': agg_df['category_rank'],
                'data_completeness_alltime': (agg_df['days_with_activity'] / days_in_window * 100).round(1).astype('float64'),
                'processed_at': pd.Timestamp.now(tz='UTC')
            })
        else:
            # 🔧 FIXED: For 7d, 30d, 90d - use timeframe-specific rankings
            analytics_df = pd.DataFrame({
                'analysis_date': analysis_date,
                'window_start_date': window_start,
                'window_end_date': window_end,
                'technology': agg_df['technology'],
                'technology_category': agg_df['technology_category'],
                f'total_mentions_{timeframe}': agg_df['total_mentions'].astype('int64'),
                f'avg_daily_mentions_{timeframe}': agg_df['avg_daily_mentions'].astype('float64'),
                f'peak_day_mentions_{timeframe}': agg_df['peak_day_mentions'].astype('int64'),
                f'days_with_activity_{timeframe}': agg_df['days_with_activity'].astype('int32'),
                f'trend_direction_{timeframe}': 'stable',  # Simplified for now
                # 🔧 FIXED: Use timeframe-specific rankings (not alltime!)
                f'mention_rank_{timeframe}': agg_df['mention_rank'],
                f'category_rank_{timeframe}': agg_df['category_rank'],
                f'data_completeness_{timeframe}': (agg_df['days_with_activity'] / days_in_window * 100).round(1).astype('float64'),
                'processed_at': pd.Timestamp.now(tz='UTC')
            })
            
            # Add 7d specific columns
            if timeframe == '7d':
                analytics_df['hotness_signal_7d'] = analytics_df[f'avg_daily_mentions_{timeframe}'].apply(
                    lambda x: 'hot' if x >= 200 else 'warm' if x >= 50 else 'cool'
                )
        
        # Step 6: Validate schema
        schema_funcs = {
            '7d': get_analytics_technology_trends_7d_schema,
            '30d': get_analytics_technology_trends_30d_schema,
            '90d': get_analytics_technology_trends_90d_schema,
            'alltime': get_analytics_technology_trends_alltime_schema
        }
        
        schema = schema_funcs[timeframe]()
        if not validate_dataframe_against_schema(analytics_df, schema, f"Analytics-{timeframe}"):
            raise ValueError(f"Analytics {timeframe} DataFrame failed schema validation")
        
        # Step 7: Safe delete existing data
        delete_result = safe_delete_analytics_data(timeframe, analysis_date, storage_options)
        if not delete_result['success']:
            raise Exception(f"Safe delete failed: {delete_result['error']}")
        
        # Step 8: Write new data
        analytics_table = pa.table(analytics_df, schema=schema)
        table_path = config['table_path']
        
        write_deltalake(
            table_or_uri=table_path,
            data=analytics_table,
            storage_options=storage_options,
            mode="append",
            partition_by=["analysis_date"]
        )
        
        print(f"   ✅ {timeframe}: {len(analytics_df)} technologies analyzed")
        
        return {
            'status': 'success',
            'timeframe': timeframe,
            'technologies_analyzed': len(analytics_df),
            'window_start': window_start,
            'window_end': window_end,
            'days_analyzed': days_in_window,
            'backup_info': backup_info
        }
        
    except Exception as e:
        print(f"   ❌ {timeframe} analytics failed: {e}")
        
        # Attempt rollback
        print(f"   🔄 Attempting rollback for {timeframe}...")
        rollback_success = rollback_analytics_data(timeframe, backup_info, storage_options)
        
        return {
            'status': 'failed', 
            'error': str(e),
            'timeframe': timeframe,
            'rollback_attempted': True,
            'rollback_success': rollback_success,
            'backup_info': backup_info
        }


def get_latest_analysis_date() -> str:
    """
    Get the latest date that should be used for analytics analysis
    Looks at Gold table to find most recent data
    """
    try:
        storage_options = get_minio_storage_options()
        gold_path = get_enterprise_table_path(GOLD_TABLE, "gold")
        
        if not check_table_exists(GOLD_TABLE, "gold", "github"):
            raise Exception("Gold table doesn't exist")
        
        gold_dt = DeltaTable(gold_path, storage_options=storage_options)
        all_gold_df = gold_dt.to_pandas()
        
        if len(all_gold_df) == 0:
            raise Exception("No Gold data available")
        
        latest_date = all_gold_df['date'].max()
        print(f"📅 Latest Gold data date: {latest_date}")
        return latest_date
        
    except Exception as e:
        print(f"❌ Could not determine latest analysis date: {e}")
        raise


def process_analytics_trends(**context) -> Dict:
    """
    Main Analytics processing function - called by DAG
    
    This function processes all analytics timeframes (7d, 30d, 90d, alltime)
    with robust error handling and rollback capabilities.
    """
    try:
        print("\n📊 ANALYTICS PROCESSING - SEPARATE MODULE")
        print("🧠 Business Intelligence + Insights Layer")  
        print("🛡️ With error handling and rollback capability")
        print("=" * 60)
        
        # Determine analysis date
        try:
            analysis_date = get_latest_analysis_date()
        except Exception as e:
            return {'status': 'blocked', 'reason': 'no_gold_data', 'error': str(e)}
        
        # Load Gold data
        storage_options = get_minio_storage_options()
        gold_path = get_enterprise_table_path(GOLD_TABLE, "gold")
        gold_dt = DeltaTable(gold_path, storage_options=storage_options)
        all_gold_df = gold_dt.to_pandas()
        
        print(f"📊 Loaded {len(all_gold_df)} records from Gold layer")
        print(f"📅 Gold date range: {all_gold_df['date'].min()} to {all_gold_df['date'].max()}")
        print(f"🎯 Analysis date: {analysis_date}")
        
        # Process each analytics timeframe with error handling
        analytics_results = {}
        successful_timeframes = []
        failed_timeframes = []
        
        for timeframe, config in ANALYTICS_TABLES.items():
            print(f"\n🔄 Processing {timeframe} analytics...")
            
            try:
                result = process_analytics_timeframe_safe(
                    all_gold_df, analysis_date, timeframe, config, storage_options
                )
                analytics_results[timeframe] = result
                
                if result.get('status') == 'success':
                    successful_timeframes.append(timeframe)
                elif result.get('status') == 'skipped':
                    print(f"   ⏩ {timeframe} skipped: {result.get('reason')}")
                else:
                    failed_timeframes.append(timeframe)
                    
            except Exception as e:
                print(f"❌ {timeframe} analytics failed with exception: {e}")
                analytics_results[timeframe] = {'status': 'failed', 'error': str(e)}
                failed_timeframes.append(timeframe)
        
        # Summary
        successful = len(successful_timeframes)
        total = len(analytics_results)
        
        print(f"\n📊 ANALYTICS SUMMARY: {successful}/{total} timeframes processed")
        if successful_timeframes:
            print(f"   ✅ Successful: {successful_timeframes}")
        if failed_timeframes:
            print(f"   ❌ Failed: {failed_timeframes}")
        
        # Determine overall status
        if successful == total:
            overall_status = 'success'
        elif successful > 0:
            overall_status = 'partial_success'
        else:
            overall_status = 'failed'
        
        return {
            'status': overall_status,
            'analysis_date': analysis_date,
            'timeframes_processed': successful,
            'total_timeframes': total,
            'successful_timeframes': successful_timeframes,
            'failed_timeframes': failed_timeframes,
            'analytics_results': analytics_results,
            'message': f'Analytics: {successful}/{total} timeframes processed'
        }
        
    except Exception as e:
        error_msg = f"Analytics processing failed: {str(e)}"
        print(f"❌ {error_msg}")
        return {'status': 'failed', 'error': str(e), 'message': error_msg}


if __name__ == "__main__":
    # Test analytics processing independently
    print("🧪 TESTING ANALYTICS PROCESSING")
    print("=" * 40)
    
    mock_context = {}
    result = process_analytics_trends(**mock_context)
    
    print("\n📊 Analytics test result:")
    print(f"   Status: {result['status']}")
    print(f"   Timeframes: {result.get('timeframes_processed', 0)}/{result.get('total_timeframes', 0)}")
    if 'successful_timeframes' in result:
        print(f"   Successful: {result['successful_timeframes']}")
    if 'failed_timeframes' in result:
        print(f"   Failed: {result['failed_timeframes']}")