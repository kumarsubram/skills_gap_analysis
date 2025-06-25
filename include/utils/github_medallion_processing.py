"""
FIXED Medallion Processing - Single Run Sequential: Silver → Gold
=================================================================

FIXES:
1. Single run processes Silver → Gold sequentially in one DAG execution
2. Gold task dynamically detects what needs processing after Silver completes
3. Less aggressive idempotency - processes what assessment says to process
4. Better error handling and debugging output

Key functions to replace in: include/utils/github_medallion_processing.py
"""

from typing import Dict, List
import pandas as pd
import pyarrow as pa
from deltalake import write_deltalake, DeltaTable

from include.utils.delta_table_utils import (
    get_table_count_for_date,
    get_minio_storage_options,
    check_table_exists,
    get_enterprise_table_path
)
from include.schemas.github_schemas import (
    get_silver_keyword_trends_schema,
    get_gold_technology_daily_activity_schema,
    validate_dataframe_against_schema
)
# FIXED: Use simplified classification for Gold layer
from include.business_logic.technology_classification import classify_technology_for_gold
from include.utils.github_date_processing import get_processing_dates

# FIXED: Consistent table naming across entire pipeline
BRONZE_TABLE = "keyword_extractions"
SILVER_TABLE = "keyword_trends"
GOLD_TABLE = "technology_daily_activity"  # Must match everywhere
MIN_BRONZE_RECORDS = 5


def check_dates_exist_in_layer_flexible(dates: List[str], table_name: str, layer: str) -> Dict[str, bool]:
    """Check which dates exist in a layer - LESS AGGRESSIVE for better processing"""
    existence_map = {}
    
    print(f"🔍 Checking {len(dates)} dates in {layer} layer...")
    
    for date_str in dates:
        exists = False
        try:
            count = get_table_count_for_date(table_name, date_str, layer, "github")
            exists = count is not None and count > 0
            status_icon = "✅" if exists else "❌"
            print(f"   {status_icon} {date_str}: {count if count else 0} records")
        except Exception as e:
            print(f"   ❌ {date_str}: Error checking - {e}")
            exists = False
        
        existence_map[date_str] = exists
    
    existing_count = sum(1 for exists in existence_map.values() if exists)
    print(f"📊 Existence check: {existing_count}/{len(dates)} dates already exist")
    
    return existence_map


def assess_processing_status(**context) -> Dict:
    """FIXED: Assessment using single-run date processing logic"""
    print("🧠 ASSESSMENT USING SINGLE-RUN DATE PROCESSING")
    print("=" * 50)
    
    try:
        processing_info = get_processing_dates(**context)
        
        print("📋 Processing assessment result:")
        print(f"   Strategy: {processing_info['strategy']}")
        print(f"   Process count: {processing_info['process_count']}")
        print(f"   Silver needed: {processing_info['process_silver']}")
        print(f"   Gold needed: {processing_info['process_gold']}")
        print(f"   Analytics needed: {processing_info['process_analytics']}")
        
        if processing_info['process_count'] == 0 and not processing_info['process_analytics']:
            print("⏩ NO PROCESSING NEEDED")
            return {
                'dates_to_process': [],
                'process_silver': False,
                'process_gold': False,
                'process_analytics': False,
                'strategy': processing_info['strategy'],
                'processing_info': processing_info
            }
        
        dates_to_process = processing_info['dates_to_process']
        strategy = processing_info['strategy']
        
        print(f"\n🎯 ASSESSMENT RESULT: {len(dates_to_process)} dates to process")
        print(f"📅 Dates: {dates_to_process}")
        
        return {
            'dates_to_process': dates_to_process,
            'process_silver': processing_info['process_silver'],
            'process_gold': processing_info['process_gold'],
            'process_analytics': processing_info['process_analytics'],
            'processing_info': processing_info,
            'strategy': strategy
        }
        
    except Exception as e:
        print(f"❌ Assessment failed: {e}")
        import traceback
        traceback.print_exc()
        
        # Return safe defaults
        return {
            'dates_to_process': [],
            'process_silver': False,
            'process_gold': False,
            'process_analytics': False,
            'strategy': 'error',
            'processing_info': {'error': str(e)}
        }


def validate_bronze_data_for_dates(dates_to_process: List[str]) -> Dict:
    """Validate Bronze data quality for multiple dates"""
    print(f"🔍 VALIDATING BRONZE DATA FOR {len(dates_to_process)} DATES")
    
    try:
        bronze_path = get_enterprise_table_path(BRONZE_TABLE, "bronze")
        storage_options = get_minio_storage_options()
        
        if not check_table_exists(BRONZE_TABLE, "bronze", "github"):
            return {
                'valid': False,
                'valid_dates': [],
                'message': 'Bronze table not found'
            }
        
        dt = DeltaTable(bronze_path, storage_options=storage_options)
        bronze_df = dt.to_pandas()
        
        valid_dates = []
        total_records = 0
        
        for date_str in dates_to_process:
            date_df = bronze_df[bronze_df['date'] == date_str]
            record_count = len(date_df)
            
            if record_count >= MIN_BRONZE_RECORDS:
                valid_dates.append(date_str)
                total_records += record_count
                print(f"   ✅ {date_str}: {record_count:,} records")
            else:
                print(f"   ⚠️ {date_str}: {record_count} records (need >= {MIN_BRONZE_RECORDS})")
        
        print(f"📊 Validation: {len(valid_dates)}/{len(dates_to_process)} dates valid")
        
        return {
            'valid': len(valid_dates) > 0,
            'valid_dates': valid_dates,
            'total_records': total_records,
            'message': f'{len(valid_dates)} dates have valid Bronze data'
        }
        
    except Exception as e:
        print(f"❌ Bronze validation error: {e}")
        return {
            'valid': False,
            'valid_dates': [],
            'message': f'Validation failed: {str(e)}'
        }


def process_bronze_to_silver(**context) -> Dict:
    """SINGLE RUN: Process Silver as part of complete pipeline"""
    try:
        status = assess_processing_status(**context)
        
        if not status['process_silver']:
            print("⏩ SILVER PROCESSING SKIPPED")
            print("   Reason: Assessment says no Silver processing needed")
            return {'status': 'skipped', 'reason': 'not_needed_per_assessment'}
        
        # Get Silver dates from assessment
        assessment_details = status.get('processing_info', {}).get('assessment_details', {})
        silver_dates = assessment_details.get('silver_dates', [])
        
        if not silver_dates:
            print("⏩ SILVER PROCESSING SKIPPED")
            print("   Reason: No Silver dates in assessment")
            return {'status': 'skipped', 'reason': 'no_silver_dates'}
        
        print(f"\n🥈 SINGLE RUN SILVER PROCESSING: {len(silver_dates)} dates")
        print(f"📅 Silver dates: {silver_dates}")
        
        # Validate Bronze data
        validation = validate_bronze_data_for_dates(silver_dates)
        if not validation['valid']:
            return {'status': 'failed', 'error': validation['message']}
        
        valid_dates = validation['valid_dates']
        
        # Process all valid dates
        storage_options = get_minio_storage_options()
        bronze_path = get_enterprise_table_path(BRONZE_TABLE, "bronze")
        bronze_dt = DeltaTable(bronze_path, storage_options=storage_options)
        
        all_silver_data = []
        processed_dates = []
        
        for date_str in valid_dates:
            try:
                print(f"🔄 Processing {date_str}...")
                
                # Load Bronze data for this date
                bronze_df = bronze_dt.to_pandas(filters=[('date', '=', date_str)])
                
                if len(bronze_df) == 0:
                    print(f"   ⚠️ No Bronze data for {date_str}")
                    continue
                
                # Aggregate to Silver
                silver_agg = bronze_df.groupby(['date', 'keyword']).agg({
                    'mentions': 'sum',
                    'repo_mentions': 'sum', 
                    'event_mentions': 'sum',
                    'top_repo': 'first',
                    'hour': 'count'
                }).reset_index()
                
                # Create Silver DataFrame
                silver_df = pd.DataFrame({
                    'date': silver_agg['date'],
                    'technology': silver_agg['keyword'],
                    'daily_mentions': silver_agg['mentions'].astype('int64'),
                    'daily_repo_mentions': silver_agg['repo_mentions'].astype('int64'),
                    'daily_event_mentions': silver_agg['event_mentions'].astype('int64'),
                    'primary_repository': silver_agg['top_repo'],
                    'data_quality_score': (
                        (silver_agg['mentions'] / silver_agg['mentions'].max() * 50) +
                        (silver_agg['hour'] / 24 * 30) + 20
                    ).clip(0, 100).astype('float64'),
                    'source_record_count': silver_agg['hour'].astype('int32'),
                    'processed_at': pd.Timestamp.now(tz='UTC'),
                    'source_layer': 'bronze_batch'
                })
                
                all_silver_data.append(silver_df)
                processed_dates.append(date_str)
                print(f"   ✅ {date_str}: {len(silver_df)} technologies")
                
            except Exception as e:
                print(f"   ❌ {date_str} failed: {e}")
                continue
        
        if not all_silver_data:
            return {'status': 'failed', 'error': 'No valid data to process'}
        
        # Combine all Silver data
        combined_silver_df = pd.concat(all_silver_data, ignore_index=True)
        
        # Validate schema
        silver_schema = get_silver_keyword_trends_schema()
        if not validate_dataframe_against_schema(combined_silver_df, silver_schema, "Silver"):
            raise ValueError("Silver DataFrame failed schema validation")
        
        # Delete existing data for these dates before writing (safer)
        silver_path = get_enterprise_table_path(SILVER_TABLE, "silver")
        
        try:
            # Try to delete existing data for these dates
            silver_dt_existing = DeltaTable(silver_path, storage_options=storage_options)
            for date_str in processed_dates:
                try:
                    silver_dt_existing.delete(f"date == '{date_str}'")
                    print(f"   🗑️ Cleaned existing Silver data for {date_str}")
                except Exception as e:
                    print(f"   ℹ️ No existing Silver data to clean for {date_str}: {e}")
        except Exception as e:
            print(f"   ℹ️ Silver table may not exist yet: {e}")
        
        # Write to Silver table
        silver_table = pa.table(combined_silver_df, schema=silver_schema)
        
        write_deltalake(
            table_or_uri=silver_path,
            data=silver_table,
            storage_options=storage_options,
            mode="append",
            partition_by=["date"]
        )
        
        print(f"\n✅ SILVER COMPLETE: {len(processed_dates)} dates processed")
        print("🔄 Gold will now process gaps including these new Silver dates")
        
        return {
            'status': 'success',
            'dates_processed': processed_dates,
            'total_technologies': len(combined_silver_df),
            'total_mentions': int(combined_silver_df['daily_mentions'].sum()),
            'message': f'Silver: {len(processed_dates)} dates processed - ready for Gold'
        }
        
    except Exception as e:
        error_msg = f"Silver processing failed: {str(e)}"
        print(f"❌ {error_msg}")
        import traceback
        traceback.print_exc()
        return {'status': 'failed', 'error': str(e), 'message': error_msg}


def process_silver_to_gold(**context) -> Dict:
    """SINGLE RUN: Dynamic Gold processing - detects what needs processing after Silver completion"""
    try:
        print("\n🥇 SINGLE RUN GOLD PROCESSING - DYNAMIC DETECTION")
        print("=" * 55)
        
        # Get Silver processing results to understand what just happened
        ti = context.get('task_instance')
        silver_result = ti.xcom_pull(task_ids='run_silver_processing') if ti else None
        
        # DYNAMIC: Check what Silver data exists NOW (after Silver task completion)
        silver_path = get_enterprise_table_path(SILVER_TABLE, "silver")
        storage_options = get_minio_storage_options()
        
        if not check_table_exists(SILVER_TABLE, "silver", "github"):
            print("⏩ GOLD PROCESSING SKIPPED - No Silver table exists")
            return {'status': 'skipped', 'reason': 'no_silver_table'}
        
        # Get current Silver dates (including any just processed)
        silver_dt = DeltaTable(silver_path, storage_options=storage_options)
        silver_df = silver_dt.to_pandas()
        
        if len(silver_df) == 0:
            print("⏩ GOLD PROCESSING SKIPPED - Silver table is empty")
            return {'status': 'skipped', 'reason': 'silver_table_empty'}
        
        available_silver_dates = sorted(silver_df['date'].unique().tolist())
        print(f"📊 Available Silver dates NOW: {len(available_silver_dates)}")
        
        # Check which Gold dates are missing (dynamic check)
        gold_path = get_enterprise_table_path(GOLD_TABLE, "gold")
        
        if check_table_exists(GOLD_TABLE, "gold", "github"):
            gold_dt = DeltaTable(gold_path, storage_options=storage_options)
            gold_df = gold_dt.to_pandas()
            existing_gold_dates = set(gold_df['date'].unique().tolist())
            print(f"📊 Existing Gold dates: {len(existing_gold_dates)}")
        else:
            existing_gold_dates = set()
            print("📊 No Gold table exists - will create")
        
        # Find missing Gold dates (including newly created Silver dates)
        missing_gold_dates = sorted(set(available_silver_dates) - existing_gold_dates)
        
        if not missing_gold_dates:
            print("⏩ GOLD PROCESSING SKIPPED - All Silver dates already in Gold")
            return {'status': 'skipped', 'reason': 'gold_up_to_date'}
        
        print(f"🔄 DYNAMIC DETECTION: Processing {len(missing_gold_dates)} Gold dates")
        print(f"📅 Gold dates to process: {missing_gold_dates}")
        
        # Show what caused these Gold gaps
        if silver_result and silver_result.get('status') == 'success':
            newly_processed_silver = silver_result.get('dates_processed', [])
            overlap = set(missing_gold_dates) & set(newly_processed_silver)
            if overlap:
                print(f"   🔄 {len(overlap)} dates from Silver processing: {sorted(overlap)}")
            existing_gaps = set(missing_gold_dates) - set(newly_processed_silver)
            if existing_gaps:
                print(f"   📋 {len(existing_gaps)} existing gaps: {sorted(existing_gaps)}")
        
        # Process missing Gold dates
        all_gold_data = []
        processed_dates = []
        
        for date_str in missing_gold_dates:
            try:
                print(f"🔄 Processing Gold for {date_str}...")
                
                # Load Silver data for this date
                silver_df_date = silver_dt.to_pandas(filters=[('date', '=', date_str)])
                
                if len(silver_df_date) == 0:
                    print(f"   ⚠️ No Silver data for {date_str}")
                    continue
                
                # Apply SIMPLIFIED Gold classifications
                gold_df = silver_df_date.copy()
                
                for idx, row in gold_df.iterrows():
                    # FIXED: Use simplified classification for Gold
                    classification = classify_technology_for_gold(row['technology'], row['daily_mentions'])
                    gold_df.loc[idx, 'technology_category'] = classification['technology_category']
                    gold_df.loc[idx, 'mention_volume'] = classification['mention_volume']
                    gold_df.loc[idx, 'data_completeness'] = classification['data_completeness']
                
                # Create clean Gold DataFrame with SIMPLIFIED schema
                gold_clean = pd.DataFrame({
                    'date': gold_df['date'],
                    'technology': gold_df['technology'],
                    'daily_mentions': gold_df['daily_mentions'].astype('int64'),
                    'daily_repo_mentions': gold_df['daily_repo_mentions'].astype('int64'),
                    'daily_event_mentions': gold_df['daily_event_mentions'].astype('int64'),
                    'primary_repository': gold_df['primary_repository'],
                    'technology_category': gold_df['technology_category'],
                    'mention_volume': gold_df['mention_volume'],  # SIMPLIFIED
                    'data_completeness': gold_df['data_completeness'],  # SIMPLIFIED
                    'processed_at': pd.Timestamp.now(tz='UTC'),
                    'data_source': 'github_silver'
                })
                
                all_gold_data.append(gold_clean)
                processed_dates.append(date_str)
                print(f"   ✅ {date_str}: {len(gold_clean)} technologies classified")
                
            except Exception as e:
                print(f"   ❌ {date_str} failed: {e}")
                import traceback
                traceback.print_exc()
                continue
        
        if not all_gold_data:
            return {'status': 'failed', 'error': 'No valid Gold data to process'}
        
        # Combine all Gold data
        combined_gold_df = pd.concat(all_gold_data, ignore_index=True)
        
        # Validate schema
        gold_schema = get_gold_technology_daily_activity_schema()
        if not validate_dataframe_against_schema(combined_gold_df, gold_schema, "Gold"):
            raise ValueError("Gold DataFrame failed schema validation")
        
        # Delete existing data for these dates before writing
        try:
            # Try to delete existing data for these dates
            gold_dt_existing = DeltaTable(gold_path, storage_options=storage_options)
            for date_str in processed_dates:
                try:
                    gold_dt_existing.delete(f"date == '{date_str}'")
                    print(f"   🗑️ Cleaned existing Gold data for {date_str}")
                except Exception as e:
                    print(f"   ℹ️ No existing Gold data to clean for {date_str}: {e}")
        except Exception as e:
            print(f"   ℹ️ Gold table may not exist yet: {e}")
        
        # Write to Gold table
        gold_table = pa.table(combined_gold_df, schema=gold_schema)
        
        write_deltalake(
            table_or_uri=gold_path,
            data=gold_table,
            storage_options=storage_options,
            mode="append",
            partition_by=["date"]
        )
        
        # Summary
        category_summary = combined_gold_df['technology_category'].value_counts()
        volume_summary = combined_gold_df['mention_volume'].value_counts()
        
        print(f"\n✅ GOLD COMPLETE: {len(processed_dates)} dates processed")
        print(f"📂 Categories: {dict(category_summary.head(3))}")
        print(f"📊 Volumes: {dict(volume_summary)}")
        print("🔄 Analytics will now process with complete Gold data")
        
        return {
            'status': 'success',
            'dates_processed': processed_dates,
            'technologies_classified': len(combined_gold_df),
            'category_summary': dict(category_summary),
            'volume_summary': dict(volume_summary),
            'message': f'Gold: {len(processed_dates)} dates processed - ready for Analytics'
        }
        
    except Exception as e:
        error_msg = f"Gold processing failed: {str(e)}"
        print(f"❌ {error_msg}")
        import traceback
        traceback.print_exc()
        return {'status': 'failed', 'error': str(e), 'message': error_msg}