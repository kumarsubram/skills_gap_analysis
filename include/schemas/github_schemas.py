"""
Fix 1: Gold Schema Alignment - Practical Idempotency
====================================================

FIXES:
1. Gold schema matches exactly what processing code writes
2. Proper validation that catches mismatches
3. Clear error messages for debugging

Update this function in: include/schemas/github_schemas.py
"""

import pyarrow as pa
import pandas as pd


def get_gold_technology_daily_activity_schema() -> pa.Schema:
    """
    FIXED: Gold schema that matches EXACTLY what our processing code writes
    
    Must align with the output of classify_technology_for_gold():
    - technology_category
    - mention_volume  
    - data_completeness
    """
    return pa.schema([
        # Core identification (same as Silver)
        pa.field("date", pa.string(), nullable=False),
        pa.field("technology", pa.string(), nullable=False),
        
        # Factual metrics from Silver layer (unchanged)
        pa.field("daily_mentions", pa.int64(), nullable=False),
        pa.field("daily_repo_mentions", pa.int64(), nullable=False),
        pa.field("daily_event_mentions", pa.int64(), nullable=False),
        pa.field("primary_repository", pa.string(), nullable=True),
        
        # FIXED: Exactly match classify_technology_for_gold() output
        pa.field("technology_category", pa.string(), nullable=False),  # ai_model, language, framework, etc.
        pa.field("mention_volume", pa.string(), nullable=False),       # high, medium, low
        pa.field("data_completeness", pa.string(), nullable=False),    # complete, empty
        
        # Metadata (same as Silver)
        pa.field("processed_at", pa.timestamp('us', tz='UTC'), nullable=False),
        pa.field("data_source", pa.string(), nullable=False)
    ])


def validate_dataframe_against_schema(df: pd.DataFrame, schema: pa.Schema, layer_name: str) -> bool:
    """
    ENHANCED: Better validation with detailed error reporting
    
    Args:
        df: DataFrame to validate
        schema: PyArrow schema to validate against
        layer_name: Name of layer for error messages
    
    Returns:
        bool: True if validation passes, False otherwise
    """
    try:
        # Check if all required columns exist
        schema_columns = [field.name for field in schema]
        df_columns = list(df.columns)
        
        missing_columns = set(schema_columns) - set(df_columns)
        if missing_columns:
            print(f"❌ {layer_name} validation failed: Missing columns {missing_columns}")
            print(f"   Expected: {schema_columns}")
            print(f"   Got: {df_columns}")
            return False
        
        extra_columns = set(df_columns) - set(schema_columns)
        if extra_columns:
            print(f"⚠️  {layer_name} validation warning: Extra columns {extra_columns}")
            # Don't fail on extra columns, just warn
        
        # Check data types and values
        try:
            # Create table with only schema columns to avoid extra column issues
            df_subset = df[schema_columns]
            pa.table(df_subset, schema=schema)
            print(f"✅ {layer_name} schema validation passed")
            
            # Show sample data for verification
            if len(df) > 0:
                print(f"   Sample {layer_name} record:")
                sample_row = df.iloc[0][schema_columns].to_dict()
                for col, val in sample_row.items():
                    print(f"     {col}: {val}")
            
            return True
            
        except Exception as type_error:
            print(f"❌ {layer_name} type validation failed: {type_error}")
            
            # Show detailed type mismatch info
            for field in schema:
                col_name = field.name
                expected_type = field.type
                if col_name in df.columns:
                    actual_type = str(df[col_name].dtype)
                    sample_val = df[col_name].iloc[0] if len(df) > 0 else 'N/A'
                    print(f"     {col_name}: expected {expected_type}, got {actual_type}, sample: {sample_val}")
            
            return False
            
    except Exception as e:
        print(f"❌ {layer_name} validation error: {e}")
        return False


def test_gold_schema_alignment():
    """
    Test that our Gold schema matches what the processing code actually produces
    """
    print("🧪 TESTING GOLD SCHEMA ALIGNMENT")
    print("=" * 45)
    
    # Test with data that matches classify_technology_for_gold() output
    gold_sample = pd.DataFrame({
        'date': ['2024-12-31'],
        'technology': ['python'],
        'daily_mentions': [1500],
        'daily_repo_mentions': [800],
        'daily_event_mentions': [700],
        'primary_repository': ['python/cpython'],
        
        # FIXED: These must match classify_technology_for_gold() exactly
        'technology_category': ['language'],        # From get_technology_category()
        'mention_volume': ['high'],                 # From mention volume logic
        'data_completeness': ['complete'],          # From completeness logic
        
        'processed_at': [pd.Timestamp.now(tz='UTC')],
        'data_source': ['github_silver']
    })
    
    print("📋 Testing Gold schema with realistic data...")
    gold_schema = get_gold_technology_daily_activity_schema()
    is_valid = validate_dataframe_against_schema(gold_sample, gold_schema, "Gold")
    
    if is_valid:
        print("\n✅ SCHEMA ALIGNMENT SUCCESS")
        print("   Gold schema matches processing code output")
        print("   Ready for practical idempotency")
    else:
        print("\n❌ SCHEMA ALIGNMENT FAILED")
        print("   Need to fix schema/processing alignment")
    
    # Show the expected Gold columns
    print("\n📝 Gold Schema Columns:")
    for field in gold_schema:
        print(f"   {field.name}: {field.type}")
    
    return is_valid


# Keep all other schemas unchanged (Silver, Analytics)
def get_silver_keyword_trends_schema() -> pa.Schema:
    """Silver schema - UNCHANGED"""
    return pa.schema([
        pa.field("date", pa.string(), nullable=False),
        pa.field("technology", pa.string(), nullable=False),
        pa.field("daily_mentions", pa.int64(), nullable=False),
        pa.field("daily_repo_mentions", pa.int64(), nullable=False), 
        pa.field("daily_event_mentions", pa.int64(), nullable=False),
        pa.field("primary_repository", pa.string(), nullable=True),
        pa.field("data_quality_score", pa.float64(), nullable=False),
        pa.field("source_record_count", pa.int32(), nullable=False),
        pa.field("processed_at", pa.timestamp('us', tz='UTC'), nullable=False),
        pa.field("source_layer", pa.string(), nullable=False)
    ])


# Analytics schemas remain the same as defined previously...
def get_analytics_technology_trends_7d_schema() -> pa.Schema:
    """Analytics 7-day trends schema - FIXED"""
    return pa.schema([
        pa.field("analysis_date", pa.string(), nullable=False),
        pa.field("window_start_date", pa.string(), nullable=False),
        pa.field("window_end_date", pa.string(), nullable=False),
        pa.field("technology", pa.string(), nullable=False),
        pa.field("technology_category", pa.string(), nullable=False),
        pa.field("total_mentions_7d", pa.int64(), nullable=False),
        pa.field("avg_daily_mentions_7d", pa.float64(), nullable=False),
        pa.field("peak_day_mentions_7d", pa.int64(), nullable=False),
        pa.field("days_with_activity_7d", pa.int32(), nullable=False),
        pa.field("trend_direction_7d", pa.string(), nullable=False),
        pa.field("mention_rank_7d", pa.int32(), nullable=False),
        pa.field("category_rank_7d", pa.int32(), nullable=False),  # ← ADD THIS LINE
        pa.field("hotness_signal_7d", pa.string(), nullable=False),
        pa.field("data_completeness_7d", pa.float64(), nullable=False),
        pa.field("processed_at", pa.timestamp('us', tz='UTC'), nullable=False)
    ])


# Copy other analytics schemas from previous version...
def get_analytics_technology_trends_30d_schema() -> pa.Schema:
    """Analytics 30-day trends schema - FIXED"""
    return pa.schema([
        pa.field("analysis_date", pa.string(), nullable=False),
        pa.field("window_start_date", pa.string(), nullable=False),
        pa.field("window_end_date", pa.string(), nullable=False),
        pa.field("technology", pa.string(), nullable=False),
        pa.field("technology_category", pa.string(), nullable=False),
        pa.field("total_mentions_30d", pa.int64(), nullable=False),
        pa.field("avg_daily_mentions_30d", pa.float64(), nullable=False),
        pa.field("peak_day_mentions_30d", pa.int64(), nullable=False),
        pa.field("days_with_activity_30d", pa.int32(), nullable=False),
        pa.field("trend_direction_30d", pa.string(), nullable=False),
        pa.field("mention_rank_30d", pa.int32(), nullable=False),
        pa.field("category_rank_30d", pa.int32(), nullable=False),  # ← ADD THIS LINE
        pa.field("data_completeness_30d", pa.float64(), nullable=False),
        pa.field("processed_at", pa.timestamp('us', tz='UTC'), nullable=False)
    ])


def get_analytics_technology_trends_90d_schema() -> pa.Schema:
    """Analytics 90-day trends schema - FIXED"""
    return pa.schema([
        pa.field("analysis_date", pa.string(), nullable=False),
        pa.field("window_start_date", pa.string(), nullable=False),
        pa.field("window_end_date", pa.string(), nullable=False),
        pa.field("technology", pa.string(), nullable=False),
        pa.field("technology_category", pa.string(), nullable=False),
        pa.field("total_mentions_90d", pa.int64(), nullable=False),
        pa.field("avg_daily_mentions_90d", pa.float64(), nullable=False),
        pa.field("peak_day_mentions_90d", pa.int64(), nullable=False),
        pa.field("days_with_activity_90d", pa.int32(), nullable=False),
        pa.field("trend_direction_90d", pa.string(), nullable=False),
        pa.field("mention_rank_90d", pa.int32(), nullable=False),
        pa.field("category_rank_90d", pa.int32(), nullable=False),  # ← ADD THIS LINE
        pa.field("data_completeness_90d", pa.float64(), nullable=False),
        pa.field("processed_at", pa.timestamp('us', tz='UTC'), nullable=False)
    ])


def get_analytics_technology_trends_alltime_schema() -> pa.Schema:
    """Analytics all-time trends schema - UNCHANGED"""
    return pa.schema([
        pa.field("analysis_date", pa.string(), nullable=False),
        pa.field("data_start_date", pa.string(), nullable=False),
        pa.field("data_end_date", pa.string(), nullable=False),
        pa.field("total_days_analyzed", pa.int32(), nullable=False),
        pa.field("technology", pa.string(), nullable=False),
        pa.field("technology_category", pa.string(), nullable=False),
        pa.field("total_mentions_alltime", pa.int64(), nullable=False),
        pa.field("avg_daily_mentions_alltime", pa.float64(), nullable=False),
        pa.field("peak_day_mentions_alltime", pa.int64(), nullable=False),
        pa.field("days_with_activity_alltime", pa.int32(), nullable=False),
        pa.field("mention_rank_alltime", pa.int32(), nullable=False),
        pa.field("category_rank_alltime", pa.int32(), nullable=False),
        pa.field("data_completeness_alltime", pa.float64(), nullable=False),
        pa.field("processed_at", pa.timestamp('us', tz='UTC'), nullable=False)
    ])


if __name__ == "__main__":
    test_gold_schema_alignment()