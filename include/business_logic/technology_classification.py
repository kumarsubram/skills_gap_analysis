"""
SIMPLIFIED Gold Layer Classification - Step 2
============================================

FIXES:
1. Gold layer only does basic categorization (not complex business logic)
2. Removes redundant/meaningless single-date calculations
3. Focuses on data quality and basic enrichment
4. Saves complex analytics for the Analytics layer

Replace: include/business_logic/technology_classification.py
"""

import json
from typing import Dict
from pathlib import Path


def load_tech_keywords() -> Dict[str, Dict[str, str]]:
    """Load technology keywords mapping from JSON file"""
    try:
        possible_paths = [
            Path(__file__).parent.parent / "jsons" / "tech_keywords.json",
            Path("/opt/airflow/include/jsons/tech_keywords.json")
        ]
        
        for tech_keywords_path in possible_paths:
            if tech_keywords_path.exists():
                with open(tech_keywords_path, 'r') as f:
                    keywords = json.load(f)
                    print(f"📋 Loaded {len(keywords)} technology keywords")
                    return keywords
        
        print("⚠️  Tech keywords file not found")
        return {}
        
    except Exception as e:
        print(f"⚠️  Could not load tech keywords: {e}")
        return {}


# Load tech keywords once at module level
TECH_KEYWORDS = load_tech_keywords()


def get_technology_category(technology: str) -> str:
    """
    Get technology category - SIMPLIFIED for Gold layer
    
    This is the ONLY classification that belongs in Gold.
    Complex business logic goes in Analytics layer.
    
    Returns standardized category from tech_keywords.json:
    - ai_model, ai_framework, ai_technique, ai_service, ai_tool
    - language  
    - framework, database, cloud, devops, tool, etc.
    """
    tech_lower = technology.lower().strip()
    
    # Direct lookup from tech_keywords.json
    if tech_lower in TECH_KEYWORDS:
        keyword_type = TECH_KEYWORDS[tech_lower].get('type', 'tool')
        return standardize_category_name(keyword_type)
    
    # Pattern matching for technologies not in keywords file
    return infer_category_from_patterns(tech_lower)


def standardize_category_name(raw_type: str) -> str:
    """ENHANCED: Better category mapping aligned with your tech_keywords.json"""
    category_mapping = {
        # AI/ML categories (2025 priority - more granular)
        'ai_model': 'ai_model',
        'ai_framework': 'ai_framework', 
        'ai_technique': 'ai_technique',
        'ai_service': 'ai_service',
        'ai_tool': 'ai_tool',
        'ai_platform': 'ai_platform',
        'ml_framework': 'ai_framework',
        'computer_vision': 'ai_framework',
        'nlp': 'ai_framework',
        'vector_db': 'ai_framework',
        
        # Core Programming
        'language': 'language',
        'query_language': 'language',
        
        # Frameworks & Libraries
        'framework': 'framework',
        'web_framework': 'framework',
        'mobile_framework': 'framework',
        'desktop_framework': 'framework',
        'css_framework': 'framework',
        'library': 'library',
        'orm': 'library',                   # You have this - good
        
        # Data & Storage
        'database': 'database',
        'big_data': 'big_data',            # You have this - keep separate
        
        # Infrastructure & DevOps  
        'cloud': 'cloud',
        'devops': 'devops',
        'monitoring': 'devops',            # You have this - consolidate under devops
        
        # Development Tools
        'tool': 'tool',
        'testing': 'tool',                 # You have this - consolidate
        'build_tool': 'tool',             # You have this - consolidate  
        'package_manager': 'tool',        # You have this - consolidate
        'version_control': 'tool',        # You have this - consolidate
        'css_preprocessor': 'tool',       # You have this - consolidate
        'css_tool': 'tool',               # You have this - consolidate
        
        # Specialized Platforms
        'platform': 'platform',           # You have this - good
        'blockchain': 'blockchain'         # You have this - keep separate
    }
    
    return category_mapping.get(raw_type, 'tool')

    
def infer_category_from_patterns(tech_lower: str) -> str:
    """ENHANCED: Updated patterns to match your comprehensive JSON"""
    
    # AI/ML patterns - UPDATED with your JSON entries
    ai_model_patterns = [
        'gpt', 'claude', 'llama', 'bert', 'roberta', 't5', 'bloom', 'palm',
        'codex', 'davinci', 'curie', 'babbage', 'ada', 'whisper', 'dall-e',
        'stable-diffusion', 'midjourney', 'bard', 'chatgpt', 'gemini', 'qwen',
        'deepseek', 'mixtral', 'mistral', 'phi', 'grok', 'yi', 'gemma',    # ADD: from your JSON
        'codestral', 'yolo'  # ADD: missing from your patterns
    ]
    
    ai_framework_patterns = [
        'tensorflow', 'pytorch', 'keras', 'scikit', 'sklearn', 'transformers',
        'langchain', 'llamaindex', 'haystack', 'autogen', 'guidance', 'pinecone',
        'weaviate', 'chroma', 'qdrant', 'faiss', 'ollama', 'vllm',
        # ADD: Missing AI frameworks from your JSON
        'langgraph', 'langsmith', 'datasets', 'accelerate', 'peft',
        'litellm', 'crewai', 'semantic-kernel', 'instructor', 'outlines', 'marvin',
        'vercel ai', 'openai-node', 'langchainjs', 'milvus', 'pgvector'
    ]

    blockchain_patterns = [
        # Core (you have these)
        'web3', 'ethereum', 'bitcoin', 'solidity',        
        # ADD: High priority DeFi protocols (2025 hot)
        'uniswap', 'aave', 'compound', 'defi',        
        # ADD: Web3 dev tools (high GitHub activity)
        'ethers', 'wagmi', 'metamask',        
        # ADD: Layer 2 (you have in JSON but not patterns)
        'polygon', 'arbitrum', 'optimism',        
        # ADD: Other hot chains
        'solana', 'cardano'
    ]
    
    # Programming languages - ADD missing ones from your JSON
    language_patterns = [
        'python', 'javascript', 'typescript', 'java', 'kotlin', 'scala',
        'go', 'golang', 'rust', 'cpp', 'c++', 'csharp', 'c#', 'php', 'ruby',
        'swift', 'dart', 'elixir', 'haskell', 'clojure', 'erlang', 'julia',
        'r', 'matlab', 'lua', 'perl', 'bash', 'powershell', 'zig', 'nim',
        'crystal', 'mojo', 'solidity', 'f#', 'ocaml'  # ADD: missing languages
    ]
    
    # Web frameworks - ADD missing ones
    web_framework_patterns = [
        'react', 'vue', 'angular', 'svelte', 'nextjs', 'nuxt', 'remix',
        'django', 'flask', 'fastapi', 'express', 'nestjs', 'laravel',
        'rails', 'spring', 'asp.net', 'gin', 'echo', 'fiber', 'phoenix',
        # ADD: Missing frameworks from your JSON
        'sveltekit', 'solid', 'solidjs', 'qwik', 'astro', 'quart', 'starlette',
        'fastify', 'koa', 'hapi', 'trpc', 'apollo', 'symfony', 'sinatra', 'blazor',
        'axum', 'actix', 'rocket'
    ]
    
    # Databases - ADD missing ones
    database_patterns = [
        'mysql', 'postgresql', 'postgres', 'sqlite', 'mongodb', 'redis',
        'elasticsearch', 'cassandra', 'dynamodb', 'neo4j', 'influxdb',
        'clickhouse', 'snowflake', 'bigquery', 'supabase', 'planetscale',
        'neon', 'turso', 'arangodb', 'couchdb',
        # ADD: Missing databases from your JSON
        'mariadb', 'oracle', 'opensearch', 'solr', 'timescaledb', 'redshift', 'databricks'
    ]

    # Cloud & DevOps
    cloud_patterns = [
        'aws', 'azure', 'gcp', 'google-cloud', 'digitalocean', 'vercel',
        'netlify', 'heroku', 'railway', 'render', 'cloudflare'
    ]

    # DevOps - ADD missing tools
    devops_patterns = [
        'docker', 'kubernetes', 'k8s', 'terraform', 'ansible', 'jenkins',
        'gitlab-ci', 'github-actions', 'prometheus', 'grafana', 'helm',
        # ADD: Missing DevOps tools from your JSON  
        'istio', 'pulumi', 'chef', 'puppet', 'vagrant', 'circleci', 'azure-devops',
        'tekton', 'argo', 'argocd', 'flux', 'jaeger', 'zipkin', 'datadog', 'newrelic', 'sentry'
    ]
    
    # Testing - NEW category from your JSON
    testing_patterns = [
        'jest', 'vitest', 'cypress', 'playwright', 'selenium', 'webdriver',
        'testing-library', 'mocha', 'chai', 'jasmine', 'pytest', 'unittest',
        'nose', 'junit', 'testng', 'rspec', 'minitest', 'phpunit'
    ]
    
    # Package managers - NEW category from your JSON
    package_manager_patterns = [
        'npm', 'yarn', 'pnpm', 'bun', 'pip', 'poetry', 'pipenv', 'conda',
        'maven', 'gradle', 'cargo', 'composer', 'bundler', 'nuget'
    ]
    
    # CSS frameworks - NEW category from your JSON
    css_framework_patterns = [
        'tailwind', 'tailwindcss', 'bootstrap', 'bulma', 'material-ui', 'mui',
        'chakra-ui', 'ant design', 'antd', 'mantine', 'shadcn', 'radix',
        'headlessui', 'styled-components', 'emotion'
    ]
    
    # Check patterns in order of specificity
    if any(pattern in tech_lower for pattern in ai_model_patterns):
        return 'ai_model'
    elif any(pattern in tech_lower for pattern in ai_framework_patterns):
        return 'ai_framework'
    elif any(pattern in tech_lower for pattern in blockchain_patterns):  # ADD THIS LINE
        return 'blockchain'
    elif any(pattern in tech_lower for pattern in testing_patterns):
        return 'tool'  # Testing tools go under 'tool'
    elif any(pattern in tech_lower for pattern in package_manager_patterns):
        return 'tool'  # Package managers go under 'tool'
    elif any(pattern in tech_lower for pattern in css_framework_patterns):
        return 'framework'  # CSS frameworks are still frameworks
    elif any(pattern in tech_lower for pattern in language_patterns):
        return 'language'
    elif any(pattern in tech_lower for pattern in web_framework_patterns):
        return 'framework'
    elif any(pattern in tech_lower for pattern in database_patterns):
        return 'database'
    elif any(pattern in tech_lower for pattern in cloud_patterns):
        return 'cloud'
    elif any(pattern in tech_lower for pattern in devops_patterns):
        return 'devops'
    else:
        return 'tool'  # Safe default

def classify_technology_for_gold(technology: str, daily_mentions: int) -> Dict[str, str]:
    """
    SIMPLIFIED Gold layer classification - Only basic categorization
    
    Gold layer should focus on:
    1. Technology categorization (ai_model, language, framework, etc.)
    2. Basic data validation and enrichment
    3. NOT complex business logic (that goes in Analytics)
    
    Args:
        technology: Technology name
        daily_mentions: Number of daily mentions (for basic validation)
    
    Returns:
        dict: Simple Gold classifications focused on data quality
    """
    try:
        # Core classification - the only real business logic for Gold
        tech_category = get_technology_category(technology)
        
        # Basic data quality indicators (simple, factual)
        data_completeness = 'complete' if daily_mentions > 0 else 'empty'
        
        # Simple mention volume tier (factual, not interpretive)
        if daily_mentions >= 1000:
            mention_volume = 'high'
        elif daily_mentions >= 100:
            mention_volume = 'medium'
        else:
            mention_volume = 'low'
        
        return {
            'technology_category': tech_category,
            'mention_volume': mention_volume,
            'data_completeness': data_completeness
        }
        
    except Exception as e:
        print(f"⚠️  Classification error for {technology}: {e}")
        # Return safe defaults
        return {
            'technology_category': 'tool',
            'mention_volume': 'low',
            'data_completeness': 'empty'
        }


# REMOVED: Complex business logic functions that belong in Analytics
# - get_activity_level_today() → Analytics layer
# - get_mention_tier_today() → Analytics layer  
# - Business intelligence calculations → Analytics layer
# - Growth/trend analysis → Analytics layer
# - Investment signals → Analytics layer


def get_category_distribution() -> Dict[str, int]:
    """Get distribution of technology categories from loaded keywords"""
    if not TECH_KEYWORDS:
        return {}
    
    category_counts = {}
    for tech, info in TECH_KEYWORDS.items():
        raw_type = info.get('type', 'tool')
        standardized_type = standardize_category_name(raw_type)
        category_counts[standardized_type] = category_counts.get(standardized_type, 0) + 1
    
    return dict(sorted(category_counts.items(), key=lambda x: x[1], reverse=True))


def test_simplified_classification():
    """Test the simplified Gold classification system"""
    test_technologies = [
        # AI/ML
        ('gpt-4', 1500),
        ('langchain', 800),
        ('pytorch', 1200),
        
        # Languages
        ('python', 2500),
        ('typescript', 1800),
        ('rust', 400),
        
        # Frameworks
        ('react', 2000),
        ('nextjs', 900),
        ('django', 700),
        
        # Databases
        ('postgresql', 800),
        ('mongodb', 600),
        ('redis', 450),
        
        # Cloud/DevOps
        ('kubernetes', 1100),
        ('docker', 1300),
        ('aws', 1000),
        
        # Tools
        ('git', 1500),
        ('npm', 800),
    ]
    
    print("🧪 SIMPLIFIED GOLD CLASSIFICATION TEST")
    print("=" * 50)
    
    # Show category distribution
    category_dist = get_category_distribution()
    print("📊 Category Distribution in Keywords File:")
    for category, count in list(category_dist.items())[:8]:
        print(f"   {category}: {count} technologies")
    
    print(f"\n🔧 Testing {len(test_technologies)} technologies:")
    print("-" * 50)
    
    for tech, mentions in test_technologies:
        result = classify_technology_for_gold(tech, mentions)
        
        print(f"✅ {tech} ({mentions} mentions):")
        print(f"   Category: {result['technology_category']}")
        print(f"   Volume: {result['mention_volume']}")
        print(f"   Completeness: {result['data_completeness']}")
        print()
    
    print("📋 SIMPLIFIED APPROACH:")
    print("   ✅ Gold = Basic categorization + data quality")
    print("   🧠 Analytics = Complex business logic + trends")
    print("   🎯 Result = Clean separation of concerns")


if __name__ == "__main__":
    # Test simplified classification
    test_simplified_classification()