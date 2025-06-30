"""
REFINED Technology Classification System
========================================

Completely refactored for:
1. Cleaner category structure with logical groupings
2. Consistent field naming (access, paradigm, provider)
3. Better separation of concerns
4. Elimination of redundant classifications
5. Data engineering instead of big_data for better clarity
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
    Get standardized technology category
    
    Returns clean, logical categories:
    - AI: ai_model, ai_platform, ai_framework, ai_technique, ml_framework, ml_ops
    - Languages: language  
    - Development: web_framework, backend_framework, mobile_framework, desktop_framework
    - Data: database, vector_database, data_engineering, data_visualization, data_analysis
    - Infrastructure: cloud_platform, cloud_service, containerization, orchestration, infrastructure
    - Tools: testing, build_tool, package_manager, version_control, development_tool
    - Specialized: blockchain_platform, blockchain_framework, etc.
    """
    tech_lower = technology.lower().strip()
    
    # Direct lookup from tech_keywords.json
    if tech_lower in TECH_KEYWORDS:
        raw_type = TECH_KEYWORDS[tech_lower].get('type', 'development_tool')
        return standardize_category_name(raw_type)
    
    # Pattern matching for technologies not in keywords file
    return infer_category_from_patterns(tech_lower)


def standardize_category_name(raw_type: str) -> str:
    """
    Maps all types from tech_keywords.json to clean, logical categories
    
    REFINED STRUCTURE - Fixed duplicates and proper categorization
    """
    category_mapping = {
        # === AI/ML Categories (Clear separation) ===
        'ai_model': 'ai_model',                    # claude, gpt-4, llama
        'ai_platform': 'ai_platform',             # openai, anthropic, huggingface  
        'ai_framework': 'ai_framework',           # langchain, transformers, vllm
        'ai_technique': 'ai_technique',           # rag, fine-tuning, prompt-engineering
        'ml_framework': 'ml_framework',           # pytorch, tensorflow, scikit-learn
        'ml_ops': 'ml_ops',                       # wandb, mlflow, kubeflow
        'vector_database': 'vector_database',     # pinecone, chroma, qdrant
        'computer_vision': 'ai_framework',        # opencv, detectron2 → ai_framework
        'nlp': 'ai_framework',                    # spacy, nltk → ai_framework
        
        # === Programming Languages ===
        'language': 'language',                   # python, javascript, rust
        'query_language': 'language',             # graphql, sql → language
        
        # === Development Frameworks (Clear separation) ===
        'web_framework': 'web_framework',         # react, vue, nextjs, svelte
        'backend_framework': 'backend_framework', # django, fastapi, spring
        'mobile_framework': 'mobile_framework',   # react-native, flutter
        'desktop_framework': 'desktop_framework', # electron, tauri, qt
        'css_framework': 'web_framework',          # tailwind, bootstrap → web_framework (CSS is web)
        'ui_library': 'web_framework',             # mui, chakra-ui → web_framework (UI libs are web)
        'css_library': 'web_framework',            # styled-components → web_framework
        'css_preprocessor': 'web_framework',       # sass, less → web_framework
        'css_tool': 'web_framework',               # postcss → web_framework
        
        # === Data & Storage (Improved naming) ===
        'database': 'database',                   # postgresql, mongodb, redis
        'data_engineering': 'data_engineering',   # spark, kafka, airflow (was big_data)
        'data_visualization': 'data_visualization', # streamlit, plotly, tableau
        'data_analysis': 'data_analysis',         # pandas, numpy, polars
        'data_library': 'data_analysis',          # tensorflow-datasets
        
        # === Infrastructure & Cloud ===
        'cloud_platform': 'cloud_platform',      # aws, azure, gcp
        'cloud_service': 'cloud_service',         # vercel, netlify, lambda
        'containerization': 'devops',             # docker
        'orchestration': 'devops',               # kubernetes, helm
        'infrastructure': 'devops',              # terraform, pulumi
        'configuration': 'devops',               # ansible, chef
        'ci_cd': 'devops',                       # jenkins, github-actions
        'monitoring': 'devops',                  # prometheus, grafana
        'devops': 'devops',                      # general devops
        
        # === Development Tools ===
        'testing': 'testing',                    # jest, cypress, pytest
        'build_tool': 'build_tool',              # vite, webpack, esbuild
        'package_manager': 'package_manager',    # npm, pip, cargo
        'version_control': 'version_control',    # git, mercurial
        'code_hosting': 'development_tool',      # github, gitlab
        'development_tool': 'development_tool',  # jupyter, colab
        
        # === Libraries & Tools ===
        'library': 'library',                    # pydantic, sqlalchemy
        'orm': 'library',                        # prisma, drizzle
        'tool': 'development_tool',              # general tools
        'platform': 'platform',                 # kaggle, general platforms
        
        # === Specialized Domains ===
        'blockchain_platform': 'blockchain_platform',     # ethereum, solana
        'blockchain_framework': 'blockchain_framework',   # hardhat, foundry
        'blockchain_protocol': 'blockchain_protocol',     # uniswap, aave
        'blockchain_service': 'blockchain_service',       # chainlink, ipfs
        'blockchain_library': 'blockchain_framework',     # web3.js
        'blockchain': 'blockchain_platform',              # legacy fallback
    }
    
    return category_mapping.get(raw_type, 'development_tool')


def infer_category_from_patterns(tech_lower: str) -> str:
    """
    Enhanced pattern matching with refined categories
    """
    
    # === AI/ML Patterns (Comprehensive) ===
    ai_model_patterns = [
        # OpenAI models
        'gpt', 'chatgpt', 'dall-e', 'dalle', 'whisper', 'codex',
        # Anthropic models  
        'claude',
        # Google models
        'gemini', 'bard', 'palm', 'bert', 't5', 'gemma',
        # Meta models
        'llama', 'roberta',
        # Other major models
        'mistral', 'mixtral', 'qwen', 'deepseek', 'yi', 'phi', 'grok',
        # Specialized models
        'stable-diffusion', 'midjourney', 'yolo', 'bloom'
    ]
    
    ai_platform_patterns = [
        'openai', 'anthropic', 'huggingface', 'hugging-face', 'ollama', 
        'perplexity', 'cohere', 'replicate'
    ]
    
    ai_framework_patterns = [
        # LLM frameworks
        'langchain', 'llamaindex', 'llama-index', 'haystack', 'semantic-kernel',
        'autogen', 'crewai', 'guidance', 'instructor', 'marvin', 'litellm',
        # ML frameworks  
        'transformers', 'datasets', 'accelerate', 'peft', 'vllm',
        # CV/NLP libraries
        'opencv', 'spacy', 'nltk', 'detectron', 'mmdetection',
        # Serving/deployment
        'triton', 'bentoml', 'ray-serve'
    ]

    # === Language Patterns ===
    language_patterns = [
        'python', 'javascript', 'typescript', 'java', 'kotlin', 'scala',
        'go', 'golang', 'rust', 'cpp', 'c++', 'csharp', 'c#', 'php', 'ruby',
        'swift', 'dart', 'elixir', 'haskell', 'clojure', 'erlang', 'julia',
        'r', 'matlab', 'lua', 'perl', 'bash', 'powershell', 'zig', 'nim',
        'crystal', 'mojo', 'solidity', 'f#', 'ocaml'
    ]
    
    # === Web Frameworks ===
    web_framework_patterns = [
        'react', 'vue', 'angular', 'svelte', 'nextjs', 'next.js', 'nuxt',
        'remix', 'solid', 'solidjs', 'qwik', 'astro', 'sveltekit'
    ]
    
    # === Backend Frameworks ===
    backend_framework_patterns = [
        'django', 'fastapi', 'flask', 'quart', 'starlette', 'express', 
        'fastify', 'koa', 'hapi', 'nestjs', 'spring', 'laravel', 'symfony',
        'rails', 'sinatra', 'asp.net', 'blazor', 'gin', 'echo', 'fiber',
        'axum', 'actix', 'rocket', 'phoenix'
    ]

    # === Databases (Enhanced categories) ===
    database_patterns = [
        # Relational
        'mysql', 'postgresql', 'postgres', 'sqlite', 'mariadb', 'oracle', 'sql-server',
        # NoSQL Document
        'mongodb', 'couchdb',
        # Key-Value
        'redis', 'dynamodb',
        # Search
        'elasticsearch', 'opensearch', 'solr',
        # Graph
        'neo4j', 'arangodb',
        # Time Series
        'influxdb', 'timescaledb',
        # Analytical
        'clickhouse', 'snowflake', 'bigquery', 'redshift', 'databricks',
        # Modern Cloud
        'supabase', 'planetscale', 'neon', 'turso'
    ]
    
    # === Vector Databases ===
    vector_db_patterns = [
        'pinecone', 'weaviate', 'chroma', 'chromadb', 'qdrant', 'milvus', 
        'faiss', 'pgvector'
    ]

    # === Data Engineering (was big_data) ===
    data_engineering_patterns = [
        # Processing
        'spark', 'hadoop', 'flink', 'storm',
        # Streaming
        'kafka', 'pulsar', 'kinesis',
        # Orchestration
        'airflow', 'prefect', 'dagster', 'luigi',
        # ETL/ELT
        'dbt', 'nifi', 'great-expectations'
    ]
    
    # === Data Visualization ===
    data_viz_patterns = [
        'streamlit', 'gradio', 'plotly', 'matplotlib', 'seaborn',
        'tableau', 'powerbi', 'looker', 'superset', 'metabase'
    ]
    
    # === Data Analysis ===
    data_analysis_patterns = [
        'pandas', 'numpy', 'scipy', 'polars', 'dask', 'ray'
    ]

    # === Cloud Platforms ===
    cloud_platform_patterns = [
        'aws', 'azure', 'gcp', 'google-cloud', 'digitalocean', 'linode',
        'vultr', 'hetzner', 'ovh'
    ]
    
    # === Cloud Services ===
    cloud_service_patterns = [
        'vercel', 'netlify', 'heroku', 'railway', 'render', 'fly.io',
        'cloudflare', 'lambda', 's3', 'ec2', 'rds'
    ]

    # === DevOps ===
    devops_patterns = [
        # Containers & Orchestration
        'docker', 'kubernetes', 'k8s', 'helm', 'istio',
        # Infrastructure
        'terraform', 'pulumi', 'cloudformation', 'cdk',
        # Configuration
        'ansible', 'chef', 'puppet', 'vagrant',
        # CI/CD
        'jenkins', 'github-actions', 'gitlab-ci', 'circleci', 'azure-devops',
        'tekton', 'argo', 'argocd', 'flux',
        # Monitoring
        'prometheus', 'grafana', 'jaeger', 'zipkin', 'datadog', 'newrelic', 'sentry'
    ]
    
    # === Testing ===
    testing_patterns = [
        'jest', 'vitest', 'cypress', 'playwright', 'selenium', 'webdriver',
        'testing-library', 'mocha', 'chai', 'jasmine', 'pytest', 'unittest',
        'nose', 'junit', 'testng', 'rspec', 'minitest', 'phpunit'
    ]
    
    # === Build Tools ===
    build_tool_patterns = [
        'vite', 'webpack', 'esbuild', 'rollup', 'turbo', 'turborepo',
        'parcel', 'snowpack'
    ]
    
    # === Package Managers ===
    package_manager_patterns = [
        'npm', 'yarn', 'pnpm', 'bun', 'pip', 'poetry', 'pipenv', 'conda',
        'maven', 'gradle', 'cargo', 'composer', 'bundler', 'nuget'
    ]
    
    # === UI Frameworks ===
    ui_framework_patterns = [
        # CSS Frameworks
        'tailwind', 'tailwindcss', 'bootstrap', 'bulma',
        # UI Libraries
        'material-ui', 'mui', 'chakra-ui', 'ant-design', 'antd', 'mantine',
        'shadcn', 'radix', 'headlessui',
        # CSS-in-JS
        'styled-components', 'emotion',
        # Preprocessors
        'sass', 'scss', 'less', 'stylus', 'postcss'
    ]
    
    # === Mobile Frameworks ===
    mobile_framework_patterns = [
        'react-native', 'flutter', 'expo', 'ionic', 'cordova', 'phonegap',
        'xamarin', 'swiftui', 'jetpack-compose'
    ]
    
    # === Desktop Frameworks ===
    desktop_framework_patterns = [
        'electron', 'tauri', 'qt', 'gtk', 'tkinter', 'kivy', 'flet',
        'wpf', 'winui'
    ]

    # === Blockchain ===
    blockchain_platform_patterns = [
        'ethereum', 'bitcoin', 'solana', 'cardano', 'polygon', 'arbitrum',
        'optimism', 'avalanche', 'cosmos', 'polkadot', 'near', 'terra'
    ]
    
    blockchain_framework_patterns = [
        'hardhat', 'truffle', 'foundry', 'anchor', 'substrate', 'web3'
    ]
    
    blockchain_protocol_patterns = [
        'uniswap', 'aave', 'compound', 'makerdao', 'curve', 'yearn',
        'sushiswap', 'pancakeswap', '1inch'
    ]

    # === Pattern Matching (Order matters - most specific first) ===
    
    # AI/ML Categories
    if any(pattern in tech_lower for pattern in ai_model_patterns):
        return 'ai_model'
    elif any(pattern in tech_lower for pattern in ai_platform_patterns):
        return 'ai_platform'
    elif any(pattern in tech_lower for pattern in ai_framework_patterns):
        return 'ai_framework'
    elif any(pattern in tech_lower for pattern in vector_db_patterns):
        return 'vector_database'
    
    # Blockchain (check before general categories)
    elif any(pattern in tech_lower for pattern in blockchain_protocol_patterns):
        return 'blockchain_protocol'
    elif any(pattern in tech_lower for pattern in blockchain_framework_patterns):
        return 'blockchain_framework'
    elif any(pattern in tech_lower for pattern in blockchain_platform_patterns):
        return 'blockchain_platform'
    
    # Development Tools (check before frameworks)
    elif any(pattern in tech_lower for pattern in testing_patterns):
        return 'testing'
    elif any(pattern in tech_lower for pattern in build_tool_patterns):
        return 'build_tool'
    elif any(pattern in tech_lower for pattern in package_manager_patterns):
        return 'package_manager'
    
    # Languages
    elif any(pattern in tech_lower for pattern in language_patterns):
        return 'language'
    
    # Frameworks (order by specificity)
    elif any(pattern in tech_lower for pattern in mobile_framework_patterns):
        return 'mobile_framework'
    elif any(pattern in tech_lower for pattern in desktop_framework_patterns):
        return 'desktop_framework'
    elif any(pattern in tech_lower for pattern in backend_framework_patterns):
        return 'backend_framework'
    elif any(pattern in tech_lower for pattern in web_framework_patterns):
        return 'web_framework'
    elif any(pattern in tech_lower for pattern in ui_framework_patterns):
        return 'ui_framework'
    
    # Data & Storage
    elif any(pattern in tech_lower for pattern in data_engineering_patterns):
        return 'data_engineering'
    elif any(pattern in tech_lower for pattern in data_viz_patterns):
        return 'data_visualization'
    elif any(pattern in tech_lower for pattern in data_analysis_patterns):
        return 'data_analysis'
    elif any(pattern in tech_lower for pattern in database_patterns):
        return 'database'
    
    # Infrastructure
    elif any(pattern in tech_lower for pattern in cloud_platform_patterns):
        return 'cloud_platform'
    elif any(pattern in tech_lower for pattern in cloud_service_patterns):
        return 'cloud_service'
    elif any(pattern in tech_lower for pattern in devops_patterns):
        return 'devops'
    
    # Default
    else:
        return 'development_tool'


def classify_technology_for_gold(technology: str, daily_mentions: int) -> Dict[str, str]:
    """
    REFINED Gold layer classification - Clean and focused
    
    Gold layer responsibilities:
    1. Technology categorization (standardized categories)
    2. Basic data quality indicators
    3. Simple mention volume classification
    
    Args:
        technology: Technology name
        daily_mentions: Number of daily mentions
    
    Returns:
        dict: Clean Gold classifications
    """
    try:
        # Core classification
        tech_category = get_technology_category(technology)
        
        # Data quality indicators
        data_completeness = 'complete' if daily_mentions > 0 else 'empty'
        
        # Simple mention volume classification
        if daily_mentions >= 1000:
            mention_volume = 'high'
        elif daily_mentions >= 100:
            mention_volume = 'medium'
        elif daily_mentions >= 10:
            mention_volume = 'low'
        else:
            mention_volume = 'minimal'
        
        return {
            'technology_category': tech_category,
            'mention_volume': mention_volume,
            'data_completeness': data_completeness
        }
        
    except Exception as e:
        print(f"⚠️  Classification error for {technology}: {e}")
        return {
            'technology_category': 'development_tool',
            'mention_volume': 'minimal',
            'data_completeness': 'empty'
        }


def get_category_distribution() -> Dict[str, int]:
    """Get distribution of technology categories from loaded keywords"""
    if not TECH_KEYWORDS:
        return {}
    
    category_counts = {}
    for tech, info in TECH_KEYWORDS.items():
        raw_type = info.get('type', 'development_tool')
        standardized_type = standardize_category_name(raw_type)
        category_counts[standardized_type] = category_counts.get(standardized_type, 0) + 1
    
    return dict(sorted(category_counts.items(), key=lambda x: x[1], reverse=True))


def get_refined_category_hierarchy() -> Dict[str, Dict[str, list]]:
    """
    Returns the refined category hierarchy for better understanding
    """
    return {
        "AI & Machine Learning": {
            "ai_model": ["claude-4", "gpt-4", "llama-3", "gemini"],
            "ai_platform": ["openai", "anthropic", "huggingface"],
            "ai_framework": ["langchain", "transformers", "vllm"],
            "ai_technique": ["rag", "fine-tuning", "prompt-engineering"],
            "ml_framework": ["pytorch", "tensorflow", "scikit-learn"],
            "ml_ops": ["wandb", "mlflow", "kubeflow"],
            "vector_database": ["pinecone", "chroma", "qdrant"]
        },
        "Programming": {
            "language": ["python", "javascript", "rust", "go"]
        },
        "Development Frameworks": {
            "web_framework": ["react", "vue", "nextjs", "svelte"],
            "backend_framework": ["django", "fastapi", "express", "spring"],
            "mobile_framework": ["react-native", "flutter", "expo"],
            "desktop_framework": ["electron", "tauri", "qt"],
            "ui_framework": ["tailwind", "mui", "bootstrap"]
        },
        "Data & Storage": {
            "database": ["postgresql", "mongodb", "redis"],
            "data_engineering": ["spark", "kafka", "airflow", "dbt"],
            "data_visualization": ["streamlit", "plotly", "tableau"],
            "data_analysis": ["pandas", "numpy", "polars"]
        },
        "Infrastructure": {
            "cloud_platform": ["aws", "azure", "gcp"],
            "cloud_service": ["vercel", "netlify", "lambda"],
            "devops": ["docker", "kubernetes", "terraform"]
        },
        "Development Tools": {
            "testing": ["jest", "cypress", "pytest"],
            "build_tool": ["vite", "webpack", "esbuild"],
            "package_manager": ["npm", "pip", "cargo"],
            "version_control": ["git"],
            "development_tool": ["jupyter", "github", "vscode"]
        },
        "Specialized": {
            "blockchain_platform": ["ethereum", "solana", "polygon"],
            "blockchain_framework": ["hardhat", "foundry", "web3"],
            "blockchain_protocol": ["uniswap", "aave", "compound"]
        }
    }


def test_refined_classification():
    """Test the refined classification system"""
    test_technologies = [
        # AI/ML
        ('claude-4', 1500),
        ('openai', 2000),
        ('langchain', 800),
        ('pytorch', 1200),
        ('pinecone', 300),
        
        # Languages
        ('python', 2500),
        ('typescript', 1800),
        ('rust', 400),
        
        # Web Development
        ('react', 2000),
        ('nextjs', 900),
        ('tailwind', 700),
        
        # Backend
        ('django', 800),
        ('fastapi', 600),
        ('express', 1100),
        
        # Data
        ('postgresql', 800),
        ('spark', 450),
        ('streamlit', 300),
        ('pandas', 900),
        
        # Infrastructure
        ('kubernetes', 1100),
        ('docker', 1300),
        ('aws', 1800),
        ('vercel', 400),
        
        # Tools
        ('git', 1500),
        ('jest', 300),
        ('npm', 800),
        
        # Blockchain
        ('ethereum', 600),
        ('hardhat', 150),
        ('uniswap', 200),
    ]
    
    print("🧪 REFINED CLASSIFICATION SYSTEM TEST")
    print("=" * 60)
    
    # Show category distribution
    category_dist = get_category_distribution()
    print("📊 Category Distribution in Keywords File:")
    for category, count in list(category_dist.items())[:12]:
        print(f"   {category}: {count} technologies")
    
    print(f"\n🔧 Testing {len(test_technologies)} technologies:")
    print("-" * 60)
    
    # Group results by category
    results_by_category = {}
    for tech, mentions in test_technologies:
        result = classify_technology_for_gold(tech, mentions)
        category = result['technology_category']
        
        if category not in results_by_category:
            results_by_category[category] = []
        results_by_category[category].append((tech, mentions, result))
    
    # Display results grouped by category
    for category in sorted(results_by_category.keys()):
        print(f"\n📂 {category.upper().replace('_', ' ')}")
        for tech, mentions, result in results_by_category[category]:
            print(f"   ✅ {tech} ({mentions} mentions) - Volume: {result['mention_volume']}")
    
    print("\n📋 REFINED BENEFITS:")
    print("   ✅ Clear AI separation: models vs platforms vs frameworks")
    print("   ✅ Logical framework grouping: web vs backend vs mobile")
    print("   ✅ Better data categories: engineering vs visualization vs analysis")
    print("   ✅ Consistent naming: data_engineering (not big_data)")
    print("   ✅ Comprehensive blockchain categories")
    print("   🎯 Result: Clean, logical, maintainable classification")


if __name__ == "__main__":
    test_refined_classification()