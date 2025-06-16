#!/usr/bin/env python3
"""
Auto-initialize Airflow connections on startup
Place in include/utils/ and call from airflow-init
"""

import os
import yaml
from airflow import settings
from airflow.models import Connection

def load_connections_from_yaml():
    """Load connections from YAML config file"""
    config_path = '/opt/airflow/config/airflow_connections.yaml'
    
    if not os.path.exists(config_path):
        print(f"⚠️  No connections config found at {config_path}")
        return
    
    with open(config_path, 'r') as f:
        connections_config = yaml.safe_load(f)
    
    session = settings.Session()
    
    for conn_id, conn_data in connections_config.items():
        # Check if connection already exists
        existing = session.query(Connection).filter(Connection.conn_id == conn_id).first()
        
        if existing:
            print(f"✅ Connection '{conn_id}' already exists")
            continue
        
        # Create new connection
        new_conn = Connection(
            conn_id=conn_id,
            conn_type=conn_data.get('conn_type'),
            host=conn_data.get('host'),
            port=conn_data.get('port'),
            login=conn_data.get('login'),
            password=conn_data.get('password'),
            schema=conn_data.get('schema'),
            extra=conn_data.get('extra')
        )
        
        session.add(new_conn)
        print(f"✅ Created connection '{conn_id}'")
    
    session.commit()
    session.close()
    print("🎉 Connections setup complete!")

if __name__ == "__main__":
    load_connections_from_yaml()