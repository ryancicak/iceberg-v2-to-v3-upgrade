#!/usr/bin/env python3
"""
Shared configuration loader for Iceberg V2 to V3 Upgrade Tool.
"""

import os
import uuid
from dotenv import load_dotenv


def load_config():
    """Load configuration from environment variables."""
    load_dotenv()
    
    # Generate unique suffix for demo resources
    unique_id = uuid.uuid4().hex[:8]
    
    config = {
        # Databricks
        "DATABRICKS_HOST": os.getenv("DATABRICKS_HOST"),
        "DATABRICKS_TOKEN": os.getenv("DATABRICKS_TOKEN"),
        
        # AWS
        "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "AWS_REGION": os.getenv("AWS_DEFAULT_REGION", "us-west-2"),
        
        # EMR
        "EMR_CLUSTER_ID": os.getenv("EMR_CLUSTER_ID"),
        "EMR_PEM_PATH": os.getenv("EMR_PEM_PATH"),
        
        # Demo/Table settings
        "S3_BUCKET": os.getenv("S3_BUCKET", f"iceberg-v3-upgrade-demo-{unique_id}"),
        "GLUE_DATABASE": os.getenv("GLUE_DATABASE", f"iceberg_v3_demo_{unique_id}"),
        
        # Federation
        "CATALOG_NAME": os.getenv("CATALOG_NAME"),
    }
    
    return config


def validate_config(config, required_keys=None):
    """Validate that required configuration keys are present."""
    if required_keys is None:
        required_keys = [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_REGION",
            "EMR_CLUSTER_ID",
            "EMR_PEM_PATH",
        ]
    
    missing = []
    for key in required_keys:
        if not config.get(key):
            missing.append(key)
    
    if missing:
        raise ValueError(f"Missing required configuration: {', '.join(missing)}")
    
    return True


def print_config(config, mask_secrets=True):
    """Print configuration (masking sensitive values)."""
    print("\n" + "=" * 60)
    print("CONFIGURATION")
    print("=" * 60)
    
    secret_keys = ["TOKEN", "SECRET", "PASSWORD", "KEY"]
    
    for key, value in config.items():
        if value is None:
            display = "(not set)"
        elif mask_secrets and any(s in key.upper() for s in secret_keys):
            display = "*" * 8 + str(value)[-4:] if value else "(not set)"
        else:
            display = value
        print(f"  {key}: {display}")
    
    print("=" * 60 + "\n")


if __name__ == "__main__":
    try:
        cfg = load_config()
        print_config(cfg)
        validate_config(cfg)
        print("[OK] Configuration is valid!")
    except ValueError as e:
        print(f"[ERROR] Configuration error: {e}")

