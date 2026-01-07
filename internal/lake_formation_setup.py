#!/usr/bin/env python3
"""
AWS Lake Formation permission setup for Iceberg V2 to V3 Upgrade.
"""

import argparse
import boto3
from config import load_config


def get_account_id(config):
    """Get AWS account ID."""
    sts = boto3.client(
        'sts',
        region_name=config["AWS_REGION"],
        aws_access_key_id=config["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=config["AWS_SECRET_ACCESS_KEY"]
    )
    return sts.get_caller_identity()["Account"]


def grant_database_permissions(config, principal_arn, database_name):
    """Grant Lake Formation permissions on a database."""
    lf = boto3.client(
        'lakeformation',
        region_name=config["AWS_REGION"],
        aws_access_key_id=config["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=config["AWS_SECRET_ACCESS_KEY"]
    )
    account_id = get_account_id(config)
    
    print(f"Granting permissions on database '{database_name}' to {principal_arn}...")
    
    try:
        lf.grant_permissions(
            Principal={'DataLakePrincipalIdentifier': principal_arn},
            Resource={
                'Database': {
                    'CatalogId': account_id,
                    'Name': database_name
                }
            },
            Permissions=['ALL', 'ALTER', 'CREATE_TABLE', 'DESCRIBE', 'DROP'],
            PermissionsWithGrantOption=['ALL', 'ALTER', 'CREATE_TABLE', 'DESCRIBE', 'DROP']
        )
        print(f"  ✅ Database permissions granted")
    except Exception as e:
        if "AlreadyExists" in str(e):
            print(f"  ✓ Permissions already exist")
        else:
            print(f"  ❌ Error: {e}")
            return False
    
    return True


def grant_table_permissions(config, principal_arn, database_name, table_name):
    """Grant Lake Formation permissions on a table."""
    lf = boto3.client(
        'lakeformation',
        region_name=config["AWS_REGION"],
        aws_access_key_id=config["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=config["AWS_SECRET_ACCESS_KEY"]
    )
    account_id = get_account_id(config)
    
    print(f"Granting permissions on table '{database_name}.{table_name}' to {principal_arn}...")
    
    try:
        lf.grant_permissions(
            Principal={'DataLakePrincipalIdentifier': principal_arn},
            Resource={
                'Table': {
                    'CatalogId': account_id,
                    'DatabaseName': database_name,
                    'Name': table_name
                }
            },
            Permissions=['ALL', 'ALTER', 'DELETE', 'DESCRIBE', 'DROP', 'INSERT', 'SELECT'],
            PermissionsWithGrantOption=['ALL', 'ALTER', 'DELETE', 'DESCRIBE', 'DROP', 'INSERT', 'SELECT']
        )
        print(f"  ✅ Table permissions granted")
    except Exception as e:
        if "AlreadyExists" in str(e):
            print(f"  ✓ Permissions already exist")
        else:
            print(f"  ❌ Error: {e}")
            return False
    
    return True


def grant_iam_allowed_principals(config, database_name, table_name=None):
    """Grant IAM_ALLOWED_PRINCIPALS access (for IAM-based access)."""
    lf = boto3.client(
        'lakeformation',
        region_name=config["AWS_REGION"],
        aws_access_key_id=config["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=config["AWS_SECRET_ACCESS_KEY"]
    )
    account_id = get_account_id(config)
    
    principal = 'IAM_ALLOWED_PRINCIPALS'
    
    # Database permissions
    print(f"Granting IAM_ALLOWED_PRINCIPALS on database '{database_name}'...")
    try:
        lf.grant_permissions(
            Principal={'DataLakePrincipalIdentifier': principal},
            Resource={
                'Database': {
                    'CatalogId': account_id,
                    'Name': database_name
                }
            },
            Permissions=['DESCRIBE', 'CREATE_TABLE']
        )
        print(f"  ✅ Database IAM access granted")
    except Exception as e:
        if "AlreadyExists" in str(e):
            print(f"  ✓ Already exists")
        else:
            print(f"  ⚠️ {e}")
    
    # Table permissions (if specified)
    if table_name:
        print(f"Granting IAM_ALLOWED_PRINCIPALS on table '{database_name}.{table_name}'...")
        try:
            lf.grant_permissions(
                Principal={'DataLakePrincipalIdentifier': principal},
                Resource={
                    'Table': {
                        'CatalogId': account_id,
                        'DatabaseName': database_name,
                        'Name': table_name
                    }
                },
                Permissions=['SELECT', 'DESCRIBE', 'ALTER', 'DELETE', 'INSERT']
            )
            print(f"  ✅ Table IAM access granted")
        except Exception as e:
            if "AlreadyExists" in str(e):
                print(f"  ✓ Already exists")
            else:
                print(f"  ⚠️ {e}")


def register_s3_location(config, s3_path, role_arn=None):
    """Register an S3 location with Lake Formation."""
    lf = boto3.client(
        'lakeformation',
        region_name=config["AWS_REGION"],
        aws_access_key_id=config["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=config["AWS_SECRET_ACCESS_KEY"]
    )
    
    print(f"Registering S3 location: {s3_path}...")
    
    try:
        if role_arn:
            lf.register_resource(
                ResourceArn=s3_path,
                RoleArn=role_arn
            )
        else:
            lf.register_resource(ResourceArn=s3_path)
        print(f"  ✅ S3 location registered")
    except lf.exceptions.AlreadyExistsException:
        print(f"  ✓ Already registered")
    except Exception as e:
        print(f"  ⚠️ {e}")


def main():
    parser = argparse.ArgumentParser(description="Setup Lake Formation permissions")
    parser.add_argument("-d", "--database", required=True, help="Glue database name")
    parser.add_argument("-t", "--table", help="Glue table name (optional)")
    parser.add_argument("-p", "--principal", required=True, help="IAM role ARN to grant permissions to")
    parser.add_argument("--s3-path", help="S3 path to register (optional)")
    
    args = parser.parse_args()
    config = load_config()
    
    print("\n" + "=" * 60)
    print("LAKE FORMATION SETUP")
    print("=" * 60)
    
    # Grant database permissions
    grant_database_permissions(config, args.principal, args.database)
    
    # Grant table permissions if specified
    if args.table:
        grant_table_permissions(config, args.principal, args.database, args.table)
    
    # Grant IAM_ALLOWED_PRINCIPALS
    grant_iam_allowed_principals(config, args.database, args.table)
    
    # Register S3 location if specified
    if args.s3_path:
        register_s3_location(config, args.s3_path)
    
    print("\n✅ Lake Formation setup complete!")


if __name__ == "__main__":
    main()

