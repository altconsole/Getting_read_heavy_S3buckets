import boto3
import pandas as pd

def get_s3_bucket_objects_count(bucket_name):
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    count = response.get('KeyCount', 0)
    return count

def get_all_s3_buckets():
    s3_client = boto3.client('s3')
    response = s3_client.list_buckets()
    buckets = response['Buckets']
    return buckets

def is_bucket_read_heavy(bucket_name, s3_client):
    # Get the S3 bucket access logs prefix
    response = s3_client.get_bucket_logging(Bucket=bucket_name)
    if 'LoggingEnabled' in response:
        log_prefix = response['LoggingEnabled']['TargetPrefix']
        log_bucket = response['LoggingEnabled']['TargetBucket']
        
        # Check if there are access logs
        response = s3_client.list_objects_v2(Bucket=log_bucket, Prefix=log_prefix)
        if 'KeyCount' in response and response['KeyCount'] > 0:
            return True
    
    return False

# AWS S3 Configuration
aws_profile = 'YOUR_PROFILE'
region_name = 'YOUR_REGION'

# Initialize AWS clients using AWS profile
session = boto3.Session(profile_name=aws_profile, region_name=region_name)
s3_client = session.client('s3')

# Get all S3 buckets
buckets = get_all_s3_buckets()

# Calculate object counts for each bucket
bucket_object_counts = {}
read_heavy_buckets = []

for bucket in buckets:
    bucket_name = bucket['Name']
    object_count = get_s3_bucket_objects_count(bucket_name)
    bucket_object_counts[bucket_name] = object_count
    
    if is_bucket_read_heavy(bucket_name, s3_client):
        read_heavy_buckets.append(bucket_name)

# Create a Pandas DataFrame with bucket names and object counts
df = pd.DataFrame(list(bucket_object_counts.items()), columns=['Bucket Name', 'Object Count'])

# Save the DataFrame to an Excel file
output_file = 's3_bucket_objects.xlsx'
df.to_excel(output_file, index=False)

# Create a Pandas DataFrame with read-heavy bucket names
df_read_heavy = pd.DataFrame(read_heavy_buckets, columns=['Read-Heavy Buckets'])

# Save the DataFrame to the same Excel file, in a separate sheet
with pd.ExcelWriter(output_file, engine='openpyxl', mode='a') as writer:
    df_read_heavy.to_excel(writer, sheet_name='Read-Heavy Buckets', index=False)
