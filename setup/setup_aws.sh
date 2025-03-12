#!/bin/bash
# create_buckets.sh

# Check if .env exists and source it
if [ -f .env ]; then
  echo "Loading environment variables from .env..."
  source .env
else
  echo "Error: .env file not found!"
  exit 1
fi

# Set the default AWS region to Paris (eu-west-3)
export AWS_DEFAULT_REGION="eu-west-3"
echo "AWS_DEFAULT_REGION set to $AWS_DEFAULT_REGION"

# Configure AWS CLI with the credentials and region
echo "Configuring AWS CLI..."
aws configure set aws_access_key_id "${AWS_ACCESS_KEY_ID}"
aws configure set aws_secret_access_key "${AWS_SECRET_ACCESS_KEY}"
aws configure set default.region "${AWS_DEFAULT_REGION}"

# Define LocalStack endpoint
LOCALSTACK_ENDPOINT="http://localhost:4566"


echo "Creating bucket: staging"
aws --endpoint-url=$LOCALSTACK_ENDPOINT s3 mb s3://staging

echo "Bucket creation complete."
