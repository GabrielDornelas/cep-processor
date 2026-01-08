# AWS Deployment Guide

This guide describes how to deploy the CEP Processor to AWS using AWS Glue and AWS Lambda.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Infrastructure Setup](#infrastructure-setup)
3. [Deploy AWS Glue Job](#deploy-aws-glue-job)
4. [Deploy AWS Lambda](#deploy-aws-lambda)
5. [Configure Triggers](#configure-triggers)
6. [Monitoring and Troubleshooting](#monitoring-and-troubleshooting)

## Prerequisites

- AWS account with appropriate permissions
- AWS CLI configured
- Python 3.11+ installed locally
- Access to:
  - AWS Glue
  - AWS Lambda
  - Amazon S3
  - Amazon RDS (PostgreSQL)
  - AWS IAM (for creating roles)

## Infrastructure Setup

### 1. Create S3 Buckets

```bash
# Create buckets for code and data
aws s3 mb s3://cep-processor-code-<your-account-id>
aws s3 mb s3://cep-processor-data-<your-account-id>
```

### 2. Create RDS PostgreSQL

```bash
# Via AWS Console or CLI
aws rds create-db-instance \
  --db-instance-identifier cep-processor-db \
  --db-instance-class db.t3.micro \
  --engine postgres \
  --master-username cep_user \
  --master-user-password <your-password> \
  --allocated-storage 20 \
  --vpc-security-group-ids <your-security-group-id>
```

**Important**: Configure the Security Group to allow connections from Glue/Lambda.

### 3. Create IAM Roles

#### Role for Glue Job

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::cep-processor-data-*/*",
        "arn:aws:s3:::cep-processor-code-*/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:*"
    }
  ]
}
```

#### Role for Lambda

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject"],
      "Resource": "arn:aws:s3:::cep-processor-data-*/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:*"
    },
    {
      "Effect": "Allow",
      "Action": ["rds:DescribeDBInstances"],
      "Resource": "*"
    }
  ]
}
```

## Deploy AWS Glue Job

### 1. Prepare Code

```bash
# Create temporary directory
mkdir -p glue_deploy
cd glue_deploy

# Copy necessary code
cp -r ../src .
cp ../aws/glue_job.py .
cp ../requirements.txt .

# Create initialization file
cat > __init__.py << EOF
# Glue job package
EOF
```

### 2. Create Python Package

```bash
# Install dependencies in local directory
pip install -r requirements.txt -t .

# Remove unnecessary files
find . -type d -name "__pycache__" -exec rm -r {} + 2>/dev/null || true
find . -type d -name "*.dist-info" -exec rm -r {} + 2>/dev/null || true

# Create ZIP
zip -r glue_job.zip . -x "*.pyc" -x "*.pyo"
```

### 3. Upload to S3

```bash
aws s3 cp glue_job.zip s3://cep-processor-code-<your-account-id>/glue_job.zip
aws s3 cp glue_job.py s3://cep-processor-code-<your-account-id>/glue_job.py
```

### 4. Create Glue Job via Console

1. Access AWS Glue Console
2. Click "Jobs" → "Add job"
3. Configure:

   - **Name**: `cep-processor-job`
   - **IAM Role**: Role created earlier
   - **Type**: Spark
   - **Glue version**: 4.0
   - **Language**: Python 3
   - **Script path**: `s3://cep-processor-code-<your-account-id>/glue_job.py`
   - **Python library path**: `s3://cep-processor-code-<your-account-id>/glue_job.zip`
   - **Temporary directory**: `s3://cep-processor-data-<your-account-id>/temp/`

4. In "Job parameters", add:

   ```
   --S3_INPUT_PATH=s3://cep-processor-data-<your-account-id>/input/
   --S3_OUTPUT_PATH=s3://cep-processor-data-<your-account-id>/output/
   --DATABASE_URL=postgresql://user:password@rds-endpoint:5432/cep_processor
   --RATE_LIMIT_PER_SECOND=2.0
   ```

5. In "Advanced properties" → "Environment variables", add:
   ```
   VIACEP_BASE_URL=https://viacep.com.br/ws
   REQUEST_TIMEOUT=30
   RETRY_ATTEMPTS=3
   ```

### 5. Run Job

```bash
# Via CLI
aws glue start-job-run --job-name cep-processor-job

# Or schedule via EventBridge (see Triggers section)
```

## Deploy AWS Lambda

### 1. Prepare Code

```bash
# Create temporary directory
mkdir -p lambda_deploy
cd lambda_deploy

# Copy necessary code
cp -r ../src .
cp ../aws/lambda_function.py lambda_function.py
cp ../requirements.txt .

# Install dependencies (boto3 is already included in Lambda)
grep -v "boto3" requirements.txt > requirements_lambda.txt
pip install -r requirements_lambda.txt -t .

# Remove unnecessary files
find . -type d -name "__pycache__" -exec rm -r {} + 2>/dev/null || true
find . -type d -name "*.dist-info" -exec rm -r {} + 2>/dev/null || true
find . -type d -name "tests" -exec rm -r {} + 2>/dev/null || true

# Create ZIP
zip -r lambda_function.zip . -x "*.pyc" -x "*.pyo"
```

### 2. Create Lambda Function

```bash
# Create function
aws lambda create-function \
  --function-name cep-processor-lambda \
  --runtime python3.11 \
  --role arn:aws:iam::<your-account-id>:role/lambda-execution-role \
  --handler lambda_function.lambda_handler \
  --zip-file fileb://lambda_function.zip \
  --timeout 900 \
  --memory-size 512 \
  --environment Variables="{
    DATABASE_URL=postgresql://user:password@rds-endpoint:5432/cep_processor,
    VIACEP_BASE_URL=https://viacep.com.br/ws,
    REQUEST_TIMEOUT=30,
    RETRY_ATTEMPTS=3
  }"

# Or update existing function
aws lambda update-function-code \
  --function-name cep-processor-lambda \
  --zip-file fileb://lambda_function.zip
```

### 3. Configure VPC (if RDS is in VPC)

```bash
aws lambda update-function-configuration \
  --function-name cep-processor-lambda \
  --vpc-config SubnetIds=subnet-xxx,subnet-yyy,SecurityGroupIds=sg-xxx
```

## Configure Triggers

### 1. S3 Trigger (Lambda)

When a CSV is uploaded to S3, process automatically:

```bash
aws lambda add-permission \
  --function-name cep-processor-lambda \
  --statement-id s3-trigger \
  --action lambda:InvokeFunction \
  --principal s3.amazonaws.com \
  --source-arn arn:aws:s3:::cep-processor-data-<your-account-id>

# Configure via Console:
# S3 → Bucket → Properties → Event notifications
# Add notification → Lambda function → cep-processor-lambda
# Prefix: input/
# Suffix: .csv
```

### 2. EventBridge Schedule (Glue or Lambda)

Run daily:

```bash
# Create rule
aws events put-rule \
  --name cep-processor-daily \
  --schedule-expression "cron(0 2 * * ? *)" \
  --state ENABLED

# Add target (Glue)
aws events put-targets \
  --rule cep-processor-daily \
  --targets "Id=1,Arn=arn:aws:glue:region:account:job/cep-processor-job"

# Or Lambda
aws events put-targets \
  --rule cep-processor-daily \
  --targets "Id=1,Arn=arn:aws:lambda:region:account:function:cep-processor-lambda"
```

### 3. API Gateway (Lambda)

Create REST API for on-demand CEP processing:

```bash
# Create API
aws apigateway create-rest-api --name cep-processor-api

# Configure POST /process
# Integration: Lambda Function → cep-processor-lambda
```

### 4. SQS Queue (Lambda)

Process CEPs from a queue:

```bash
# Create queue
aws sqs create-queue --queue-name cep-processing-queue

# Configure Lambda trigger
aws lambda create-event-source-mapping \
  --function-name cep-processor-lambda \
  --event-source-arn arn:aws:sqs:region:account:cep-processing-queue \
  --batch-size 10
```

## Monitoring and Troubleshooting

### CloudWatch Logs

```bash
# View Glue logs
aws logs tail /aws-glue/jobs/output --follow

# View Lambda logs
aws logs tail /aws/lambda/cep-processor-lambda --follow
```

### Important Metrics

- **Glue**: Job duration, DPU hours, Records processed
- **Lambda**: Invocations, Errors, Duration, Throttles
- **RDS**: CPU, Connections, Database connections

### Common Troubleshooting

1. **RDS Connection Error**:

   - Check Security Groups
   - Verify Lambda/Glue is in the same VPC
   - Check credentials

2. **Lambda Timeout**:

   - Increase timeout (max 15 minutes)
   - Process in smaller batches
   - Use Glue for batch processing

3. **Memory Error**:

   - Increase memory allocation
   - Optimize code to use less memory

4. **ViaCEP Rate Limiting**:
   - Adjust `RATE_LIMIT_PER_SECOND`
   - Implement retry with backoff (already implemented)

### Local Testing

```bash
# Test Lambda locally
python -c "
import json
from aws.lambda_function import lambda_handler

# Test API Gateway
event = {
    'body': json.dumps({'cep': '01310100'}),
    'requestContext': {}
}
result = lambda_handler(event, None)
print(result)
"
```

## Estimated Costs

- **Glue**: ~$0.44 per DPU-hour (batch processing)
- **Lambda**: $0.20 per 1M requests + $0.0000166667 per GB-second
- **RDS**: Depends on instance (db.t3.micro ~$15/month)
- **S3**: $0.023 per GB stored + transfers

## Next Steps

1. Implement CloudWatch alerts
2. Configure automatic RDS backups
3. Implement CI/CD with GitHub Actions or AWS CodePipeline
4. Add monitoring with CloudWatch Dashboards
5. Implement autoscaling for Lambda (if needed)
