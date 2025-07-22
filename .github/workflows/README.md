# Welcome Home Pipeline - GitHub Actions Workflow

## Overview

The GitHub Actions workflow is designed to run the Welcome Home data export pipeline with the following features:

- **Daily Schedule**: Runs automatically at 00:15 EST (05:15 UTC) every day
- **Step Isolation**: Individual table processing steps can fail without affecting other tables
- **Selective Execution**: Ability to run specific steps or tables independently
- **Easy Restart**: Failed steps can be restarted without re-running successful ones

## Workflow Structure

The workflow consists of 4 main jobs:

1. **Setup**: Determines which tables to process
2. **API/Blob Upload**: Downloads data from API and uploads to Azure Blob Storage (parallel for each table)
3. **Snowflake Load**: Loads data from Azure Blob to Snowflake (parallel for each table)
4. **Summary**: Reports overall pipeline status

## Current Tables Processed

- Prospects
- Residents  
- Activities
- DepositTransactions

## Manual Triggering & Restart Options

### 1. Run All Tables (Both Steps)
```
Go to Actions → Welcome Home Pipeline → Run workflow
Leave all fields empty and click "Run workflow"
```

### 2. Run Specific Tables Only
```
Go to Actions → Welcome Home Pipeline → Run workflow
Tables: "Prospects,Activities"
Step type: "both"
```

### 3. Restart Failed API/Blob Steps Only
```
Go to Actions → Welcome Home Pipeline → Run workflow  
Tables: "Prospects,Residents" (failed tables)
Step type: "api_blob_only"
```

### 4. Restart Failed Snowflake Steps Only
```
Go to Actions → Welcome Home Pipeline → Run workflow
Tables: "Activities,DepositTransactions" (failed tables)  
Step type: "snowflake_only"
```

## How to Identify Failed Steps

1. Go to the failed workflow run
2. Check the job status:
   - ✅ Green = Success
   - ❌ Red = Failed
   - ⚠️ Yellow = Some steps failed (due to `continue-on-error: true`)
3. Click on failed jobs to see which specific tables failed
4. Use the table names in manual restart

## Step Isolation Benefits

- **No Cascade Failures**: If `Prospects` API download fails, `Residents` processing continues
- **Granular Restart**: Restart only the specific step that failed for specific tables
- **Resource Efficiency**: Don't re-process successful tables
- **Parallel Processing**: All tables process simultaneously for faster execution

## Logs and Artifacts

Each step uploads logs as artifacts:
- `api-blob-logs-{TableName}`: Logs from API download and blob upload
- `snowflake-logs-{TableName}`: Logs from Snowflake loading
- Retention: 7 days

## Example Restart Scenarios

### Scenario 1: API Rate Limit Hit
**Problem**: Prospects and Residents API calls failed due to rate limiting
**Solution**: 
```
Tables: "Prospects,Residents"
Step type: "api_blob_only"
```

### Scenario 2: Snowflake Connection Issue  
**Problem**: All Snowflake loads failed due to network issue
**Solution**:
```
Tables: "" (empty = all tables)
Step type: "snowflake_only"  
```

### Scenario 3: Single Table SQL Error
**Problem**: Activities table has SQL syntax error
**Solution**:
1. Fix the SQL file: `sql/load_activities.sql`
2. Restart:
```
Tables: "Activities"
Step type: "snowflake_only"
```

## Monitoring

The workflow provides clear status reporting:
- Individual job status for each table/step combination
- Summary job with overall pipeline health
- Detailed logs for troubleshooting
- Emoji indicators for quick status assessment (✅❌⚠️💡)

## Configuration

The workflow uses the same configuration and secrets as the original pipeline:
- `WELCOME_HOME_API`: API key for Welcome Home
- `AZURE_CONNECTION_STRING`: Azure storage connection
- `SNOWFLAKE_PASSWORD`: Snowflake database password

All other configuration comes from `config.ini` in the repository.
