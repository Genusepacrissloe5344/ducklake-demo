# do this if you use SSO. duckdb cannot create a secret using AWS SSO.
set -a && source .env && set +a && eval $(aws configure export-credentials --profile $AWS_PROFILE --format env) && cd ducklake_dbt && uv run dbt debug
