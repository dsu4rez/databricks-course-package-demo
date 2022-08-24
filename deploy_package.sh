# Configure Databricks CLI
echo "Configuring Databricks CLI..."
export DATABRICKS_AAD_TOKEN=$(az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d | jq -r .accessToken)
databricks_workspace_url=$(az databricks workspace show --resource-group dbxlab-rg --name dbxlab-databricks | jq -r .workspaceUrl)
databricks_host="https://$databricks_workspace_url"
databricks configure --host $databricks_host --aad-token

# Build wheel
echo "Building package..."
python3 setup.py bdist_wheel --universal

# Upload wheel package
echo "Deploying package..."
databricks fs rm -r dbfs:/jobs/logs_jobs/
databricks fs cp ./dist/logs_jobs-0.1-py2.py3-none-any.whl dbfs:/jobs/logs_jobs/logs_jobs-0.1-py2.py3-none-any.whl