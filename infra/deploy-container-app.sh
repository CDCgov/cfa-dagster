#!/bin/bash
set -e

# Variables
RESOURCE_GROUP="ext-edav-cfa-prd"
CONTAINER_APP_NAME="dagster"

# Create a temporary YAML file for deployment
TEMP_YAML="infra/temp-dagster-container-app.yaml"
cp "infra/dagster-container-app.yaml" "$TEMP_YAML"

# Replace the placeholder with the current date
sed -i "s|__DEPLOY_DATE__|$(date)|g" "$TEMP_YAML"

# Deploy the container app
az containerapp update \
  --name "$CONTAINER_APP_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --yaml "$TEMP_YAML"

# Remove the temporary YAML file
rm "$TEMP_YAML"

echo "Container app '$CONTAINER_APP_NAME' is being updated."
