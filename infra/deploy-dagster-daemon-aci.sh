#!/bin/bash
SUBSCRIPTION_ID="$(az account show --query id --output tsv)"
ACI_NAME=dagster-daemon
ACI_IMAGE=cfaprdbatchcr.azurecr.io/cfa-dagster-infra:latest
RESOURCE_GROUP=ext-edav-cfa-prd
ACI_ACR_IDENTITY="/subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.ManagedIdentity/userAssignedIdentities/dagster-daemon-mi"
ACI_SUBNET_ID="/subscriptions/$SUBSCRIPTION_ID/resourceGroups/EXT-EDAV-CFA-Network-PRD/providers/Microsoft.Network/virtualNetworks/EXT_EDAV_CFA_VNET_PRD/subnets/EXT_EDAV_CFA_CONTAINER_INSTANCE_PRD"
# this is the account name and the file share name
AZURE_FILE_SHARE=cfadagsterfs
AZURE_FILE_VOLUME_ACCOUNT_KEY=$(az storage account keys list --resource-group "$RESOURCE_GROUP" --account-name "$AZURE_FILE_SHARE" | jq -r '.[0].value')
az container create \
	--resource-group "$RESOURCE_GROUP" \
	--subscription "$SUBSCRIPTION_ID" \
	--name "$ACI_NAME" \
	--image "$ACI_IMAGE" \
	--acr-identity "$ACI_ACR_IDENTITY" \
	--assign-identity "$ACI_ACR_IDENTITY" \
	--environment-variables DAGSTER_HOME=/opt/dagster/dagster_home DAGSTER_USER=prod \
	--log-analytics-workspace "DefaultWorkspace-$SUBSCRIPTION_ID-EUS" \
	--azure-file-volume-share-name "$AZURE_FILE_SHARE" \
	--azure-file-volume-account-name "$AZURE_FILE_SHARE" \
	--azure-file-volume-account-key "$AZURE_FILE_VOLUME_ACCOUNT_KEY" \
	--azure-file-volume-mount-path /opt/dagster \
	--command-line "dagster-daemon run" \
	--subnet "$ACI_SUBNET_ID" \
	--ports 80 \
	--restart-policy Always \
	--os-type Linux \
	--cpu 2 \
	--memory 4
