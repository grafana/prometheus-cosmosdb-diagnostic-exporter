package main

import (
	"errors"
)

type authMode string

const (
	authModeSharedKey authMode = "shared-key"
	authModeARMKey    authMode = "arm-key-lookup"
	authModeEntraID   authMode = "entra-id"
)

// selectAuthMode uses the storage key if set, otherwise an ARM key lookup if both
// subscription ID and resource group are set, otherwise Microsoft Entra ID.
func selectAuthMode(cfg *Config) (authMode, error) {
	if cfg.StorageAccountKey != "" {
		return authModeSharedKey, nil
	}
	switch {
	case cfg.SubscriptionID != "" && cfg.StorageResourceGroup != "":
		return authModeARMKey, nil
	case cfg.SubscriptionID == "" && cfg.StorageResourceGroup == "":
		return authModeEntraID, nil
	case cfg.SubscriptionID == "":
		return "", errors.New("-subscription-id is required when -storage-resource-group is set (or unset both to use Microsoft Entra ID authentication)")
	default:
		return "", errors.New("-storage-resource-group is required when -subscription-id is set (or unset both to use Microsoft Entra ID authentication)")
	}
}
