terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0.2"
    }
  }

  required_version = ">= 1.1.0"
}

provider "azurerm" {
  features {
    resource_group {
      prevent_deletion_if_contains_resources = false
    }
  }
}

resource "azurerm_resource_group" "meltano_state_test" {
  name     = "meltano_state_test"
  location = "brazilsouth"
}

resource "azurerm_storage_account" "meltanostatestorage" {
  name                     = "meltanostatestorage"
  resource_group_name      = azurerm_resource_group.meltano_state_test.name
  location                 = azurerm_resource_group.meltano_state_test.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

resource "azurerm_storage_container" "meltanostatecontainer" {
  name                  = "meltanostatecontainer"
  storage_account_name  = azurerm_storage_account.meltanostatestorage.name
  container_access_type = "private"
}
