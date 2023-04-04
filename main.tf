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

resource "azurerm_resource_group" "airflow_training" {
  name     = "airflow_training"
  location = "brazilsouth"
}

resource "azurerm_storage_account" "dockeracivolumes" {
  name                     = "dockeracivolumes"
  resource_group_name      = azurerm_resource_group.airflow_training.name
  location                 = azurerm_resource_group.airflow_training.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

variable "volumes" {
  description = "Required volumes"
  type        = list(string)
  default = [
    "postgres-db-volume",
    "indicium-lfs",
    "analytics-db-volume",
    "dags",
    "logs",
    "plugins",
    "sources",
    "dbdata",
    "init-db-data",
    "analytics-config"
  ]

}

resource "azurerm_storage_share" "fileshare" {
  for_each             = toset(var.volumes)
  name                 = each.value
  storage_account_name = azurerm_storage_account.dockeracivolumes.name
  quota                = 10
}

resource "null_resource" "sharemount" {
  for_each = toset(var.volumes)
  provisioner "local-exec" {
    command = "sudo mkdir -p /mnt/${azurerm_storage_account.dockeracivolumes.name}/${each.value} && sudo mount -t cifs //${azurerm_storage_account.dockeracivolumes.name}.file.core.windows.net/${each.value} /mnt/${azurerm_storage_account.dockeracivolumes.name}/${each.value} -o vers=3.0,username=${azurerm_storage_account.dockeracivolumes.name},password=${azurerm_storage_account.dockeracivolumes.primary_access_key},dir_mode=0777,file_mode=0777"
  }
  depends_on = [azurerm_storage_share.fileshare]
}
