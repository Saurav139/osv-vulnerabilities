resource "azurerm_storage_account" "osv_storage" {
  name                     = var.storage_account_name
  resource_group_name      = var.resource_group_name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true  # Enables hierarchical namespace for Data Lake Gen2
}

resource "azurerm_storage_container" "osv_container" {
  name                  = var.container_name
  storage_account_name  = azurerm_storage_account.osv_storage.name
  container_access_type = "private"
}

resource "azurerm_storage_data_lake_gen2_filesystem" "osv_filesystem" {
  name               = "osvfilesystem"
  storage_account_id = azurerm_storage_account.osv_storage.id
}