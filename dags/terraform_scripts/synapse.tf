resource "azurerm_synapse_workspace" "osv_synapse" {
  name                = var.synapse_workspace_name
  resource_group_name = var.resource_group_name
  location            = var.location
  storage_data_lake_gen2_filesystem_id = azurerm_storage_account.osv_storage.id
  sql_administrator_login              = var.sql_admin_user
  sql_administrator_login_password      = var.sql_admin_password
}

resource "azurerm_synapse_spark_pool" "osv_spark_pool" {
  name                 = var.synapse_spark_pool_name
  synapse_workspace_id = azurerm_synapse_workspace.osv_synapse.id
  node_size_family     = "MemoryOptimized"
  node_size            = "Small"
}
