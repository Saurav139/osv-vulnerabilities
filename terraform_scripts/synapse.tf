resource "azurerm_synapse_workspace" "osv_synapse" {
  name                = var.synapse_workspace_name
  resource_group_name = var.resource_group_name
  location            = var.location
  storage_data_lake_gen2_filesystem_id = azurerm_storage_account.osv_storage.id
  sql_administrator_login              = var.sql_admin_user
  sql_administrator_login_password      = var.sql_admin_password
}

# Add the Synapse Spark Pool with auto-scaling
resource "azurerm_synapse_spark_pool" "osvsparkpool" {
  name                 = "osvsparkpool"
  synapse_workspace_id = azurerm_synapse_workspace.osv_synapse.id
  node_size_family     = "MemoryOptimized"
  node_size            = "Small"

  # Choose either auto-scale or fixed node count (DO NOT use both)
  auto_scale {
    min_node_count = 3
    max_node_count = 10
  }

  
}