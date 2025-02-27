container_name = "osv-vulnerabilities-parquet"
storage_account_name = "osvdataingestion"
synapse_workspace_name = "osv-synapse"
vm_public_ip = "40.87.80.202"
saurav [ ~/osv-vulnerabilities/terraform_scripts ]$ ls
main.tf  outputs.tf  storage.tf  synapse.tf  terraform.tfstate  terraform.tfstate.backup  variables.tf  vm.tf
saurav [ ~/osv-vulnerabilities/terraform_scripts ]$ cat synapse.tf
resource "azurerm_synapse_workspace" "osv_synapse" {
  name                                 = var.synapse_workspace_name
  resource_group_name                  = var.resource_group_name
  location                             = var.location
  storage_data_lake_gen2_filesystem_id = azurerm_storage_data_lake_gen2_filesystem.osv_filesystem.id
  sql_administrator_login              = var.sql_admin_user
  sql_administrator_login_password     = var.sql_admin_password

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_synapse_spark_pool" "osvsparkpool" {
  name                 = "osvsparkpool"
  synapse_workspace_id = azurerm_synapse_workspace.osv_synapse.id
  node_size_family     = "MemoryOptimized"
  node_size            = "Small"
        spark_version = "3.3"
  auto_scale {
    min_node_count = 3
    max_node_count = 10
  }
}