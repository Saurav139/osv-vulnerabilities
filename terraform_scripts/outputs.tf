output "storage_account_name" {
  value = azurerm_storage_account.osv_storage.name
}

output "container_name" {
  value = azurerm_storage_container.osv_container.name
}

output "vm_public_ip" {
  value = azurerm_linux_virtual_machine.osv_vm.public_ip_address
}

output "synapse_workspace_name" {
  value = azurerm_synapse_workspace.osv_synapse.name
}
