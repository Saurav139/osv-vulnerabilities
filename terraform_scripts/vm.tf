resource "azurerm_linux_virtual_machine" "osv_vm" {
  name                = "osv-airflow-vm"
  resource_group_name = var.resource_group_name
  location            = var.location
  size                = "Standard_B2s"
  admin_username      = "osvadmin"

  network_interface_ids = [azurerm_network_interface.osv_nic.id]

  admin_ssh_key {
    username   = "osvadmin"
    public_key = file("~/.ssh/id_rsa.pub")
  }

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "Standard_LRS"
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "UbuntuServer"
    sku       = "18.04-LTS"
    version   = "latest"
  }

  provisioner "remote-exec" {
    connection {
      type        = "ssh"
      user        = "osvadmin"
      private_key = file("~/.ssh/id_rsa")
      host        = azurerm_linux_virtual_machine.osv_vm.public_ip_address
    }

    inline = [
      "sudo apt update -y",
      "sudo apt install -y python3-pip virtualenv unzip",
      "virtualenv airflow_env",
      "source airflow_env/bin/activate",
      "pip install apache-airflow",
      "mkdir -p ~/airflow/dags",
      "mv /mnt/data/osv-data-ingestion-base.py ~/airflow/dags/",
      "mv /mnt/data/requirements.txt ~/airflow/",
      "pip install -r ~/airflow/requirements.txt",  
      "airflow db init",
      "airflow webserver -p 8080 &",
      "airflow scheduler &"
    ]
  }
}
