terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~>3.0"
    }
  }
}

provider "azurerm" {
  features {}
}
saurav [ ~/osv-vulnerabilities/terraform_scripts ]$ ls
main.tf  outputs.tf  storage.tf  synapse.tf  terraform.tfstate  variables.tf  vm.tf
saurav [ ~/osv-vulnerabilities/terraform_scripts ]$ cat vm.tf
# Define Virtual Network (VNet)
resource "azurerm_virtual_network" "osv_vnet" {
  name                = "osv-vnet"
  resource_group_name = var.resource_group_name
  location            = var.location
  address_space       = ["10.0.0.0/16"]
}

# Define Subnet
resource "azurerm_subnet" "osv_subnet" {
  name                 = "osv-subnet"
  resource_group_name  = var.resource_group_name
  virtual_network_name = azurerm_virtual_network.osv_vnet.name
  address_prefixes     = ["10.0.1.0/24"]
}

# Define Public IP for the VM
resource "azurerm_public_ip" "osv_vm_ip" {
  name                = "osv-vm-ip"
  resource_group_name = var.resource_group_name
  location            = var.location
  allocation_method   = "Dynamic"
}

# Define Network Interface (NIC)
resource "azurerm_network_interface" "osv_nic" {
  name                = "osv-vm-nic"
  resource_group_name = var.resource_group_name
  location            = var.location

  ip_configuration {
    name                          = "osv-ip-config"
    subnet_id                     = azurerm_subnet.osv_subnet.id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.osv_vm_ip.id
  }
}

# Define Azure Linux Virtual Machine
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

  # Ensure Airflow setup happens after network interface is available
  depends_on = [azurerm_network_interface.osv_nic]

  # Remote provisioning - Installs Airflow and dependencies
  provisioner "remote-exec" {
    connection {
      type        = "ssh"
      user        = "osvadmin"
      private_key = file("~/.ssh/id_rsa")
      host        = self.public_ip_address
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