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

module "storage" {
  source = "./storage.tf"
}

module "vm" {
  source = "./vm.tf"
}

module "synapse" {
  source = "./synapse.tf"
}
