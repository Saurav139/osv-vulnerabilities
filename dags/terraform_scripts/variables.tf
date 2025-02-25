variable "resource_group_name" {
  default = "osv-resource-group"
}

variable "location" {
  default = "East US"
}

variable "storage_account_name" {
  default = "osvdataingestion"
}

variable "container_name" {
  default = "osv-vulnerabilities-parquet"
}

variable "synapse_workspace_name" {
  default = "osv-synapse"
}

variable "synapse_spark_pool_name" {
  default = "osv-spark-pool"
}

variable "sql_admin_user" {
  default = "adminuser"
}

variable "sql_admin_password" {
  default = "SecurePass123!"
}
