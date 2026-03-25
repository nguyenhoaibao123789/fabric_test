terraform {
  required_version = ">= 1.6"

  required_providers {
    fabric = {
      source  = "microsoft/fabric"
      version = "~> 1.0"
    }
  }
}

# Authenticate via Azure CLI: run `az login` once, then `terraform apply`
provider "fabric" {
  use_cli = true
  preview = true
}
