variable "workspace_id" {
  type        = string
  description = "Fabric workspace ID — copy from the workspace URL in the Fabric portal"
}

variable "env" {
  type        = string
  description = "Environment name: dev or prod"
}

variable "spark_env_name" {
  type        = string
  description = "Display name for the Fabric Spark Environment item"
}
