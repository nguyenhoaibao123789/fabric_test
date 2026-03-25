variable "workspace_id" {
  type        = string
  description = "Fabric workspace ID — copy from the workspace URL in the Fabric portal"
}

variable "spark_env_name" {
  type        = string
  description = "Display name for the Fabric Spark Environment item"
}

variable "git_repo_url" {
  type        = string
  description = "HTTPS URL of the Git repository containing DAGs (e.g. https://github.com/org/repo)"
}

variable "git_branch" {
  type        = string
  description = "Branch Airflow will sync DAGs from"
  default     = "main"
}

variable "git_dags_folder" {
  type        = string
  description = "Path inside the repo where DAG files live"
  default     = "fabric/dags"
}

variable "github_pat" {
  type        = string
  sensitive   = true
  description = "GitHub Personal Access Token with repo:read scope — stored write-only, never appears in state or logs"
}
