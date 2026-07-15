variable "environment" {
  description = "Deployment environment used in names and cost allocation tags."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging or prod."
  }
}

variable "project" {
  description = "Project identifier used in resource names and tags."
  type        = string
  default     = "rentcars-case"
}

variable "kms_key_arn" {
  description = "Customer-managed KMS key ARN used to encrypt the data lake."
  type        = string
  sensitive   = true
}
