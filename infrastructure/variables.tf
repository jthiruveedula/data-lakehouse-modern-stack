variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP region for all resources"
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Deployment environment: dev | staging | prod"
  type        = string
  default     = "dev"
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging, or prod."
  }
}

variable "subnetwork" {
  description = "VPC subnetwork for Dataproc and Composer"
  type        = string
  default     = "default"
}

variable "master_machine_type" {
  description = "Dataproc master node machine type"
  type        = string
  default     = "n2-highmem-4"
}

variable "worker_machine_type" {
  description = "Dataproc worker node machine type"
  type        = string
  default     = "n2-standard-8"
}

variable "worker_count" {
  description = "Number of primary Dataproc workers"
  type        = number
  default     = 3
}

variable "preemptible_count" {
  description = "Number of preemptible / spot Dataproc workers"
  type        = number
  default     = 2
}

variable "trino_node_count" {
  description = "Number of Trino worker nodes on GKE"
  type        = number
  default     = 3
}
