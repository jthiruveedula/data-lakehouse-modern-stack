terraform {
  required_version = ">= 1.7"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 5.0"
    }
  }
  backend "gcs" {
    bucket = "tf-state-data-lakehouse"
    prefix = "terraform/state"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ── GCS Buckets ─────────────────────────────────────────────────────────────

resource "google_storage_bucket" "lakehouse" {
  name                        = "${var.project_id}-lakehouse"
  location                    = var.region
  uniform_bucket_level_access = true
  versioning { enabled = true }

  lifecycle_rule {
    condition {
      age                   = 30
      matches_storage_class = ["STANDARD"]
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  lifecycle_rule {
    condition {
      age = 365
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }

  labels = local.common_labels
}

resource "google_storage_bucket_iam_binding" "lakehouse_spark" {
  bucket = google_storage_bucket.lakehouse.name
  role   = "roles/storage.objectAdmin"
  members = [
    "serviceAccount:${google_service_account.dataproc.email}",
  ]
}

# ── Service Accounts ─────────────────────────────────────────────────────────

resource "google_service_account" "dataproc" {
  account_id   = "dataproc-lakehouse"
  display_name = "Dataproc Lakehouse Service Account"
}

resource "google_project_iam_member" "dataproc_roles" {
  for_each = toset([
    "roles/dataproc.worker",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/storage.objectAdmin",
    "roles/dataplex.developer",
  ])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.dataproc.email}"
}

# ── Dataproc Cluster ─────────────────────────────────────────────────────────

resource "google_dataproc_cluster" "spark" {
  name    = "lakehouse-spark-${var.environment}"
  region  = var.region
  project = var.project_id

  cluster_config {
    staging_bucket = google_storage_bucket.lakehouse.name

    master_config {
      num_instances = 1
      machine_type  = var.master_machine_type
      disk_config {
        boot_disk_type    = "pd-ssd"
        boot_disk_size_gb = 100
      }
    }

    worker_config {
      num_instances = var.worker_count
      machine_type  = var.worker_machine_type
      disk_config {
        boot_disk_type    = "pd-standard"
        boot_disk_size_gb = 500
        num_local_ssds    = 1
      }
    }

    preemptible_worker_config {
      num_instances = var.preemptible_count
    }

    software_config {
      image_version = "2.1-debian11"
      properties = {
        "spark:spark.sql.extensions"               = "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        "spark:spark.sql.catalog.spark_catalog"    = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        "spark:spark.sql.catalog.iceberg"          = "org.apache.iceberg.spark.SparkCatalog"
        "spark:spark.sql.catalog.iceberg.type"     = "hadoop"
        "spark:spark.sql.catalog.iceberg.warehouse" = "gs://${var.project_id}-lakehouse/iceberg"
        "spark:spark.jars.packages"                = "io.delta:delta-spark_2.12:3.1.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0"
      }
    }

    gce_cluster_config {
      service_account        = google_service_account.dataproc.email
      service_account_scopes = ["cloud-platform"]
      subnetwork             = var.subnetwork
      tags                   = ["dataproc-lakehouse"]
    }

    autoscaling_config {
      policy_uri = google_dataproc_autoscaling_policy.policy.name
    }
  }

  labels = local.common_labels
}

resource "google_dataproc_autoscaling_policy" "policy" {
  policy_id = "lakehouse-autoscale"
  location  = var.region

  worker_config {
    min_instances = var.worker_count
    max_instances = var.worker_count * 4
  }

  basic_algorithm {
    yarn_config {
      scale_up_factor                = 1.0
      scale_down_factor              = 1.0
      scale_up_min_worker_fraction   = 0.0
      scale_down_min_worker_fraction = 0.0
      graceful_decommission_timeout  = "3600s"
    }
    cooldown_period = "600s"
  }
}

# ── BigQuery Dataset ─────────────────────────────────────────────────────────

resource "google_bigquery_dataset" "gold" {
  dataset_id  = "lakehouse_gold"
  location    = var.region
  description = "Gold layer — analytics-ready star schema tables"
  labels      = local.common_labels

  access {
    role          = "OWNER"
    user_by_email = google_service_account.dataproc.email
  }
}

# ── Cloud Composer (Airflow) ─────────────────────────────────────────────────

resource "google_composer_environment" "orchestrator" {
  name    = "lakehouse-composer-${var.environment}"
  region  = var.region
  project = var.project_id

  config {
    software_config {
      image_version  = "composer-2.6.6-airflow-2.7.3"
      python_version = "3"
      env_variables = {
        GCS_BUCKET  = google_storage_bucket.lakehouse.name
        GCP_PROJECT = var.project_id
      }
      pypi_packages = {
        "delta-spark"          = "==3.1.0"
        "dbt-spark"            = "==1.8.0"
        "great-expectations"   = "==0.18.15"
      }
    }

    workloads_config {
      scheduler {
        cpu        = 1
        memory_gb  = 2
        storage_gb = 5
        count      = 1
      }
      web_server {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 5
      }
      worker {
        cpu        = 2
        memory_gb  = 8
        storage_gb = 50
        min_count  = 2
        max_count  = 8
      }
    }

    environment_size = "ENVIRONMENT_SIZE_SMALL"

    node_config {
      service_account = google_service_account.dataproc.email
      subnetwork      = var.subnetwork
    }
  }

  labels = local.common_labels
}

# ── DataPlex ─────────────────────────────────────────────────────────────────

resource "google_dataplex_lake" "lakehouse" {
  name     = "lakehouse"
  location = var.region
  project  = var.project_id
  labels   = local.common_labels
}

resource "google_dataplex_zone" "bronze" {
  name     = "bronze"
  lake     = google_dataplex_lake.lakehouse.name
  location = var.region
  project  = var.project_id
  type     = "RAW"
  resource_spec { location_type = "MULTI_REGION" }
  discovery_spec { enabled = false }
}

resource "google_dataplex_zone" "silver" {
  name     = "silver"
  lake     = google_dataplex_lake.lakehouse.name
  location = var.region
  project  = var.project_id
  type     = "CURATED"
  resource_spec { location_type = "MULTI_REGION" }
  discovery_spec { enabled = false }
}

resource "google_dataplex_zone" "gold" {
  name     = "gold"
  lake     = google_dataplex_lake.lakehouse.name
  location = var.region
  project  = var.project_id
  type     = "CURATED"
  resource_spec { location_type = "MULTI_REGION" }
  discovery_spec { enabled = false }
}

locals {
  common_labels = {
    project     = var.project_id
    environment = var.environment
    team        = "data-platform"
    managed_by  = "terraform"
  }
}
