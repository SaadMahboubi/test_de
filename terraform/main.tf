provider "google" {
  credentials = file("../service-account-file.json")
  project     = var.project_id
  region      = var.region
}

resource "google_composer_environment" "composer_env" {
  name    = var.composer_name
  region  = var.region
  project = var.project_id

  config {
    software_config {
      image_version = "composer-2-airflow-2"
    }

    workloads_config {
      scheduler {
        cpu        = 0.5
        memory_gb  = 1.5
        storage_gb = 1
      }
      web_server {
        cpu        = 0.5
        memory_gb  = 1.5
        storage_gb = 1
      }
      worker {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 10
      }
    }
  }
}

# Récupération du nom du bucket généré par Cloud Composer
data "google_composer_environment" "composer_env_data" {
  name    = google_composer_environment.composer_env.name
  region  = google_composer_environment.composer_env.region
  project = google_composer_environment.composer_env.project
}