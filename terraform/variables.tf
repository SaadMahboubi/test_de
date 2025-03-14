variable "project_id" {
  description = "ID du projet GCP"
  type        = string
}

variable "region" {
  description = "Région de déploiement"
  type        = string
  default     = "europe-west1"
}

variable "zone" {
  description = "Zone de déploiement"
  type        = string
  default     = "europe-west1-b"
}

variable "composer_name" {
  description = "Nom de l'environnement Cloud Composer"
  type        = string
  default     = "my-composer-env"
}

variable "service_account" {
  description = "Service account avec permissions Composer"
  type        = string
}

