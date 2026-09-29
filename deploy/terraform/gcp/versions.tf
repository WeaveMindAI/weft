# The cloud a weft install runs on, on GCP: one small machine running
# weft and its Postgres, and Google's serverless pieces around it (Cloud
# Run for the serverless roles and project workers, Cloud Build for
# builds, Cloud Tasks for wakes). Applied by .github/workflows/install-gcp.yml.
terraform {
  required_version = ">= 1.6"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  # The state lives in a bucket you create once (see the cloud chapter of
  # the docs); the workflow passes its name with -backend-config.
  backend "gcs" {
    prefix = "weft"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

data "google_project" "this" {}

resource "google_project_service" "apis" {
  for_each = toset([
    "artifactregistry.googleapis.com",
    "cloudbuild.googleapis.com",
    "cloudtasks.googleapis.com",
    "compute.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "run.googleapis.com",
    "secretmanager.googleapis.com",
    "storage.googleapis.com",
  ])
  service            = each.value
  disable_on_destroy = false
}
