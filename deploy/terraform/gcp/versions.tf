# The cloud a weft install runs on, on GCP, with no machine of its own:
# Google's serverless pieces (Cloud Run for weft's roles, the holders and
# project workers, Cloud Build for builds, Cloud Tasks for wakes), infra
# machines on Compute Engine as projects ask for them, and the database
# wherever its URL points. Applied by .github/workflows/install-gcp.yml.
terraform {
  required_version = ">= 1.6"

  required_providers {
    google = {
      source = "hashicorp/google"
      # Cloud Run worker pools (the holders) arrived in 7, and
      # `google_workload_identity_service_agent` (machines.tf) in 7.28.
      version = "~> 7.28"
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
    # the certificates of the door in front of the install's domains
    "certificatemanager.googleapis.com",
    "cloudbuild.googleapis.com",
    "cloudtasks.googleapis.com",
    "compute.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    # carrying infra machine events to the supervisor (machines.tf)
    "logging.googleapis.com",
    "pubsub.googleapis.com",
    "run.googleapis.com",
    "secretmanager.googleapis.com",
    "storage.googleapis.com",
    # making Cloud Logging's service agent up front (machines.tf)
    "workloadidentity.googleapis.com",
  ])
  service            = each.value
  disable_on_destroy = false
}
