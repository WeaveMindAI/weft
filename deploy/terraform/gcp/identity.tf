# Who may do what. Nothing here holds a key file: the machine and the
# serverless roles run as the core account, project workers and infra
# machines as an account per project (created by the core when a project
# first runs), and GitHub Actions authenticate through Workload Identity
# Federation.

# weft's own account: the machine, and any role placed serverless.
resource "google_service_account" "core" {
  account_id   = "${var.name}-core"
  display_name = "weft runtime"
}

locals {
  core = "serviceAccount:${google_service_account.core.email}"
  # What the core does across the project:
  core_project_roles = [
    # deploy each project's workers and the serverless roles, and let
    # only the core call them
    "roles/run.admin",
    # start builds
    "roles/cloudbuild.builds.editor",
    # set wakes, as itself
    "roles/cloudtasks.enqueuer",
    # run each infra node's machine and disks
    "roles/compute.instanceAdmin.v1",
    # run builds as the builder, and workers and infra machines as each
    # project's account (see below for why this is project-wide)
    "roles/iam.serviceAccountUser",
    # write its logs
    "roles/logging.logWriter",
  ]
}

resource "google_project_iam_member" "core" {
  for_each = toset(local.core_project_roles)
  project  = var.project_id
  role     = each.value
  member   = local.core
}

# Make and remove each project's account, and nothing else about accounts:
# no key, no change to any account's own IAM policy (the Service Account
# Admin role would let the core grant itself any account's powers).
# IAM Conditions cannot narrow this or the actAs grant above to the
# `wp-` accounts: a service account is not a resource `resource.name`
# conditions can match.
resource "google_project_iam_custom_role" "project_accounts" {
  role_id = "${replace(var.name, "-", "_")}_project_accounts"
  title   = "weft project accounts"
  permissions = [
    "iam.serviceAccounts.create",
    "iam.serviceAccounts.delete",
    "iam.serviceAccounts.get",
  ]
}

resource "google_project_iam_member" "core_project_accounts" {
  project = var.project_id
  role    = google_project_iam_custom_role.project_accounts.id
  member  = local.core
}

# Reads project images, deletes the ones nothing uses any more (`weft
# clean`), and lets each project's account pull (a change to the
# repository's own IAM, which only the admin role carries).
resource "google_artifact_registry_repository_iam_member" "core_registry" {
  location   = google_artifact_registry_repository.images.location
  repository = google_artifact_registry_repository.images.name
  role       = "roles/artifactregistry.admin"
  member     = local.core
}

resource "google_storage_bucket_iam_member" "core_stages_builds" {
  bucket = google_storage_bucket.builds.name
  role   = "roles/storage.objectAdmin"
  member = local.core
}

# Cloud Build runs a project's build as an account of its own, which can
# read the staged context, push the image, and write the build's log.
resource "google_service_account" "builder" {
  account_id   = "${var.name}-builder"
  display_name = "weft image builder"
}

resource "google_storage_bucket_iam_member" "builder_reads_contexts" {
  bucket = google_storage_bucket.builds.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.builder.email}"
}

resource "google_artifact_registry_repository_iam_member" "builder_push" {
  location   = google_artifact_registry_repository.images.location
  repository = google_artifact_registry_repository.images.name
  role       = "roles/artifactregistry.writer"
  member     = "serviceAccount:${google_service_account.builder.email}"
}

resource "google_project_iam_member" "builder_logs" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.builder.email}"
}

# GitHub Actions in every repository of `frontend_repos`, to deploy a
# frontend. (The install workflow itself authenticates with the identity
# you made once by hand before the first apply; see the cloud chapter.)
resource "google_iam_workload_identity_pool" "github" {
  workload_identity_pool_id = "${var.name}-github"
  display_name              = "GitHub Actions"
  depends_on                = [google_project_service.apis]
}

resource "google_iam_workload_identity_pool_provider" "github" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.github.workload_identity_pool_id
  workload_identity_pool_provider_id = "github"
  display_name                       = "GitHub Actions"

  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.repository" = "assertion.repository"
  }
  attribute_condition = "assertion.repository in ${jsonencode(var.frontend_repos)}"

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

# A frontend's CI pushes its image and deploys it to its own Cloud Run
# service, and nothing else.
resource "google_service_account" "deployer" {
  account_id   = "${var.name}-deployer"
  display_name = "weft frontend deployer"
}

resource "google_artifact_registry_repository_iam_member" "deployer_push" {
  location   = google_artifact_registry_repository.frontends.location
  repository = google_artifact_registry_repository.frontends.name
  role       = "roles/artifactregistry.writer"
  member     = "serviceAccount:${google_service_account.deployer.email}"
}

# Each frontend repository deploys to one Cloud Run service of its own,
# made here, and the deployer may change that service and no other: no
# role on the project's Cloud Run at all. A project-wide grant would let a
# frontend repository replace a project's worker service (workers trust
# every call that reaches them) or open one to the internet. The service
# is public without any IAM change (`invoker_iam_disabled`), so no deploy
# ever needs `setIamPolicy`. Terraform makes the service with a stand-in
# image and then leaves what runs on it to the repository's workflow.
locals {
  # SYNC: the frontend service name <-> crates/weft-cli/templates/ci/gcp.yml (FRONT_SERVICE)
  frontend_services = { for repo in var.frontend_repos : repo => "${var.name}-front-${substr(sha256(repo), 0, 12)}" }
}

resource "google_cloud_run_v2_service" "frontend" {
  for_each             = local.frontend_services
  name                 = each.value
  location             = var.region
  ingress              = "INGRESS_TRAFFIC_ALL"
  invoker_iam_disabled = true
  deletion_protection  = false
  labels               = { "weft-frontend-repo" = substr(replace(lower(each.key), "/[^a-z0-9_-]/", "_"), 0, 63) }

  template {
    service_account = google_service_account.frontend.email
    containers {
      image = "us-docker.pkg.dev/cloudrun/container/hello"
    }
  }

  lifecycle {
    ignore_changes = [template, client, client_version]
  }
  depends_on = [google_project_service.apis]
}

resource "google_cloud_run_v2_service_iam_member" "deployer_runs_frontend" {
  for_each = google_cloud_run_v2_service.frontend
  project  = var.project_id
  location = each.value.location
  name     = each.value.name
  role     = "roles/run.developer"
  member   = "serviceAccount:${google_service_account.deployer.email}"
}

# Google lists `run.operations.get` among what a deploy needs "to read the
# status of the service", and an operation is not a resource a grant on
# one service reaches: it is only granted on the project. This role holds
# that one read, so the deployer still cannot change any other service.
resource "google_project_iam_custom_role" "deploy_status" {
  role_id     = "${replace(var.name, "-", "_")}_deploy_status"
  title       = "weft frontend deploy status"
  permissions = ["run.operations.get"]
}

resource "google_project_iam_member" "deployer_reads_deploy_status" {
  project = var.project_id
  role    = google_project_iam_custom_role.deploy_status.id
  member  = "serviceAccount:${google_service_account.deployer.email}"
}

# A frontend reaches the install's private address through Direct VPC
# egress on the install's subnet.
resource "google_compute_subnetwork_iam_member" "deployer_subnet" {
  subnetwork = google_compute_subnetwork.main.name
  region     = var.region
  role       = "roles/compute.networkUser"
  member     = "serviceAccount:${google_service_account.deployer.email}"
}

# Cloud Run runs a frontend as this account, which may do nothing.
resource "google_service_account" "frontend" {
  account_id   = "${var.name}-frontend"
  display_name = "weft project frontends"
}

resource "google_service_account_iam_member" "deployer_acts_as_frontend" {
  service_account_id = google_service_account.frontend.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.deployer.email}"
}

resource "google_service_account_iam_member" "github_deploys" {
  for_each           = toset(var.frontend_repos)
  service_account_id = google_service_account.deployer.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github.name}/attribute.repository/${each.value}"
}
