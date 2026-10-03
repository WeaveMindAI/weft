# Who may do what. Nothing here holds a key file: weft's roles and the
# holders run as the core account, project workers and infra machines as
# an account per project (created by the core when a project first runs),
# and GitHub Actions authenticate through Workload Identity Federation.

# weft's own account: every role and the holders.
resource "google_service_account" "core" {
  account_id   = "${var.name}-core"
  display_name = "weft runtime"
}

locals {
  core = "serviceAccount:${google_service_account.core.email}"
  # What the core does across the project:
  core_project_roles = [
    # deploy each project's workers, set how many holders run, and let
    # only the core call what it should
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
    # make and take down the load balancer in front of the install's
    # domains (`weft domain add`), only while it has any
    "roles/compute.loadBalancerAdmin",
    # and the certificates it holds for them
    "roles/certificatemanager.editor",
  ]
}

resource "google_project_iam_member" "core" {
  for_each = toset(local.core_project_roles)
  project  = var.project_id
  role     = each.value
  member   = local.core
}

# Let each project's account write logs (an infra machine's guest agent
# writes its boot output there), a grant only the project's own policy
# can carry. The condition lets the core add or remove that one role and
# no other, so this is not a way for it to grant itself anything.
resource "google_project_iam_member" "core_grants_log_writers" {
  project = var.project_id
  role    = "roles/resourcemanager.projectIamAdmin"
  member  = local.core
  condition {
    title      = "only log writers"
    expression = "api.getAttribute('iam.googleapis.com/modifiedGrantsByRole', []).hasOnly(['roles/logging.logWriter'])"
  }
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

# Sign the object store's links as itself (storage.tf): a link is signed
# by Google for the account, never with a key the install holds.
resource "google_service_account_iam_member" "core_signs_as_itself" {
  service_account_id = google_service_account.core.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = local.core
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

# GitHub Actions in the repositories of the frontends the install hosts,
# to deploy them (`weft frontend add <name> --repo <owner/name>`). (The
# install workflow itself authenticates with the identity you made once by
# hand before the first apply; see the cloud chapter.)
resource "google_iam_workload_identity_pool" "github" {
  workload_identity_pool_id = "${var.name}-github"
  display_name              = "GitHub Actions"
  depends_on                = [google_project_service.apis]
}

resource "google_iam_workload_identity_pool_provider" "github" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.github.workload_identity_pool_id
  workload_identity_pool_provider_id = "github"
  display_name                       = "GitHub Actions"

  # A frontend's repository is let in by its id (the name can be taken by
  # somebody else once the repository is deleted or renamed).
  # SYNC: attribute.repository_id <-> crates/weft-platform-gcp/src/frontends.rs (repo_principal)
  attribute_mapping = {
    "google.subject"          = "assertion.sub"
    "attribute.repository"    = "assertion.repository"
    "attribute.repository_id" = "assertion.repository_id"
  }
  # Any repository may exchange its GitHub token here, and gets nothing
  # by it: what a repository may do is only what a binding names it for,
  # and the install binds each frontend's repository as it adds the
  # frontend (crates/weft-platform-gcp/src/frontends.rs). Google asks
  # every GitHub provider for a condition; this one only says the token
  # names a repository.
  attribute_condition = "assertion.repository != ''"

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

# Each frontend deploys to one Cloud Run service of its own, which the
# install makes as the frontend is added and lets the deployer change
# (crates/weft-platform-gcp/src/frontends.rs): no role on the project's
# Cloud Run at all. A project-wide grant would let a frontend repository
# replace a project's worker service (workers trust every call that
# reaches them) or open one to the internet. The service is public
# without any IAM change (`invokerIamDisabled`), so no deploy ever needs
# `setIamPolicy`.

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

# A frontend reaches its project's infrastructure (a database) through
# Direct VPC egress on the install's subnet.
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

# The install lets a frontend's repository deploy as the deployer when it
# adds the frontend, and takes that back when it removes it: the core may
# change who acts as the deployer, and nothing about any other account.
resource "google_project_iam_custom_role" "deployer_access" {
  role_id     = "${replace(var.name, "-", "_")}_deployer_access"
  title       = "weft frontend repositories"
  permissions = ["iam.serviceAccounts.getIamPolicy", "iam.serviceAccounts.setIamPolicy"]
}

resource "google_service_account_iam_member" "core_lets_repositories_deploy" {
  service_account_id = google_service_account.deployer.name
  role               = google_project_iam_custom_role.deployer_access.id
  member             = local.core
}
