# The machine: weft's runtime (the front door, and every role placed on
# the machine: by default the listener) and its Postgres, on
# Container-Optimized OS. The database
# and the front door's certificates live on a disk of their own, so the
# machine can be replaced or resized without losing them.

locals {
  # SYNC: the install config's shape <-> crates/weft-platform-traits/src/config.rs (InstallConfig)
  public_url           = "https://${google_compute_address.public.address}"
  machine_internal_url = "http://${google_compute_address.machine_private.address}:${local.internal_port}"
  roles = { for r in ["dispatcher", "broker", "listener", "supervisor"] : r => (
    contains(var.serverless_roles, r) ? "serverless" : (r == "listener" && var.listener_machine ? "own_machine" : "machine")
  ) }
  # SYNC: the service name <-> serverless.tf (google_cloud_run_v2_service.role)
  role_urls = merge(
    { for r in var.serverless_roles : r => "https://${var.name}-role-${r}-${data.google_project.this.number}.${var.region}.run.app" },
    var.listener_machine ? { listener = "http://${google_compute_address.listener_private[0].address}:${local.internal_port}" } : {},
  )
  install_config = {
    platform = {
      kind                     = "gcp"
      project                  = var.project_id
      region                   = var.region
      zone                     = var.zone
      network                  = google_compute_network.vpc.id
      subnet                   = google_compute_subnetwork.main.id
      artifactRegistry         = local.images_registry
      buildBucket              = google_storage_bucket.builds.name
      builderServiceAccount    = google_service_account.builder.email
      tasksQueue               = google_cloud_tasks_queue.wakes.name
      coreServiceAccount       = google_service_account.core.email
      machineInternalUrl       = local.machine_internal_url
      runtimeImage             = var.runtime_image
      deployerServiceAccount   = google_service_account.deployer.email
      frontendServiceAccount   = google_service_account.frontend.email
      workloadIdentityProvider = google_iam_workload_identity_pool_provider.github.name
      callerTokenSecret        = google_secret_manager_secret.install["WEFT_CALLER_TOKEN_SECRET"].secret_id
      infraNetworkTag          = local.infra_tag
    }
    auth      = "operator_keys"
    publicUrl = local.public_url
    listen = {
      # The front door serves the public port's routes in process; the
      # port itself stays on the loopback.
      public   = "127.0.0.1:${local.public_port}"
      internal = "0.0.0.0:${local.internal_port}"
    }
    internalUrl = "http://127.0.0.1:${local.internal_port}"
    roles       = local.roles
    roleUrls    = local.role_urls
    build = {
      compileLanes     = var.compile_lanes
      builderBaseImage = var.builder_base_image
      runtimeBaseImage = "debian:bookworm-slim"
    }
    edge = {
      # A caller's address, counted from the right of X-Forwarded-For
      # plus the peer. On the machine the front door strips what the
      # caller sent, so the peer is the caller (0). A serverless
      # dispatcher sees `caller, machine` (the machine adds the caller,
      # Google's front end adds the machine) with Google as the peer (2).
      # Per listener; this install serves no outside port (listen.outside
      # is unset), so that entry is never read.
      trustedProxyHops = {
        public  = contains(var.serverless_roles, "dispatcher") ? 2 : 0
        outside = 0
      }
      invalidTokensPerMinute = var.invalid_tokens_per_minute > 0 ? var.invalid_tokens_per_minute : null
    }
    objectStore = {
      endpoint       = "https://storage.googleapis.com"
      bucket         = google_storage_bucket.files.name
      region         = "auto"
      forcePathStyle = true
      publicInternet = true
    }
    frontDoor = {
      address  = google_compute_address.public.address
      https    = "0.0.0.0:443"
      http     = "0.0.0.0:80"
      stateDir = "/mnt/disks/data/tls"
    }
    source = {
      repository = var.weft_repository
      commit     = var.weft_commit
    }
  }
  # Which secret each environment variable of the runtime is read from.
  runtime_secrets = { for k in nonsensitive(keys(local.secrets)) : k => google_secret_manager_secret.install[k].secret_id if k != "WEFT_POSTGRES_PASSWORD" }
}

resource "google_compute_disk" "data" {
  name = "${var.name}-data"
  zone = var.zone
  type = var.data_disk_type
  size = var.data_disk_gb

  lifecycle {
    # The database lives here.
    prevent_destroy = true
  }
}

resource "google_compute_instance" "machine" {
  name         = "${var.name}-machine"
  zone         = var.zone
  machine_type = var.machine_type
  tags         = [local.machine_tag]

  # A resize (the machine lever) stops and starts the machine.
  allow_stopping_for_update = true

  boot_disk {
    initialize_params {
      image = "cos-cloud/cos-stable"
      size  = 10
      type  = "pd-standard"
    }
  }

  attached_disk {
    source      = google_compute_disk.data.id
    device_name = "weft-data"
  }

  network_interface {
    subnetwork = google_compute_subnetwork.main.id
    network_ip = google_compute_address.machine_private.address
    access_config {
      nat_ip = google_compute_address.public.address
    }
  }

  service_account {
    email  = google_service_account.core.email
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  metadata = {
    user-data = templatefile("${path.module}/machine.yaml.tftpl", {
      config          = jsonencode(local.install_config)
      runtime_image   = var.runtime_image
      project         = var.project_id
      secrets         = join(" ", [for k, v in local.runtime_secrets : "${k}=${v}"])
      postgres_secret = google_secret_manager_secret.install["WEFT_POSTGRES_PASSWORD"].secret_id
      access_apps     = nonsensitive(var.access_apps_json == "") ? "" : google_secret_manager_secret.access_apps[0].secret_id
      private_ip      = google_compute_address.machine_private.address
      registry_host   = "${var.region}-docker.pkg.dev"
    })
    google-logging-enabled = "true"
    enable-oslogin         = "TRUE"
    block-project-ssh-keys = "TRUE"
  }

  depends_on = [
    google_secret_manager_secret_version.install,
    google_secret_manager_secret_iam_member.core_reads,
  ]
}

# The OAuth apps and the runtime's own provider keys, when the install has
# any (the workflow's `WEFT_ACCESS_APPS` secret).
resource "google_secret_manager_secret" "access_apps" {
  count     = nonsensitive(var.access_apps_json == "") ? 0 : 1
  secret_id = "${var.name}-access-apps"
  replication {
    auto {}
  }
  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret_version" "access_apps" {
  count       = nonsensitive(var.access_apps_json == "") ? 0 : 1
  secret      = google_secret_manager_secret.access_apps[0].id
  secret_data = var.access_apps_json
}

resource "google_secret_manager_secret_iam_member" "core_reads_access_apps" {
  count     = nonsensitive(var.access_apps_json == "") ? 0 : 1
  secret_id = google_secret_manager_secret.access_apps[0].id
  role      = "roles/secretmanager.secretAccessor"
  member    = local.core
}
