# weft's own roles, with no machine of the install's own: the dispatcher,
# the broker, the listener and the supervisor each a Cloud Run service
# that scales to zero while nothing needs it and out on its own load, and
# the holder a worker pool whose size weft sets to what the held signals
# need (none when there are none). They all run the same runtime image.

locals {
  # Every role that is a service of its own.
  # SYNC: these roles <-> crates/weft-platform-traits/src/roles.rs (CoreRole)
  serverless_roles = ["dispatcher", "broker", "listener", "supervisor"]
  # A Cloud Run service's own address is fixed by its name, the project
  # number and the region.
  # SYNC: the service name <-> google_cloud_run_v2_service.role
  role_urls = { for r in local.serverless_roles : r => "https://${var.name}-role-${r}-${data.google_project.this.number}.${var.region}.run.app" }
  # The roles that write (every other role writes through the broker): they
  # hear the database's announcements while they are up and wake the roles
  # at zero those writes concern, after the request that wrote has
  # answered, so their CPU stays on while they are up.
  writers     = ["dispatcher", "broker"]
  holder_pool = "${var.name}-holder"

  # SYNC: the install config's shape <-> crates/weft-platform-traits/src/config.rs (InstallConfig)
  install_config = {
    platform = {
      kind                     = "gcp"
      name                     = var.name
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
      holderPool               = "projects/${var.project_id}/locations/${var.region}/workerPools/${local.holder_pool}"
      dispatcherService        = "${var.name}-role-dispatcher"
      runtimeImage             = var.runtime_image
      deployerServiceAccount   = google_service_account.deployer.email
      frontendServiceAccount   = google_service_account.frontend.email
      workloadIdentityProvider = google_iam_workload_identity_pool_provider.github.name
      infraNetworkTag          = local.infra_tag
      buildMachine             = var.build_machine == "" ? null : var.build_machine
    }
    auth      = "operator_keys"
    publicUrl = local.role_urls["dispatcher"]
    roles = merge(
      { for r in local.serverless_roles : r => "serverless" },
      { holder = "pool" },
    )
    roleUrls = local.role_urls
    holders = {
      signalsPerCopy = var.signals_per_holder
    }
    build = {
      compileLanes     = var.compile_lanes
      builderBaseImage = var.builder_base_image
      runtimeBaseImage = "debian:bookworm-slim"
    }
    edge = {
      # A caller's address, counted from the right of X-Forwarded-For
      # plus the peer. Straight to the dispatcher's service, Google's
      # front end adds the caller and is the peer (1). Through the load
      # balancer in front of the install's domains, the balancer adds the
      # caller and itself first (2). There is no outside port here.
      trustedProxyHops = {
        public  = 1
        outside = 0
        domains = 2
      }
      invalidTokensPerMinute = var.invalid_tokens_per_minute > 0 ? var.invalid_tokens_per_minute : null
    }
    objectStore = {
      kind   = "gcs"
      bucket = google_storage_bucket.files.name
    }
    source = {
      repository = var.weft_repository
      commit     = var.weft_commit
    }
  }
  # Which secret each environment variable of the runtime is read from.
  runtime_secrets = { for k in nonsensitive(keys(local.secrets)) : k => google_secret_manager_secret.install[k].secret_id }
}

resource "google_secret_manager_secret" "config" {
  secret_id = "${var.name}-install-config"
  replication {
    auto {}
  }
  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret_version" "config" {
  secret      = google_secret_manager_secret.config.id
  secret_data = jsonencode(local.install_config)
}

resource "google_secret_manager_secret_iam_member" "core_reads_config" {
  secret_id = google_secret_manager_secret.config.id
  role      = "roles/secretmanager.secretAccessor"
  member    = local.core
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

resource "google_cloud_run_v2_service" "role" {
  for_each = toset(local.serverless_roles)
  # SYNC: the service name <-> local.role_urls, local.install_config (dispatcherService)
  name     = "${var.name}-role-${each.value}"
  location = var.region
  ingress  = "INGRESS_TRAFFIC_ALL"
  # The dispatcher is the install's public API: people, editors, a
  # provider's pushes and frontends call it, each proving who they are by
  # what they send (an operator key, a token), which it checks itself, as
  # it checks the core account on its internal routes. The broker is
  # called by every project's workers and infra, each as its project's own
  # account, which the install makes as projects come, and checks every
  # call's identity itself too. Cloud Run lets any caller through to
  # those two. This rather than a grant to allUsers, which an organization
  # made since 2024 refuses (domain-restricted sharing).
  invoker_iam_disabled = contains(["dispatcher", "broker"], each.value)
  deletion_protection  = false

  template {
    service_account = google_service_account.core.email
    # A wake or an internal call may run as long as its work does.
    timeout = "3600s"

    scaling {
      min_instance_count = 0
    }

    vpc_access {
      egress = "PRIVATE_RANGES_ONLY"
      network_interfaces {
        network    = google_compute_network.vpc.id
        subnetwork = google_compute_subnetwork.roles.id
      }
    }

    volumes {
      name = "config"
      secret {
        secret = google_secret_manager_secret.config.secret_id
        items {
          version = "latest"
          path    = "config.json"
        }
      }
    }

    dynamic "volumes" {
      for_each = google_secret_manager_secret.access_apps
      content {
        name = "access-apps"
        secret {
          secret = volumes.value.secret_id
          items {
            version = "latest"
            path    = "access-apps.json"
          }
        }
      }
    }

    containers {
      image = var.runtime_image
      args  = ["serve", "--role", each.value]

      resources {
        # A writer wakes the roles its writes concern once the request
        # that wrote has answered, which needs its CPU then. Billed while
        # an instance is up, and none is up while nothing calls it.
        cpu_idle = !contains(local.writers, each.value)
      }

      env {
        name  = "WEFT_CONFIG"
        value = "/etc/weft/config.json"
      }
      dynamic "env" {
        for_each = local.runtime_secrets
        content {
          name = env.key
          value_source {
            secret_key_ref {
              secret  = env.value
              version = "latest"
            }
          }
        }
      }

      volume_mounts {
        name       = "config"
        mount_path = "/etc/weft"
      }
      dynamic "env" {
        for_each = google_secret_manager_secret.access_apps
        content {
          name  = "WEFT_ACCESS_APPS_FILE"
          value = "/etc/weft-apps/access-apps.json"
        }
      }
      dynamic "volume_mounts" {
        for_each = google_secret_manager_secret.access_apps
        content {
          name       = "access-apps"
          mount_path = "/etc/weft-apps"
        }
      }
    }
  }

  depends_on = [
    google_secret_manager_secret_version.install,
    google_secret_manager_secret_version.config,
    google_secret_manager_secret_iam_member.core_reads,
    google_secret_manager_secret_iam_member.core_reads_config,
    google_secret_manager_secret_iam_member.core_reads_access_apps,
  ]
}

resource "google_cloud_run_v2_service_iam_member" "core_calls_roles" {
  for_each = google_cloud_run_v2_service.role
  name     = each.value.name
  location = each.value.location
  role     = "roles/run.invoker"
  member   = local.core
}

# The holders: the copies of the listener that keep open the outside
# connections some signals need between fires (a socket, a stream, a
# subscription). A worker pool takes no requests and runs exactly as many
# copies as it is told, which the dispatcher sets from the held signals:
# none (and nothing billed) when there are none. Each holder claims the
# signals it holds through the broker, so they share the work and take
# over from one that stops.
resource "google_cloud_run_v2_worker_pool" "holder" {
  name                = local.holder_pool
  location            = var.region
  deletion_protection = false

  scaling {
    scaling_mode          = "MANUAL"
    manual_instance_count = 0
  }

  template {
    service_account = google_service_account.core.email

    # A held connection may lead to a project's own infra, on the
    # install's network.
    vpc_access {
      egress = "PRIVATE_RANGES_ONLY"
      network_interfaces {
        network    = google_compute_network.vpc.id
        subnetwork = google_compute_subnetwork.roles.id
      }
    }

    volumes {
      name = "config"
      secret {
        secret = google_secret_manager_secret.config.secret_id
        items {
          version = "latest"
          path    = "config.json"
        }
      }
    }

    containers {
      image = var.runtime_image
      args  = ["serve", "--role", "holder"]

      resources {
        limits = {
          cpu    = var.holder_cpu
          memory = var.holder_memory
        }
      }

      env {
        name  = "WEFT_CONFIG"
        value = "/etc/weft/config.json"
      }
      # A holder reaches the broker, as the core account, and hands an
      # entry's event to its project's worker, with the worker key derived
      # from the caller-ticket secret: it reads the install config and that
      # secret, and no other.
      env {
        name = "WEFT_CALLER_TOKEN_SECRET"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.install["WEFT_CALLER_TOKEN_SECRET"].secret_id
            version = "latest"
          }
        }
      }

      volume_mounts {
        name       = "config"
        mount_path = "/etc/weft"
      }
    }
  }

  lifecycle {
    # weft sets the count; an apply leaves it where weft put it.
    ignore_changes = [scaling[0].manual_instance_count]
  }

  depends_on = [
    google_secret_manager_secret_version.config,
    google_secret_manager_secret_iam_member.core_reads_config,
    google_secret_manager_secret_version.install,
    google_secret_manager_secret_iam_member.core_reads,
  ]
}
