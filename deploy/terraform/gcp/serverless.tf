# The roles placed serverless (`serverless_roles`): each a Cloud Run
# service of its own running the same runtime image, scaled to zero while
# nothing needs it. Only the core account may call one, the broker aside;
# it reaches the database and the machine over the install's network.

resource "google_secret_manager_secret" "config" {
  count     = length(var.serverless_roles) > 0 ? 1 : 0
  secret_id = "${var.name}-install-config"
  replication {
    auto {}
  }
  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret_version" "config" {
  count       = length(var.serverless_roles) > 0 ? 1 : 0
  secret      = google_secret_manager_secret.config[0].id
  secret_data = jsonencode(local.install_config)
}

resource "google_secret_manager_secret_iam_member" "core_reads_config" {
  count     = length(var.serverless_roles) > 0 ? 1 : 0
  secret_id = google_secret_manager_secret.config[0].id
  role      = "roles/secretmanager.secretAccessor"
  member    = local.core
}

# The database, from the serverless roles only: their own subnet (see
# network.tf for why not a tag).
resource "google_compute_firewall" "database_from_roles" {
  count         = length(var.serverless_roles) > 0 ? 1 : 0
  name          = "${var.name}-database-from-roles"
  network       = google_compute_network.vpc.id
  source_ranges = [local.roles_subnet_range]
  target_tags   = [local.machine_tag]
  allow {
    protocol = "tcp"
    ports    = ["5432"]
  }
}

resource "google_cloud_run_v2_service" "role" {
  for_each = toset(var.serverless_roles)
  # SYNC: the service name <-> machine.tf (local.role_urls)
  name                = "${var.name}-role-${each.value}"
  location            = var.region
  ingress             = "INGRESS_TRAFFIC_ALL"
  deletion_protection = false

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
        secret = google_secret_manager_secret.config[0].secret_id
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

# The broker is called by every project's workers and infra, each as its
# project's own account, which the install makes as projects come. Cloud
# Run lets any caller through to it, and the broker checks every call's
# identity itself, as it does on the machine's front door.
resource "google_cloud_run_v2_service_iam_member" "anyone_reaches_the_broker" {
  count    = contains(var.serverless_roles, "broker") ? 1 : 0
  name     = google_cloud_run_v2_service.role["broker"].name
  location = google_cloud_run_v2_service.role["broker"].location
  role     = "roles/run.invoker"
  member   = "allUsers"
}
