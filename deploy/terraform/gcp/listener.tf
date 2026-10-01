# The listener's own machine, when `listener_machine` is on: the listener
# alone, always up, so the connections its triggers hold never share the
# machine with Postgres. It has no public address; the machine's front
# door passes the wakes Cloud Tasks delivers on to it, and every other
# role reaches it at its private address.

resource "google_compute_address" "listener_private" {
  count        = var.listener_machine ? 1 : 0
  name         = "${var.name}-listener-private"
  address_type = "INTERNAL"
  subnetwork   = google_compute_subnetwork.main.id
  address      = cidrhost(local.subnet_range, 3)
}

resource "google_compute_instance" "listener" {
  count        = var.listener_machine ? 1 : 0
  name         = "${var.name}-listener"
  zone         = var.zone
  machine_type = var.listener_machine_type

  allow_stopping_for_update = true

  boot_disk {
    initialize_params {
      image = "cos-cloud/cos-stable"
      size  = 10
      type  = "pd-standard"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.main.id
    network_ip = google_compute_address.listener_private[0].address
  }

  service_account {
    email  = google_service_account.core.email
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  metadata = {
    user-data = templatefile("${path.module}/listener.yaml.tftpl", {
      config        = jsonencode(local.install_config)
      runtime_image = var.runtime_image
      project       = var.project_id
      secrets       = join(" ", [for k, v in local.runtime_secrets : "${k}=${v}"])
      access_apps   = nonsensitive(var.access_apps_json == "") ? "" : google_secret_manager_secret.access_apps[0].secret_id
      registry_host = "${var.region}-docker.pkg.dev"
    })
    google-logging-enabled = "true"
    enable-oslogin         = "TRUE"
    block-project-ssh-keys = "TRUE"
  }

  lifecycle {
    precondition {
      condition     = !contains(var.serverless_roles, "listener")
      error_message = "listener_machine gives the listener a machine of its own, and serverless_roles places it on Cloud Run; pick one."
    }
  }

  depends_on = [
    google_secret_manager_secret_version.install,
    google_secret_manager_secret_iam_member.core_reads,
  ]
}
