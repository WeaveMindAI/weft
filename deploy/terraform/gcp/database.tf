# The install's own Postgres on Cloud SQL, when the fork asks for one
# (var.cloud_sql). The default is a Postgres the user brings
# (var.database_url, a scale-to-zero one such as Neon's free plan for a
# program that is mostly idle). Cloud SQL never sleeps and bills every hour
# it runs, but it sits in the install's own region and network: no data
# transfer quota, and no trip across the internet for every query, which is
# what a busy program wants.
#
# It is reached on a private address only, through the install's VPC: the
# roles egress on the roles subnet (serverless.tf), and Google's services
# network is peered into the VPC on a range of its own. An install that
# moves here from another Postgres has its data copied by the install
# workflow before the roles are pointed at it (its step "move the database
# into the install's own").

# A range of the VPC handed to Google's services network, where the
# instance's private address comes from. Apart from the install's subnets
# (network.tf).
resource "google_compute_global_address" "cloud_sql" {
  count         = var.cloud_sql ? 1 : 0
  name          = "${var.name}-cloud-sql"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  address       = "10.20.0.0"
  prefix_length = 20
  network       = google_compute_network.vpc.id
}

resource "google_service_networking_connection" "cloud_sql" {
  count                   = var.cloud_sql ? 1 : 0
  network                 = google_compute_network.vpc.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.cloud_sql[0].name]
  depends_on              = [google_project_service.apis]
}

resource "google_sql_database_instance" "weft" {
  count            = var.cloud_sql ? 1 : 0
  name             = "${var.name}-db"
  region           = var.region
  database_version = "POSTGRES_17"
  # Everything the install keeps lives here: removing it takes a person
  # turning this off first.
  deletion_protection = true

  settings {
    tier              = var.cloud_sql_tier
    edition           = "ENTERPRISE"
    availability_type = "ZONAL"
    disk_autoresize   = true
    disk_size         = 10
    disk_type         = "PD_SSD"

    ip_configuration {
      ipv4_enabled    = false
      private_network = google_compute_network.vpc.id
      ssl_mode        = "ENCRYPTED_ONLY"
    }

    # The dispatcher and the broker open their pools on the database
    # directly, with no pooler in between, and each copy sizes them from
    # the number the database reports, so Cloud SQL's own number for the
    # tier works as it is. The flag exists only when
    # var.cloud_sql_max_connections asks for a different one.
    dynamic "database_flags" {
      for_each = var.cloud_sql_max_connections == null ? [] : [var.cloud_sql_max_connections]
      content {
        name  = "max_connections"
        value = tostring(database_flags.value)
      }
    }

    backup_configuration {
      enabled                        = true
      point_in_time_recovery_enabled = true
    }

    deletion_protection_enabled = true
  }

  depends_on = [google_service_networking_connection.cloud_sql]
}

resource "google_sql_database" "weft" {
  count    = var.cloud_sql ? 1 : 0
  name     = "weft"
  instance = google_sql_database_instance.weft[0].name
}

resource "random_password" "cloud_sql" {
  count   = var.cloud_sql ? 1 : 0
  length  = 40
  special = false
}

resource "google_sql_user" "weft" {
  count    = var.cloud_sql ? 1 : 0
  name     = "weft"
  instance = google_sql_database_instance.weft[0].name
  password = random_password.cloud_sql[0].result
}

locals {
  # The instance's address, encrypted (`require`: the connection is TLS,
  # on the install's private network).
  cloud_sql_url = var.cloud_sql ? "postgres://weft:${random_password.cloud_sql[0].result}@${google_sql_database_instance.weft[0].private_ip_address}:5432/${google_sql_database.weft[0].name}?sslmode=require" : ""
  # The Postgres the roles use: the install's own when it has one,
  # else the one the fork names.
  database_url = var.cloud_sql ? local.cloud_sql_url : var.database_url
  # What every apply that writes that address down says when there is none.
  no_database = "the install has no database: set the repository secret WEFT_DATABASE_URL to a Postgres, or the repository variable WEFT_DATABASE to cloud_sql for one of its own"
}

# The instance's address in a secret of its own, which the workflow's move
# step copies into before the roles are pointed at it (WEFT_DATABASE_URL
# still names the database being moved from until then).
resource "google_secret_manager_secret" "cloud_sql_url" {
  count     = var.cloud_sql ? 1 : 0
  secret_id = "${var.name}-cloud-sql-url"
  replication {
    auto {}
  }
  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret_version" "cloud_sql_url" {
  count       = var.cloud_sql ? 1 : 0
  secret      = google_secret_manager_secret.cloud_sql_url[0].id
  secret_data = local.cloud_sql_url
  depends_on  = [google_sql_user.weft]
}

resource "google_secret_manager_secret_iam_member" "core_reads_cloud_sql_url" {
  count     = var.cloud_sql ? 1 : 0
  secret_id = google_secret_manager_secret.cloud_sql_url[0].id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.core.email}"
}

# The address of the database this run deploys the roles on, in a secret of
# its own that the workflow's history step writes and checks before
# anything is deployed: the roles' own (WEFT_DATABASE_URL) still names the
# database they boot on now until the cloud is created, and a run that
# moves the install to another database is checked against the new one.
resource "google_secret_manager_secret" "database_check" {
  secret_id = "${var.name}-database-check-url"
  replication {
    auto {}
  }
  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret_version" "database_check" {
  secret      = google_secret_manager_secret.database_check.id
  secret_data = local.database_url
  lifecycle {
    precondition {
      condition     = nonsensitive(local.database_url != "")
      error_message = local.no_database
    }
  }
}

resource "google_secret_manager_secret_iam_member" "core_reads_database_check" {
  secret_id = google_secret_manager_secret.database_check.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.core.email}"
}
