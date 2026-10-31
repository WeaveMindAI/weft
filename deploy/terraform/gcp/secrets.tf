# Every secret the runtime reads from its environment, in Secret Manager,
# which each role gets as its service's environment.
#
# SYNC: these names <-> crates/weft-platform-traits/src/config.rs (SECRET_ENV),
#       except WEFT_IDENTITY_KEY (local installs only: on GCP a caller
#       proves who it is with a Google identity token instead) and the
#       object store's two keys (a Cloud Storage bucket is reached as the
#       core account itself)

# The key stored credentials are sealed with. Generated once and kept in
# this state; it cannot be rotated, since rows sealed with it stop opening
# under any other key.
resource "random_bytes" "sealing_key" {
  length = 32
}

resource "random_id" "caller_token_secret" {
  byte_length = 32
}

# The install's own operator key, accepted with no row of its own until
# its secret is rotated (`weft_core::signal_token::INSTALL_KEY_SECRET`).
resource "random_password" "install_key" {
  length  = 48
  special = false
}

# Its name before it was the install's key: moved, so its value (the key
# every operator of the install holds) stays what it was.
moved {
  from = random_password.bootstrap_operator_key
  to   = random_password.install_key
}

locals {
  secrets = merge(
    {
      WEFT_DATABASE_URL         = local.database_url
      CREDENTIAL_ENCRYPTION_KEY = random_bytes.sealing_key.base64
      WEFT_CALLER_TOKEN_SECRET  = random_id.caller_token_secret.hex
      # SYNC: WEFT_INSTALL_KEY <-> crates/weft-core/src/signal_token.rs (INSTALL_KEY_SECRET),
      #       outputs.tf (install_key_secret)
      WEFT_INSTALL_KEY = random_password.install_key.result
    },
    # Only when the database's address goes through a pooler (never the
    # install's own Cloud SQL, reached directly).
    { for k, v in { WEFT_DATABASE_LISTEN_URL = var.database_listen_url } : k => v if !var.cloud_sql && nonsensitive(v != "") },
  )
}

# Named after the install and the variable without its own `WEFT_`
# (`weft-caller-token-secret`, not `weft-weft-caller-token-secret`).
# SYNC: secret_id <-> outputs.tf (database_move's from_secrets)
# Renaming replaces a secret, which keeps its value (every value comes from
# this state), and the new one is made before the old one goes, so the
# roles never point at a secret that is not there.
resource "google_secret_manager_secret" "install" {
  for_each  = nonsensitive(toset(keys(local.secrets)))
  secret_id = "${var.name}-${lower(replace(trimprefix(each.value, "WEFT_"), "_", "-"))}"
  replication {
    auto {}
  }
  depends_on = [google_project_service.apis]
  lifecycle {
    create_before_destroy = true
  }
}

resource "google_secret_manager_secret_version" "install" {
  for_each    = nonsensitive(toset(keys(local.secrets)))
  secret      = google_secret_manager_secret.install[each.value].id
  secret_data = local.secrets[each.value]
  lifecycle {
    precondition {
      condition     = nonsensitive(local.database_url != "")
      error_message = local.no_database
    }
  }
}

# The roles read every secret.
resource "google_secret_manager_secret_iam_member" "core_reads" {
  for_each  = nonsensitive(toset(keys(local.secrets)))
  secret_id = google_secret_manager_secret.install[each.value].id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.core.email}"
}

