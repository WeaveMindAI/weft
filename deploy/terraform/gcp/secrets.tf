# Every secret the runtime reads from its environment, in Secret Manager.
# The machine reads them at boot into a file only root can read; a
# serverless role gets them as its service's environment.
#
# SYNC: these names <-> crates/weft-platform-traits/src/config.rs (SECRET_ENV),
#       except WEFT_POSTGRES_PASSWORD (the database container's, not the
#       runtime's) and WEFT_IDENTITY_KEY (local installs only: on GCP a
#       caller proves who it is with a Google identity token instead)

# The key stored credentials are sealed with. Generated once and kept in
# this state; it cannot be rotated, since rows sealed with it stop opening
# under any other key.
resource "random_bytes" "sealing_key" {
  length = 32
}

resource "random_password" "database" {
  length  = 32
  special = false
}

resource "random_id" "caller_token_secret" {
  byte_length = 32
}

resource "random_password" "bootstrap_operator_key" {
  length  = 48
  special = false
}

locals {
  secrets = {
    WEFT_DATABASE_URL            = "postgres://weft:${random_password.database.result}@${google_compute_address.machine_private.address}:5432/weft"
    CREDENTIAL_ENCRYPTION_KEY    = random_bytes.sealing_key.base64
    WEFT_CALLER_TOKEN_SECRET     = random_id.caller_token_secret.hex
    WEFT_BOOTSTRAP_OPERATOR_KEY  = random_password.bootstrap_operator_key.result
    WEFT_OBJECT_STORE_ACCESS_KEY = google_storage_hmac_key.object_store.access_id
    WEFT_OBJECT_STORE_SECRET_KEY = google_storage_hmac_key.object_store.secret
    WEFT_POSTGRES_PASSWORD       = random_password.database.result
  }
}

resource "google_secret_manager_secret" "install" {
  for_each  = nonsensitive(toset(keys(local.secrets)))
  secret_id = "${var.name}-${lower(replace(each.value, "_", "-"))}"
  replication {
    auto {}
  }
  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret_version" "install" {
  for_each    = nonsensitive(toset(keys(local.secrets)))
  secret      = google_secret_manager_secret.install[each.value].id
  secret_data = local.secrets[each.value]
}

# The machine and the serverless roles read every secret.
resource "google_secret_manager_secret_iam_member" "core_reads" {
  for_each  = nonsensitive(toset(keys(local.secrets)))
  secret_id = google_secret_manager_secret.install[each.value].id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.core.email}"
}

# The core grants each project's own account the ticket secret (its
# workers check a live caller's ticket with it), and nothing else.
resource "google_secret_manager_secret_iam_member" "core_shares_ticket_secret" {
  secret_id = google_secret_manager_secret.install["WEFT_CALLER_TOKEN_SECRET"].id
  role      = "roles/secretmanager.admin"
  member    = "serviceAccount:${google_service_account.core.email}"
}
