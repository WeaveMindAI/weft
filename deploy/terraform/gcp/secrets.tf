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

resource "random_password" "bootstrap_operator_key" {
  length  = 48
  special = false
}

locals {
  secrets = merge(
    {
      WEFT_DATABASE_URL           = var.database_url
      CREDENTIAL_ENCRYPTION_KEY   = random_bytes.sealing_key.base64
      WEFT_CALLER_TOKEN_SECRET    = random_id.caller_token_secret.hex
      WEFT_BOOTSTRAP_OPERATOR_KEY = random_password.bootstrap_operator_key.result
    },
    # Only when the database's address goes through a pooler.
    { for k, v in { WEFT_DATABASE_LISTEN_URL = var.database_listen_url } : k => v if nonsensitive(v != "") },
  )
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

# The roles read every secret.
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
