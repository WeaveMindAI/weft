# What the install workflow reads back: where to push the runtime's
# images, and what it tells the person at the end.

output "registry" {
  description = "Where the runtime's own images are pushed (the install workflow reads it right after creating the registry)."
  value       = local.images_registry
}

output "url" {
  description = "The install's address: the dispatcher's own Cloud Run address, which answers with no domain at all."
  value       = local.role_urls["dispatcher"]
}

# SYNC: install_key_secret <-> crates/weft-core/src/signal_token.rs (INSTALL_KEY_SECRET),
#       .github/workflows/install-gcp.yml (every step that reads the key)
output "install_key_secret" {
  description = "The Secret Manager secret holding the install's own operator key, which the install accepts until the secret is rotated."
  value       = google_secret_manager_secret.install["WEFT_INSTALL_KEY"].secret_id
}

output "database_move" {
  description = "What the install workflow's move step needs to copy the database the roles use now into the install's own Cloud SQL: the secrets to read where from (the direct address first, when the database goes through a pooler), the one the roles read, where to, and where the copying job runs. Null without Cloud SQL."
  value = var.cloud_sql ? {
    # SYNC: from_secrets <-> secrets.tf (google_secret_manager_secret.install's secret_id)
    from_secrets = ["${var.name}-database-listen-url", "${var.name}-database-url"]
    # The one the roles read their database from: the move is done once it
    # names the install's own.
    roles_secret    = "${var.name}-database-url"
    to_secret       = google_secret_manager_secret.cloud_sql_url[0].secret_id
    service_account = google_service_account.core.email
    network         = google_compute_network.vpc.name
    subnet          = google_compute_subnetwork.roles.name
    job             = "${var.name}-database-move"
  } : null
}

output "database_check" {
  description = "What the install workflow's database history step needs to run the runtime image against the database this run deploys the roles on, before any role is deployed: the secret holding its address (the install's own Cloud SQL's when it has one), and where the checking job runs."
  value = {
    database_secret = google_secret_manager_secret.database_check.secret_id
    service_account = google_service_account.core.email
    network         = google_compute_network.vpc.name
    subnet          = google_compute_subnetwork.roles.name
    job             = "${var.name}-database-history"
  }
}
