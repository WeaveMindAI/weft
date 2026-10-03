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

output "bootstrap_operator_key_secret" {
  description = "The Secret Manager secret holding the first operator key."
  value       = google_secret_manager_secret.install["WEFT_BOOTSTRAP_OPERATOR_KEY"].secret_id
}
