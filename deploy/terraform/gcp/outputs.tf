# What the install workflow reads back: where to push the runtime's
# images, and what it tells the person at the end.

output "registry" {
  description = "Where the runtime's own images are pushed (the install workflow reads it right after creating the registry)."
  value       = local.images_registry
}

output "address" {
  description = "The install's address (https://<this>), and what every domain's DNS record points at."
  value       = google_compute_address.public.address
}

output "machine" {
  description = "The machine's name, for `gcloud compute ssh --tunnel-through-iap`."
  value       = google_compute_instance.machine.name
}

output "bootstrap_operator_key_secret" {
  description = "The Secret Manager secret holding the first operator key."
  value       = google_secret_manager_secret.install["WEFT_BOOTSTRAP_OPERATOR_KEY"].secret_id
}

output "listener_machine" {
  description = "The listener's own machine, when listener_machine is on."
  value       = var.listener_machine ? google_compute_instance.listener[0].name : null
}
