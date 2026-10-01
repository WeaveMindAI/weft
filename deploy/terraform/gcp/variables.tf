variable "project_id" {
  description = "The GCP project the install lives in."
  type        = string
}

variable "region" {
  description = "Where everything lives. The free e2-micro machine is free only in us-west1, us-central1 and us-east1."
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "The zone the machine and infra machines run in, inside `region`."
  type        = string
  default     = "us-central1-a"
}

variable "name" {
  description = "Prefix of every resource this creates, so two installs can share a project."
  type        = string
  default     = "weft"
}

variable "machine_type" {
  description = "The machine running Postgres, the front door and the listener (unless listener_machine gives it a machine of its own). e2-micro is in the free tier; grow it (e2-small, e2-medium, ...) when the install outgrows 1 GB of memory."
  type        = string
  default     = "e2-micro"
}

variable "listener_machine" {
  description = "Give the listener a machine of its own that stays up, instead of sharing the machine with Postgres. Reach for it when the triggers that hold a connection open (sockets, streams, SSE subscriptions) load the machine."
  type        = bool
  default     = false
}

variable "listener_machine_type" {
  description = "The listener's own machine, when listener_machine is on."
  type        = string
  default     = "e2-small"
}

variable "data_disk_gb" {
  description = "The size of the disk holding the database and the front door's certificates. It can grow later (never shrink); the free tier covers 30 GB of standard disk, boot disk included."
  type        = number
  default     = 20
}

variable "data_disk_type" {
  description = "The data disk's type: pd-standard (free tier, slow), pd-balanced or pd-ssd."
  type        = string
  default     = "pd-standard"
}

variable "runtime_image" {
  description = "The weft-runtime image the machine and any serverless role run (the install workflow builds it and passes its ref)."
  type        = string
}

variable "builder_base_image" {
  description = "The image a project's worker compiles in (the install workflow builds it and passes its ref)."
  type        = string
}

variable "serverless_roles" {
  description = "Roles run as Cloud Run services of their own, each scaling to zero and out on its own load (any of dispatcher, broker, listener, supervisor). A role left out runs on the machine. The listener stays on a machine by default: a trigger that holds a connection open needs it up between events."
  type        = list(string)
  default     = ["dispatcher", "broker", "supervisor"]

  validation {
    condition     = alltrue([for r in var.serverless_roles : contains(["dispatcher", "broker", "listener", "supervisor"], r)])
    error_message = "serverless_roles holds only dispatcher, broker, listener and supervisor."
  }
}

variable "compile_lanes" {
  description = "How many project builds run side by side (each is one Cloud Build job)."
  type        = number
  default     = 2
}

variable "invalid_tokens_per_minute" {
  description = "Refused tokens one address may present per minute on the token doors before every token door refuses it for the rest of the minute. 0 turns the block off."
  type        = number
  default     = 30
}

variable "frontend_repos" {
  description = "GitHub repositories (owner/name) whose Actions may deploy a frontend beside the install."
  type        = list(string)
  default     = []
}

variable "weft_repository" {
  description = "The weft repository (owner/name) the install runs, so a project's CI builds the same CLI."
  type        = string
}

variable "weft_commit" {
  description = "The weft commit the install runs."
  type        = string
}

variable "access_apps_json" {
  description = "The access-apps.json holding the OAuth apps and the runtime's own provider keys, or empty for none."
  type        = string
  default     = ""
  sensitive   = true
}
