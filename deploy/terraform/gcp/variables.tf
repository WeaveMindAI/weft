# SYNC: the variables the install workflow sets (project_id, region, zone, database_url, database_listen_url, runtime_image, builder_base_image, weft_repository, weft_commit, access_apps_json) <-> .github/workflows/install-gcp.yml (the TF_VAR_* names)

variable "project_id" {
  description = "The GCP project the install lives in."
  type        = string
}

variable "region" {
  description = "Where everything lives."
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "The zone infra machines run in, inside `region`."
  type        = string
  default     = "us-central1-a"
}

variable "name" {
  description = "Prefix of every resource this creates, so two installs can share a project."
  type        = string
  default     = "weft"
}

variable "database_url" {
  description = "The Postgres the install keeps everything in, as a connection URL (postgres://user:password@host/db?sslmode=require). Any Postgres works; one that scales to zero costs nothing while the install is idle. A pooled address is fine."
  type        = string
  sensitive   = true
}

variable "database_listen_url" {
  description = "A direct (session) address of the same database, when database_url goes through a pooler that hands out a connection per transaction: a LISTEN needs a session of its own (the runtime refuses to start, naming this, when it cannot listen). Empty when database_url is already one."
  type        = string
  default     = ""
  sensitive   = true
}

variable "signals_per_holder" {
  description = "The most held signals (sockets, streams, subscriptions kept open) one holder takes; weft runs one more holder per this many."
  type        = number
  default     = 200
}

variable "holder_cpu" {
  description = "The CPU of each holder."
  type        = string
  default     = "1"
}

variable "holder_memory" {
  description = "The memory of each holder."
  type        = string
  default     = "512Mi"
}

variable "runtime_image" {
  description = "The weft-runtime image every role runs (the install workflow builds it and passes its ref)."
  type        = string
}

variable "builder_base_image" {
  description = "The image a project's worker compiles in (the install workflow builds it and passes its ref)."
  type        = string
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
