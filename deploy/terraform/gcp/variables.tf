# SYNC: the variables the install workflow sets (project_id, region, zone, database_url, database_listen_url, cloud_sql, cloud_sql_tier, cloud_sql_max_connections, dispatcher_max_copies, broker_max_copies, dispatcher_memory, broker_memory, listener_memory, supervisor_memory, holder_memory, runtime_image, builder_base_image, weft_repository, weft_commit, access_apps_json, alert_email) <-> .github/workflows/install-gcp.yml (the TF_VAR_* names)

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
  description = "The Postgres the install keeps everything in, as a connection URL (postgres://user:password@host/db?sslmode=require). Any Postgres works; one that scales to zero costs nothing while the install is idle. A pooled address is fine. Not read when cloud_sql is on, which makes the install a Postgres of its own."
  type        = string
  default     = ""
  sensitive   = true
}

variable "cloud_sql" {
  description = "Give the install a Postgres of its own on Cloud SQL, in its region and network (database.tf), instead of database_url. It bills every hour it runs and never sleeps; it has no transfer quota and no trip across the internet, which suits a busy program."
  type        = bool
  default     = false
}

variable "cloud_sql_tier" {
  description = "The Cloud SQL machine type of the install's own Postgres, when cloud_sql is on (the shared-core db-g1-small by default; a db-custom-<cpus>-<MiB> one for a busier program)."
  type        = string
  default     = "db-g1-small"
}

variable "cloud_sql_max_connections" {
  description = "How many connections the install's own Postgres takes at once, when cloud_sql is on. Unset, Cloud SQL keeps its own number for the tier, which grows with the tier's memory (50 on db-g1-small). weft reads whatever number the database reports and sizes its pools to fit, so raising this (or the tier) is the one knob for more connections. Setting it, changing it, or unsetting it again restarts the instance once, the next time the install runs."
  type        = number
  default     = null
  nullable    = true

  # Cloud SQL itself accepts 14 and up. Postgres and Cloud SQL keep a few
  # connections for themselves, so below 25 there is next to nothing left
  # to share among the copies of the dispatcher and the broker; a value
  # that low is far more likely a typo than a choice.
  validation {
    condition     = coalesce(var.cloud_sql_max_connections, 25) >= 25
    error_message = "cloud_sql_max_connections is at least 25 when set; leave it unset to keep Cloud SQL's own number for the tier."
  }
}

variable "dispatcher_max_copies" {
  description = "The most copies of the dispatcher Cloud Run runs at once. Every copy takes an equal share of the database's connections, so more copies means a smaller share each, and a copy whose share is too small to work refuses to start, naming this setting and broker_max_copies. If you need more room, give the database more connections first (cloud_sql_max_connections or a larger cloud_sql_tier), then raise this."
  type        = number
  default     = 2

  validation {
    condition     = var.dispatcher_max_copies >= 1
    error_message = "dispatcher_max_copies is at least 1."
  }
}

variable "broker_max_copies" {
  description = "The most copies of the broker Cloud Run runs at once. Every copy takes an equal share of the database's connections, so more copies means a smaller share each, and a copy whose share is too small to work refuses to start, naming this setting and dispatcher_max_copies. If you need more room, give the database more connections first (cloud_sql_max_connections or a larger cloud_sql_tier), then raise this."
  type        = number
  default     = 4

  validation {
    condition     = var.broker_max_copies >= 1
    error_message = "broker_max_copies is at least 1."
  }
}

# The memory of one copy of each role. Cloud Run's own default is 512Mi,
# and a role left at it is given 512Mi again by every apply, so each one
# is set here, where a value somebody chose survives the next install run.
# SYNC: the memory rule <-> serverless.tf (google_cloud_run_v2_service.role's precondition)
variable "dispatcher_memory" {
  description = "The memory of each copy of the dispatcher, in Mi or Gi (\"1Gi\", \"1536Mi\"), from 512Mi to 4Gi. More than the other roles by default: every version is compiled there, and on Cloud Run the files a build writes are held in memory, beside the programs the dispatcher keeps read."
  type        = string
  default     = "1Gi"
}

variable "broker_memory" {
  description = "The memory of each copy of the broker, in Mi or Gi, from 512Mi to 4Gi."
  type        = string
  default     = "512Mi"
}

variable "listener_memory" {
  description = "The memory of each copy of the listener, in Mi or Gi, from 512Mi to 4Gi."
  type        = string
  default     = "512Mi"
}

variable "supervisor_memory" {
  description = "The memory of each copy of the supervisor, in Mi or Gi, from 512Mi to 4Gi."
  type        = string
  default     = "512Mi"
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

variable "holder_high_cpu_percent" {
  description = "A holder whose CPU, averaged over a minute, reaches this percent hands part of its held signals over, keeping what brings it back to holder_target_cpu_percent: to a spare holder when one is up, else to any holder. A single signal is never split. Under this, a holder over the target hands signals over only to a spare."
  type        = number
  default     = 70
  # SYNC: the holders' CPU rule <-> crates/weft-platform-traits/src/config.rs (InstallConfig::validate), serverless.tf (the config secret's precondition)
  validation {
    condition     = var.holder_high_cpu_percent == floor(var.holder_high_cpu_percent) && var.holder_high_cpu_percent >= 21 && var.holder_high_cpu_percent <= 100
    error_message = "holder_high_cpu_percent is a whole percent from 21 to 100: a holder never uses more than 100% of its CPUs, and the target has to fit at least 20 under it."
  }
}

variable "holder_target_cpu_percent" {
  description = "The CPU each holder is meant to run at: weft runs as many holders as the CPU they say they use, summed, needs at this percent each (never fewer than the held signals need). It must be at least 20 under holder_high_cpu_percent: a holder says its CPU again only once it moved 10 points, so two holders merged into one can carry up to 20 more than they said, and the one left must stay under the high mark or the pool grows and shrinks for good."
  type        = number
  default     = 50
  # SYNC: the holders' CPU rule <-> crates/weft-platform-traits/src/config.rs (InstallConfig::validate), serverless.tf (the config secret's precondition)
  validation {
    condition     = var.holder_target_cpu_percent == floor(var.holder_target_cpu_percent) && var.holder_target_cpu_percent >= 1
    error_message = "holder_target_cpu_percent is a whole percent of 1 or more: every holder uses some CPU, so no count of them would be enough at 0."
  }
}

variable "holder_cpu" {
  description = "The CPU of each holder."
  type        = string
  default     = "1"
}

variable "holder_memory" {
  description = "The memory of each holder, in Mi or Gi (\"512Mi\", \"1Gi\"), at least 512Mi; Cloud Run refuses more than its CPU (`holder_cpu`) takes."
  type        = string
  default     = "512Mi"
  # The roles' 512Mi floor, without their 4Gi ceiling: a holder's CPU is
  # its own setting, and Cloud Run refuses more memory than that CPU takes.
  # SYNC: reading `<n>Mi` / `<n>Gi` <-> serverless.tf (local.role_memory_mi)
  validation {
    condition = try(
      tonumber(regex("^([0-9]+)Mi$", var.holder_memory)[0]),
      tonumber(regex("^([0-9]+)Gi$", var.holder_memory)[0]) * 1024,
      0,
    ) >= 512
    error_message = "holder_memory is a whole number of Mi or Gi of at least 512Mi (\"512Mi\", \"1Gi\"), Cloud Run's default and what a holder is never given less than."
  }
}

variable "runtime_image" {
  description = "The weft-runtime image every role runs (the install workflow builds it and passes its ref)."
  type        = string
}

variable "builder_base_image" {
  description = "The image a project's worker compiles in (the install workflow builds it and passes its ref)."
  type        = string
}

variable "build_machine" {
  description = "The Cloud Build machine a program's worker image compiles on. Empty is Cloud Build's default machine, which its free build minutes cover; E2_HIGHCPU_8 or E2_HIGHCPU_32 compile faster and are paid by the minute. An image is built once per version of a program, never per worker started."
  type        = string
  default     = ""
  validation {
    condition     = contains(["", "E2_MEDIUM", "E2_HIGHCPU_8", "E2_HIGHCPU_32", "N1_HIGHCPU_8", "N1_HIGHCPU_32"], var.build_machine)
    error_message = "build_machine is empty (the free default) or one of E2_MEDIUM, E2_HIGHCPU_8, E2_HIGHCPU_32, N1_HIGHCPU_8, N1_HIGHCPU_32."
  }
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

variable "wakes_retry_window" {
  description = "How long the wakes queue keeps retrying a wake its receiver fails (a user's timer, cron, poll or provider renewal, and a holder's liveness check). \"0s\" is no limit short of Cloud Tasks' own task retention: each of these wakes sets the next, so one dropped ends its chain for good. The listener itself stops after var.wake_attempts deliveries of a trigger's wake, giving the trigger up where its user sees it."
  type        = string
  default     = "0s"
}

variable "wake_attempts" {
  description = "How many deliveries of one trigger's wake (a timer, cron, poll or provider renewal) the listener answers while it keeps failing; a delivery that never reached it (the machine down, a 5xx from in front of it) is not counted. A failure on the last gives the trigger up, shown on its node, until a resync of its project (weft resync) registers it again, or, for one that signs in through a connection, that connection is reconnected. The holder's liveness check on the same queue is not bounded by it. With the default backoff, 20 is about an hour of failing tries."
  type        = number
  default     = 20

  validation {
    condition     = var.wake_attempts >= 1
    error_message = "wake_attempts must be 1 or more: every wake is delivered at least once."
  }
}

variable "wakes_longest_backoff" {
  description = "The longest pause between two tries of a wake the wakes queue keeps retrying. It is also how late a cron, a poll or a holder's liveness check can resume once its receiver is back after an outage."
  type        = string
  default     = "300s"
}

variable "alerts_enabled" {
  description = "Whether weft emails you when its own work is stuck (what `weft status` lists under `waiting on weft:`): one Cloud Monitoring alert policy on the line weft logs then, and an email channel. false makes neither, and weft logs no such line."
  type        = bool
  default     = true
}

variable "alert_after_minutes" {
  description = "How many minutes weft's own work may keep failing while weft still retries it before it counts as stuck and alerts. Work weft gave up retrying alerts at once."
  type        = number
  default     = 5

  validation {
    condition     = var.alert_after_minutes >= 0 && floor(var.alert_after_minutes) == var.alert_after_minutes
    error_message = "alert_after_minutes is a whole number of minutes, 0 or more."
  }
}

variable "alert_email" {
  description = "Where the alerts are emailed. The install workflow sets it from the repository secret WEFT_ALERT_EMAIL, or else to the first person (by address) holding the Owner role directly on the project, usually the Google account that made it. Needed when alerts_enabled is true."
  type        = string
  default     = ""
}
