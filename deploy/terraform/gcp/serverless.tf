# weft's own roles, with no machine of the install's own: the dispatcher,
# the broker, the listener and the supervisor each a Cloud Run service
# that scales to zero while nothing needs it and out on its own load, and
# the holder a worker pool whose size weft sets to what the held signals
# need (none when there are none). They all run the same runtime image.

locals {
  # Every role that is a service of its own.
  # SYNC: these roles <-> crates/weft-platform-traits/src/roles.rs (CoreRole)
  serverless_roles = ["dispatcher", "broker", "listener", "supervisor"]
  # A Cloud Run service's own address is fixed by its name, the project
  # number and the region.
  # SYNC: the service name <-> google_cloud_run_v2_service.role
  role_urls = { for r in local.serverless_roles : r => "https://${var.name}-role-${r}-${data.google_project.this.number}.${var.region}.run.app" }
  # The roles that write (every other role writes through the broker): they
  # hear the database's announcements while they are up and wake the roles
  # at zero those writes concern, after the request that wrote has
  # answered, so their CPU stays on while they are up.
  writers     = ["dispatcher", "broker"]
  holder_pool = "${var.name}-holder"
  # The roles that open the database themselves (every other role reaches
  # it through the broker), with the most copies of each Cloud Run may
  # run. Each copy takes its share of the database's connections from
  # these numbers, so they are both the services' ceilings and part of
  # the install config.
  # SYNC: these roles <-> crates/weft-platform-traits/src/config.rs (InstallConfig::max_copies)
  max_copies = {
    dispatcher = var.dispatcher_max_copies
    broker     = var.broker_max_copies
  }
  # The memory of one copy of each role, in Mi, read from the setting
  # (`<n>Mi` or `<n>Gi`); 0 when it is neither, which the role's
  # precondition refuses.
  role_memory = {
    dispatcher = var.dispatcher_memory
    broker     = var.broker_memory
    listener   = var.listener_memory
    supervisor = var.supervisor_memory
  }
  # SYNC: reading `<n>Mi` / `<n>Gi` <-> variables.tf (holder_memory's validation)
  role_memory_mi = {
    for role, memory in local.role_memory : role => try(
      tonumber(regex("^([0-9]+)Mi$", memory)[0]),
      tonumber(regex("^([0-9]+)Gi$", memory)[0]) * 1024,
      0,
    )
  }
  # How many requests one copy of a role takes at once, where it is not
  # Cloud Run's default of 80. Every worker, listener and holder process
  # keeps one line open to the broker (a websocket, which Cloud Run counts
  # as a request for as long as it is open), and a line is mostly idle, so
  # at 80 a copy would be full of lines doing nothing while the broker's
  # copies stay capped (max_copies). 1000 is the most Cloud Run allows. An
  # idle line costs a copy about 16 KiB of read buffer plus a few KiB of
  # tasks and channels, so 1000 of them fit well inside a copy's memory;
  # the broker's CPU, not its lines, is then what makes Cloud Run add a
  # copy.
  # SYNC: broker_concurrency <-> crates/weft-broker-client/src/line.rs (LINE_READ_BUFFER)
  broker_concurrency  = 1000
  request_concurrency = {
    broker = local.broker_concurrency
  }

  # SYNC: the install config's shape <-> crates/weft-platform-traits/src/config.rs (InstallConfig)
  install_config = {
    platform = {
      kind                     = "gcp"
      name                     = var.name
      project                  = var.project_id
      region                   = var.region
      zone                     = var.zone
      network                  = google_compute_network.vpc.id
      subnet                   = google_compute_subnetwork.main.id
      artifactRegistry         = local.images_registry
      buildBucket              = google_storage_bucket.builds.name
      builderServiceAccount    = google_service_account.builder.email
      builderBaseImage         = var.builder_base_image
      tasksQueue               = google_cloud_tasks_queue.wakes.name
      wakeAttempts             = var.wake_attempts
      ticksQueue               = google_cloud_tasks_queue.ticks.name
      newsTopic                = google_pubsub_topic.news.name
      holdersTopic             = google_pubsub_topic.holders.name
      coreServiceAccount       = google_service_account.core.email
      holderPool               = "projects/${var.project_id}/locations/${var.region}/workerPools/${local.holder_pool}"
      dispatcherService        = "${var.name}-role-dispatcher"
      runtimeImage             = var.runtime_image
      deployerServiceAccount   = google_service_account.deployer.email
      frontendServiceAccount   = google_service_account.frontend.email
      workloadIdentityProvider = google_iam_workload_identity_pool_provider.github.name
      infraNetworkTag          = local.infra_tag
      buildMachine             = var.build_machine == "" ? null : var.build_machine
      # SYNC: alerts <-> alerts.tf (count = var.alerts_enabled)
      alerts = var.alerts_enabled
    }
    publicUrl = local.role_urls["dispatcher"]
    roles = merge(
      { for r in local.serverless_roles : r => "serverless" },
      { holder = "pool" },
    )
    roleUrls = local.role_urls
    # How many copies of each database-opening role may run at once: a
    # copy divides the database's connections by these to size its pools.
    maxCopies = local.max_copies
    holders = {
      signalsPerCopy   = var.signals_per_holder
      highCpuPercent   = var.holder_high_cpu_percent
      targetCpuPercent = var.holder_target_cpu_percent
    }
    edge = {
      # A caller's address, counted from the right of X-Forwarded-For
      # plus the peer. Straight to the dispatcher's service, Google's
      # front end adds the caller and is the peer (1). Through the load
      # balancer in front of the install's domains, the balancer adds the
      # caller and itself first (2). There is no outside port here.
      trustedProxyHops = {
        public  = 1
        outside = 0
        domains = 2
      }
      invalidTokensPerMinute = var.invalid_tokens_per_minute > 0 ? var.invalid_tokens_per_minute : null
    }
    objectStore = {
      kind   = "gcs"
      bucket = google_storage_bucket.files.name
    }
    source = {
      repository = var.weft_repository
      commit     = var.weft_commit
    }
    alertAfterMinutes = var.alert_after_minutes
  }
  # Which secret each environment variable of the runtime is read from.
  runtime_secrets = { for k in nonsensitive(keys(local.secrets)) : k => google_secret_manager_secret.install[k].secret_id }
}

resource "google_secret_manager_secret" "config" {
  secret_id = "${var.name}-install-config"
  replication {
    auto {}
  }
  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret_version" "config" {
  secret      = google_secret_manager_secret.config.id
  secret_data = jsonencode(local.install_config)
  lifecycle {
    # SYNC: the holders' CPU rule <-> crates/weft-platform-traits/src/config.rs (InstallConfig::validate), variables.tf (holder_*_cpu_percent validation)
    precondition {
      condition     = var.holder_target_cpu_percent + 20 <= var.holder_high_cpu_percent
      error_message = "holder_target_cpu_percent must be at least 20 under holder_high_cpu_percent: a holder says its CPU again only once it moved 10 points, so two holders merged into one can carry up to 20 more than they said, and the one left must stay under the high mark, or it hands work over at once, a holder comes back, and the pool grows and shrinks for good"
    }
  }
}

resource "google_secret_manager_secret_iam_member" "core_reads_config" {
  secret_id = google_secret_manager_secret.config.id
  role      = "roles/secretmanager.secretAccessor"
  member    = local.core
}

# The OAuth apps and the runtime's own provider keys, when the install has
# any (the workflow's `WEFT_ACCESS_APPS` secret).
resource "google_secret_manager_secret" "access_apps" {
  count     = nonsensitive(var.access_apps_json == "") ? 0 : 1
  secret_id = "${var.name}-access-apps"
  replication {
    auto {}
  }
  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret_version" "access_apps" {
  count       = nonsensitive(var.access_apps_json == "") ? 0 : 1
  secret      = google_secret_manager_secret.access_apps[0].id
  secret_data = var.access_apps_json
}

resource "google_secret_manager_secret_iam_member" "core_reads_access_apps" {
  count     = nonsensitive(var.access_apps_json == "") ? 0 : 1
  secret_id = google_secret_manager_secret.access_apps[0].id
  role      = "roles/secretmanager.secretAccessor"
  member    = local.core
}

resource "google_cloud_run_v2_service" "role" {
  for_each = toset(local.serverless_roles)
  # SYNC: the service name <-> local.role_urls, local.install_config (dispatcherService)
  name     = "${var.name}-role-${each.value}"
  location = var.region
  ingress  = "INGRESS_TRAFFIC_ALL"
  # The dispatcher is the install's public API: people, editors, a
  # provider's pushes and frontends call it, each proving who they are by
  # what they send (an operator key, a token), which it checks itself, as
  # it checks the core account on its internal routes. The broker is
  # called by every project's workers and infra, each as its project's own
  # account, which the install makes as projects come, and checks every
  # call's identity itself too. Cloud Run lets any caller through to
  # those two. This rather than a grant to allUsers, which an organization
  # made since 2024 refuses (domain-restricted sharing).
  invoker_iam_disabled = contains(["dispatcher", "broker"], each.value)
  deletion_protection  = false

  template {
    service_account = google_service_account.core.email
    # The roles read their database's address from a secret's latest
    # version, which a new version does not restart them for: a database
    # that moved (database.tf) makes a new revision through this, so no
    # role keeps writing to the old one. A digest, never the address.
    labels = {
      database = substr(nonsensitive(sha256(local.database_url)), 0, 12)
    }
    # A wake or an internal call may run as long as its work does. The
    # broker tells each line this limit, and a worker whose runs only wait
    # stops before it.
    # SYNC: timeout <-> crates/weft-platform-gcp/src/runner.rs (REQUEST_CAP)
    timeout = "3600s"

    # The roles that open the database have a ceiling, since every copy
    # takes a share of a fixed number of connections and an unbounded
    # count would run the database out of them. The others reach the
    # database through the broker, so they scale out with no ceiling
    # (null leaves the attribute unset).
    scaling {
      min_instance_count = 0
      max_instance_count = lookup(local.max_copies, each.value, null)
    }
    max_instance_request_concurrency = lookup(local.request_concurrency, each.value, null)

    vpc_access {
      egress = "PRIVATE_RANGES_ONLY"
      network_interfaces {
        network    = google_compute_network.vpc.id
        subnetwork = google_compute_subnetwork.roles.id
      }
    }

    volumes {
      name = "config"
      secret {
        secret = google_secret_manager_secret.config.secret_id
        items {
          version = "latest"
          path    = "config.json"
        }
      }
    }

    dynamic "volumes" {
      for_each = google_secret_manager_secret.access_apps
      content {
        name = "access-apps"
        secret {
          secret = volumes.value.secret_id
          items {
            version = "latest"
            path    = "access-apps.json"
          }
        }
      }
    }

    containers {
      image = var.runtime_image
      args  = ["serve", "--role", each.value]

      resources {
        # A writer wakes the roles its writes concern once the request
        # that wrote has answered, which needs its CPU then. Billed while
        # an instance is up, and none is up while nothing calls it.
        cpu_idle = !contains(local.writers, each.value)
        limits = {
          memory = local.role_memory[each.value]
        }
      }

      env {
        name  = "WEFT_CONFIG"
        value = "/etc/weft/config.json"
      }
      dynamic "env" {
        for_each = local.runtime_secrets
        content {
          name = env.key
          value_source {
            secret_key_ref {
              secret  = env.value
              version = "latest"
            }
          }
        }
      }

      volume_mounts {
        name       = "config"
        mount_path = "/etc/weft"
      }
      dynamic "env" {
        for_each = google_secret_manager_secret.access_apps
        content {
          name  = "WEFT_ACCESS_APPS_FILE"
          value = "/etc/weft-apps/access-apps.json"
        }
      }
      dynamic "volume_mounts" {
        for_each = google_secret_manager_secret.access_apps
        content {
          name       = "access-apps"
          mount_path = "/etc/weft-apps"
        }
      }
    }
  }

  lifecycle {
    # A role runs on Cloud Run's default of one CPU, which takes up to
    # 4Gi; it is never given less than Cloud Run's default of 512Mi.
    # SYNC: the memory rule <-> variables.tf (dispatcher_memory, broker_memory, listener_memory, supervisor_memory)
    precondition {
      condition     = local.role_memory_mi[each.value] >= 512 && local.role_memory_mi[each.value] <= 4096
      error_message = "${each.value}_memory is \"${local.role_memory[each.value]}\": it is a whole number of Mi or Gi from 512Mi to 4Gi (\"512Mi\", \"1Gi\", \"1536Mi\"). 512Mi is Cloud Run's default and what a role is never given less than, and above 4Gi Cloud Run needs more than the one CPU a role runs on."
    }
  }

  # A role calls Google as the core account the moment it is up (the
  # broker reaches the object store as it boots, and the first pass of the
  # broker and the dispatcher sets wakes and, on the dispatcher, sizes the
  # holders' pool), so it comes up only once the holders' pool and every
  # grant of that account exist, but the right to call the roles
  # (`core_calls_roles`, made from them, after). The install config names
  # the pool by its name alone, which orders nothing. Google still takes a
  # minute or two to apply a new grant; a loop that gave up in that window
  # is tried again as its role next starts, and a call to a role that
  # Cloud Run refused is retried by its Cloud Tasks queue.
  depends_on = [
    google_cloud_run_v2_worker_pool.holder,
    google_secret_manager_secret_version.install,
    google_secret_manager_secret_version.config,
    google_secret_manager_secret_iam_member.core_reads,
    google_secret_manager_secret_iam_member.core_reads_config,
    google_secret_manager_secret_iam_member.core_reads_access_apps,
    google_project_iam_member.core,
    google_project_iam_member.core_grants_log_writers,
    google_project_iam_member.core_project_accounts,
    google_project_iam_member.core_news_subscriptions,
    google_artifact_registry_repository_iam_member.core_registry,
    google_service_account_iam_member.core_signs_as_itself,
    google_service_account_iam_member.core_lets_repositories_deploy,
    google_storage_bucket_iam_member.core_stages_builds,
    google_storage_bucket_iam_member.core_files,
    google_storage_bucket_iam_member.core_files_bucket_read,
    google_cloud_tasks_queue_iam_member.core_deletes_wakes,
    google_pubsub_topic_iam_member.core_announces_news,
    google_pubsub_topic_iam_member.core_attaches_to_news,
    google_pubsub_topic_iam_member.core_announces_to_holders,
    google_pubsub_topic_iam_member.core_attaches_to_holders,
  ]
}

resource "google_cloud_run_v2_service_iam_member" "core_calls_roles" {
  for_each = google_cloud_run_v2_service.role
  name     = each.value.name
  location = each.value.location
  role     = "roles/run.invoker"
  member   = local.core
}

# The holders: the copies of the listener that keep open the outside
# connections some signals need between fires (a socket, a stream, a
# subscription). A worker pool takes no requests and runs exactly as many
# copies as it is told, which the dispatcher sets from the held signals and
# the CPU the holders say they use: none (and nothing billed) when there
# are none. Each holder claims the
# signals it holds through the broker, so they share the work and take
# over from one that stops.
resource "google_cloud_run_v2_worker_pool" "holder" {
  name                = local.holder_pool
  location            = var.region
  deletion_protection = false

  scaling {
    scaling_mode          = "MANUAL"
    manual_instance_count = 0
  }

  template {
    service_account = google_service_account.core.email

    # A held connection may lead to a project's own infra, on the
    # install's network.
    vpc_access {
      egress = "PRIVATE_RANGES_ONLY"
      network_interfaces {
        network    = google_compute_network.vpc.id
        subnetwork = google_compute_subnetwork.roles.id
      }
    }

    volumes {
      name = "config"
      secret {
        secret = google_secret_manager_secret.config.secret_id
        items {
          version = "latest"
          path    = "config.json"
        }
      }
    }

    containers {
      image = var.runtime_image
      args  = ["serve", "--role", "holder"]

      resources {
        limits = {
          cpu    = var.holder_cpu
          memory = var.holder_memory
        }
      }

      env {
        name  = "WEFT_CONFIG"
        value = "/etc/weft/config.json"
      }
      # A holder reaches the broker, as the core account, and hands an
      # entry's event to its project's worker, with the worker key derived
      # from the caller-ticket secret: it reads the install config and that
      # secret, and no other.
      env {
        name = "WEFT_CALLER_TOKEN_SECRET"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.install["WEFT_CALLER_TOKEN_SECRET"].secret_id
            version = "latest"
          }
        }
      }

      volume_mounts {
        name       = "config"
        mount_path = "/etc/weft"
      }
    }
  }

  lifecycle {
    # weft sets the count; an apply leaves it where weft put it.
    ignore_changes = [scaling[0].manual_instance_count]
  }

  # A holder, once one runs, subscribes to the holders topic and writes its
  # logs as the core account.
  depends_on = [
    google_secret_manager_secret_version.config,
    google_secret_manager_secret_iam_member.core_reads_config,
    google_secret_manager_secret_version.install,
    google_secret_manager_secret_iam_member.core_reads,
    google_project_iam_member.core,
    google_project_iam_member.core_news_subscriptions,
    google_pubsub_topic_iam_member.core_attaches_to_holders,
  ]
}
