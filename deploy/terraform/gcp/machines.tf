# A machine running a unit of a project's infra says itself when how its
# unit stands changes (its agent asks the broker for a look). One thing
# it cannot say is that the whole machine went away: Compute Engine
# stopped it, a host failed, a preemptible one was taken back. Compute
# Engine writes each of those to the project's audit logs, and this
# carries them to the supervisor's tick, which looks at the health of
# what it owns. Nothing else wakes the supervisor while infra runs fine.

resource "google_pubsub_topic" "machine_events" {
  name       = "${var.name}-machine-events"
  depends_on = [google_project_service.apis]
}

# Every infra machine is named `wi-...` (crates/weft-core/src/infra/resolve.rs,
# resource_base). Another install in the same project shares the prefix,
# which costs its supervisor one look that finds nothing.
# SYNC: the `wi-` prefix <-> crates/weft-core/src/infra/resolve.rs (NodeRef::resource_base)
resource "google_logging_project_sink" "machine_events" {
  name        = "${var.name}-machine-events"
  destination = "pubsub.googleapis.com/${google_pubsub_topic.machine_events.id}"
  filter      = <<-EOT
    resource.type="gce_instance"
    protoPayload.resourceName:"/instances/wi-"
    protoPayload.methodName=("compute.instances.preempted" OR "compute.instances.hostError" OR "compute.instances.guestTerminate" OR "compute.instances.automaticRestart" OR "v1.compute.instances.stop" OR "v1.compute.instances.suspend" OR "v1.compute.instances.delete" OR "v1.compute.instances.reset")
  EOT
  unique_writer_identity = true
  depends_on             = [google_workload_identity_service_agent.logging]
}

# The sink writes as Cloud Logging's service agent, which Google makes on
# its own only some time after the API is first used: on a new project the
# grant to the sink's writer can name an account that does not exist yet.
# So the agent is asked for before the sink, the way Google's docs say to
# for infrastructure as code. All it needs here is to publish to the topic,
# granted below. Another install in the same project shares the agent; the
# provider never deletes one, so a destroy leaves it.
resource "google_workload_identity_service_agent" "logging" {
  parent     = "projects/${data.google_project.this.number}/locations/global/serviceProducers/logging.googleapis.com"
  depends_on = [google_project_service.apis]
}

resource "google_pubsub_topic_iam_member" "machine_events_published_by_the_sink" {
  topic  = google_pubsub_topic.machine_events.id
  role   = "roles/pubsub.publisher"
  member = google_logging_project_sink.machine_events.writer_identity
}

# The push presents the core account's identity, as the build
# notifications do (builds.tf grants Pub/Sub that).
# SYNC: the tick path <-> crates/weft-platform-traits/src/roles.rs (TICK_PATH)
resource "google_pubsub_subscription" "machine_events_wake_the_supervisor" {
  name  = "${var.name}-machine-events-wake-the-supervisor"
  topic = google_pubsub_topic.machine_events.id

  push_config {
    push_endpoint = "${local.role_urls["supervisor"]}/_weft/tick"
    oidc_token {
      service_account_email = google_service_account.core.email
      audience              = local.role_urls["supervisor"]
    }
  }

  # A look that cannot be delivered soon is worth nothing later: the next
  # event, or the next command, brings a fresh one.
  message_retention_duration = "600s"
  ack_deadline_seconds       = 600
  # Weeks without a machine event are normal.
  expiration_policy {
    ttl = ""
  }
  depends_on = [google_service_account_iam_member.pubsub_signs_as_core]
}
