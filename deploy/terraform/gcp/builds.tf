# Cloud Build announces every change of a build's state on the
# `cloud-builds` topic, once that topic exists. Each announcement wakes
# the dispatcher (its tick), and its build loop records the end at once
# (crates/weft-dispatcher/src/build/follow.rs).

# Cloud Build fixes the topic's name, so it is the project's, shared by
# every install in it: the install workflow creates it when it is missing,
# and no install's destroy takes it from the others.
# SYNC: the topic name <-> .github/workflows/install-gcp.yml (the build notices topic step)
data "google_pubsub_topic" "cloud_builds" {
  name = "cloud-builds"
}

# Pub/Sub presents the core account's identity to the dispatcher, as the
# wakes queue does, which its own service agent may only do when allowed.
resource "google_service_account_iam_member" "pubsub_signs_as_core" {
  service_account_id = google_service_account.core.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:service-${data.google_project.this.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
  depends_on         = [google_project_service.apis]
}

# The push names the loop it wakes, so the tick runs the build loop and
# leaves the dispatcher's other loops to their own times.
# SYNC: the tick path and its `loop` query <-> crates/weft-platform-traits/src/roles.rs (TICK_PATH), crates/weft-runtime/src/server.rs (TICK_LOOP_PARAM), crates/weft-dispatcher/src/build/follow.rs (the loop's name)
resource "google_pubsub_subscription" "builds_wake_the_dispatcher" {
  name  = "${var.name}-builds-wake-the-dispatcher"
  topic = data.google_pubsub_topic.cloud_builds.id

  push_config {
    push_endpoint = "${local.role_urls["dispatcher"]}/_weft/tick?loop=image_builds"
    oidc_token {
      service_account_email = google_service_account.core.email
      audience              = local.role_urls["dispatcher"]
    }
  }

  # A wake that cannot be delivered is worth nothing later: the build
  # loop's own next look comes within seconds anyway.
  message_retention_duration = "600s"
  ack_deadline_seconds       = 600
  # Weeks without a build are normal, and an expired subscription would
  # stop every wake after them.
  expiration_policy {
    ttl = ""
  }
  depends_on = [google_service_account_iam_member.pubsub_signs_as_core]
}
