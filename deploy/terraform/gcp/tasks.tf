# The queue every wake goes through: a timer, a cron, a poll, a serverless
# role's next tick. Each wake is one task, delivered once to the role that
# set it, with the core account's identity.
resource "google_cloud_tasks_queue" "wakes" {
  name       = "${var.name}-wakes"
  location   = var.region
  depends_on = [google_project_service.apis]

  retry_config {
    max_attempts  = -1
    min_backoff   = "1s"
    max_backoff   = "300s"
    max_doublings = 8
  }
}
