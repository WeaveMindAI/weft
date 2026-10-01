# Where the runtime's own images, workers and infra nodes are pushed and
# pulled from. Only the install writes here: its tags name what went into
# an image rather than a digest, so anyone who could push here could put
# their code under a tag the runtime is about to pull.
locals {
  # The address images are tagged with (`<region>-docker.pkg.dev/<project>/<repository>`).
  images_registry = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.images.repository_id}"
}

resource "google_artifact_registry_repository" "images" {
  location      = var.region
  repository_id = var.name
  format        = "DOCKER"
  depends_on    = [google_project_service.apis]

  # Every upgrade pushes a new runtime and builder base; the last three of
  # each stay (the running one and a couple to go back to), older ones go
  # after a week. Worker and infra images are the install's own to
  # reclaim: it deletes each one nothing runs any more after every build.
  cleanup_policy_dry_run = false
  cleanup_policies {
    id     = "keep-recent-runtimes"
    action = "KEEP"
    most_recent_versions {
      package_name_prefixes = ["weft-runtime", "weft-builder-base"]
      keep_count            = 3
    }
  }
  cleanup_policies {
    id     = "drop-old-runtimes"
    action = "DELETE"
    condition {
      package_name_prefixes = ["weft-runtime", "weft-builder-base"]
      older_than            = "604800s"
    }
  }
}

# Where project frontends are pushed, by each frontend repository's CI.
# A repository of its own, so a frontend credential can write nothing the
# runtime runs.
resource "google_artifact_registry_repository" "frontends" {
  location      = var.region
  repository_id = "${var.name}-frontends"
  format        = "DOCKER"
  depends_on    = [google_project_service.apis]

  # Every frontend deploy pushes an image: each frontend keeps its five
  # newest, and older ones go after a month.
  cleanup_policy_dry_run = false
  cleanup_policies {
    id     = "keep-recent-deploys"
    action = "KEEP"
    most_recent_versions {
      keep_count = 5
    }
  }
  cleanup_policies {
    id     = "drop-old-deploys"
    action = "DELETE"
    condition {
      older_than = "2592000s"
    }
  }
}
