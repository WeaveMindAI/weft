# The object store: one bucket, reached over GCS's S3-compatible API
# with an HMAC key pair of a service account that may use only it.

resource "google_storage_bucket" "files" {
  name                        = "${var.project_id}-${var.name}-files"
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  # An upload a caller started and never finished is dropped after a day.
  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }

  # Download links are signed for the browser, which fetches them from
  # whatever origin the frontend is served on.
  cors {
    origin          = ["*"]
    method          = ["GET", "HEAD", "PUT"]
    response_header = ["Content-Type", "Content-Length", "ETag"]
    max_age_seconds = 3600
  }
}

resource "google_service_account" "object_store" {
  account_id   = "${var.name}-object-store"
  display_name = "weft object store (HMAC key holder)"
}

resource "google_storage_bucket_iam_member" "object_store" {
  bucket = google_storage_bucket.files.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.object_store.email}"
}

resource "google_storage_bucket_iam_member" "object_store_bucket_read" {
  bucket = google_storage_bucket.files.name
  role   = "roles/storage.legacyBucketReader"
  member = "serviceAccount:${google_service_account.object_store.email}"
}

resource "google_storage_hmac_key" "object_store" {
  service_account_email = google_service_account.object_store.email
}

# Where a project build's context is staged for Cloud Build. A staged
# context is used once, so it is dropped after a day.
resource "google_storage_bucket" "builds" {
  name                        = "${var.project_id}-${var.name}-builds"
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "Delete"
    }
  }
}
