# One VPC, private: nothing of the install's own takes calls on it. The
# roles (Cloud Run, Direct VPC egress) reach a project's infra machines on
# it, and infra machines reach the internet through NAT. Infra machines
# have no public address.
#
# The firewall tells callers apart by subnet, never by network tag: a tag
# on a Cloud Run service's Direct VPC egress only matches egress rules,
# so an ingress rule with it as a source tag lets nothing through. That
# is why the roles egress from a subnet of their own.

locals {
  # Infra machines, project workers and frontends.
  subnet_range = "10.10.0.0/20"
  # The roles and the holders alone (Direct VPC egress wants a /26 or
  # larger).
  roles_subnet_range = "10.10.16.0/24"
  # The network tag the infra rule targets (a VM's tag works as a target;
  # see above for why no rule uses a tag as a source).
  infra_tag = "${var.name}-infra"
}

resource "google_compute_network" "vpc" {
  name                    = "${var.name}-vpc"
  auto_create_subnetworks = false
  depends_on              = [google_project_service.apis]
}

resource "google_compute_subnetwork" "main" {
  name                     = "${var.name}-subnet"
  network                  = google_compute_network.vpc.id
  ip_cidr_range            = local.subnet_range
  private_ip_google_access = true
}

resource "google_compute_subnetwork" "roles" {
  name                     = "${var.name}-roles-subnet"
  network                  = google_compute_network.vpc.id
  ip_cidr_range            = local.roles_subnet_range
  private_ip_google_access = true
}

resource "google_compute_router" "router" {
  name    = "${var.name}-router"
  network = google_compute_network.vpc.id
}

resource "google_compute_router_nat" "nat" {
  name                               = "${var.name}-nat"
  router                             = google_compute_router.router.name
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"

  log_config {
    enable = true
    filter = "ERRORS_ONLY"
  }
}

# A project's infra, on whatever ports its units declare (and its unit
# agent's), for everything of the install that talks to it: the workers,
# the frontends, the roles (the supervisor running a unit, the dispatcher
# reading a display), the holders (a connection held to a unit) and other
# infra.
resource "google_compute_firewall" "infra" {
  name          = "${var.name}-infra"
  network       = google_compute_network.vpc.id
  source_ranges = [local.subnet_range, local.roles_subnet_range]
  target_tags   = [local.infra_tag]
  allow {
    protocol = "tcp"
  }
  allow {
    protocol = "udp"
  }
}

# SSH to an infra machine, only through Identity-Aware Proxy (`gcloud
# compute ssh --tunnel-through-iap`).
resource "google_compute_firewall" "iap_ssh" {
  name          = "${var.name}-iap-ssh"
  network       = google_compute_network.vpc.id
  source_ranges = ["35.235.240.0/20"]
  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}
