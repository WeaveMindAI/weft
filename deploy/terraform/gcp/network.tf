# One VPC. The machine has a static public address (the install's own,
# and what every domain points at) and a fixed private one, which Cloud
# Run services (Direct VPC egress) and infra machines reach its internal
# port at. Infra machines have no public address; they reach the internet
# through NAT.
#
# The firewall tells callers apart by subnet, never by network tag: a tag
# on a Cloud Run service's Direct VPC egress only matches egress rules,
# so an ingress rule with it as a source tag lets nothing through. That
# is why the serverless roles, the only callers Postgres admits besides
# the machine itself, egress from a subnet of their own.

locals {
  # The machine, the listener's machine, infra machines, project workers
  # and frontends.
  subnet_range = "10.10.0.0/20"
  # The serverless roles alone (Direct VPC egress wants a /26 or larger).
  roles_subnet_range = "10.10.16.0/24"
  # SYNC: these ports <-> crates/weft-core/src/ports.rs (PUBLIC, INTERNAL,
  # UNIT_AGENT), setup.sh,
  # extension-vscode/src/localInstall.ts,
  # extension-browser/src/entrypoints/popup/App.svelte
  public_port   = 14111
  internal_port = 14113
  agent_port    = 14116
  # The network tags ingress rules target (a VM's tag works as a target;
  # see above for why no rule uses a tag as a source).
  machine_tag = "${var.name}-machine"
  infra_tag   = "${var.name}-infra"
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

# The install's address. It must never change while the install has no
# domain: every link it minted and every webhook it registered embeds it.
resource "google_compute_address" "public" {
  name = "${var.name}-public"
}

resource "google_compute_address" "machine_private" {
  name         = "${var.name}-machine-private"
  address_type = "INTERNAL"
  subnetwork   = google_compute_subnetwork.main.id
  address      = cidrhost(local.subnet_range, 2)
}

# The front door, from anywhere.
resource "google_compute_firewall" "front_door" {
  name          = "${var.name}-front-door"
  network       = google_compute_network.vpc.id
  source_ranges = ["0.0.0.0/0"]
  target_tags   = [local.machine_tag]
  allow {
    protocol = "tcp"
    ports    = ["80", "443"]
  }
}

# The internal port and the unit agents, from inside the VPC only: the
# roles, and on the main subnet the workers and the frontends (which
# reach the dispatcher's API at the machine's private address, port
# 14113). Every internal route still checks its caller's identity.
resource "google_compute_firewall" "internal" {
  name          = "${var.name}-internal"
  network       = google_compute_network.vpc.id
  source_ranges = [local.subnet_range, local.roles_subnet_range]
  allow {
    protocol = "tcp"
    ports    = [tostring(local.internal_port), tostring(local.agent_port)]
  }
}

# A project's infra, on whatever ports its units declare, for everything
# of the install that talks to it: the workers, the roles (the listener
# watching a unit, the dispatcher reading a display) and other infra.
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

# SSH only through Identity-Aware Proxy (`gcloud compute ssh --tunnel-through-iap`).
resource "google_compute_firewall" "iap_ssh" {
  name          = "${var.name}-iap-ssh"
  network       = google_compute_network.vpc.id
  source_ranges = ["35.235.240.0/20"]
  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}
