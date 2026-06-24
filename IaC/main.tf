# Lấy thông tin project hiện tại để tự động lấy Project Number
data "google_project" "project" {
  project_id = var.project_id
}

# 1. Network Configuration
resource "google_compute_network" "vpc" {
  name                    = "${var.cluster_name}-vpc"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "subnet" {
  name          = "${var.cluster_name}-subnet"
  region        = var.region
  network       = google_compute_network.vpc.name
  ip_cidr_range = "10.0.0.0/16"
}

# 2. GKE Cluster
resource "google_container_cluster" "primary" {
  name     = var.cluster_name
  location = var.zone

  deletion_protection      = false
  remove_default_node_pool = true
  initial_node_count       = 1

  network    = google_compute_network.vpc.name
  subnetwork = google_compute_subnetwork.subnet.name
  
  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }

  addons_config {
    gce_persistent_disk_csi_driver_config {
      enabled = true
    }
  }
}

# 3. System Node Pool (Nâng cấp lên 2 Node cho Istio + MLflow + MinIO)
resource "google_container_node_pool" "system_nodes" {
  name       = "system-pool"
  location   = var.zone
  cluster    = google_container_cluster.primary.name
  node_count = 2 # Đã tăng để giải quyết lỗi Pending pods

  node_config {
    machine_type = "e2-standard-4" # 4 vCPU, 16GB RAM mỗi node
    
    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]

    labels = {
      role = "system"
    }
  }
}

# 4. Serving Node Pool (Cho PhoBERT - Hỗ trợ Scale-to-Zero)
resource "google_container_node_pool" "serving_nodes" {
  name       = "serving-pool"
  location   = var.zone
  cluster    = google_container_cluster.primary.name
  
  autoscaling {
    min_node_count = 0 
    max_node_count = 5 
  }

  node_config {
    machine_type = "e2-standard-4" 
    
    taint {
      key    = "dedicated"
      value  = "serving"
      effect = "NO_SCHEDULE"
    }

    labels = {
      role = "serving"
    }

    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]
  }
}

# 5. Cấp quyền Service Account (Sử dụng dữ liệu tự động)
resource "google_project_iam_member" "node_permissions" {
  project = var.project_id
  role    = "roles/container.defaultNodeServiceAccount"
  member  = "serviceAccount:${data.google_project.project.number}-compute@developer.gserviceaccount.com"
}

resource "google_project_iam_member" "storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${data.google_project.project.number}-compute@developer.gserviceaccount.com"
}