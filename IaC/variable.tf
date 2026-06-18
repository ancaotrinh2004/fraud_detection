variable "project_id" {
  description = "ID của Google Cloud Project"
  type        = string
  default     = "aide1-488405"
}

variable "region" {
  description = "Region cho Network (giữ nguyên)"
  type        = string
  default     = "asia-southeast1"
}

# THÊM BIẾN NÀY
variable "zone" {
  description = "Zone cụ thể để đặt máy chủ (tránh zone A đang lỗi)"
  type        = string
  default     = "asia-southeast1-b" 
}

variable "cluster_name" {
  description = "Tên của GKE Cluster"
  type        = string
  default     = "aide1-kserve-cluster"
}

variable "project_number"{
  description = "project number"
  type = string
  default = "1098114675590"
}