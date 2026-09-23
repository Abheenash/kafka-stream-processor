variable "region" {
  type    = string
  default = "us-east-1"
}

variable "name_prefix" {
  type    = string
  default = "ksp"
}

variable "vpc_id" {
  description = "Existing VPC. MSK is not internet-reachable and has no public option."
  type        = string
}

variable "subnet_ids" {
  description = "At least two private subnets in different AZs — MSK requires it."
  type        = list(string)

  validation {
    condition     = length(var.subnet_ids) >= 2
    error_message = "MSK Serverless requires subnets in at least two availability zones."
  }
}
