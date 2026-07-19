# acm.tf — TLS certificate for the backend API domain, served on the ALB :443.
# ----------------------------------------------------------------------------
# DNS-validated (works regardless of where DNS is hosted — Cloudflare / Porkbun /
# Route53). The validation CNAME is emitted as the `acm_api_validation` output;
# the operator adds it at the domain's DNS host. aws_acm_certificate_validation
# then WAITS until AWS observes the record.
#
# APPLY ORDER (because the validation step blocks on the manual DNS record):
#   1. terraform apply -target=aws_acm_certificate.api        # create cert (instant)
#   2. terraform output acm_api_validation                    # get the CNAME
#   3. add that CNAME at the DNS host
#   4. terraform apply                                        # validation completes
#                                                              # + :443 listener comes up
#
# Set var.api_domain in terraform.tfvars (e.g. "api.kanida.ai"). Empty = skip
# (no cert, no HTTPS listener — the pre-HTTPS state).

variable "api_domain" {
  description = "Backend API hostname served by the ALB over HTTPS (e.g. api.kanida.ai). Empty string disables the cert + the :443 listener."
  type        = string
  default     = ""
}

resource "aws_acm_certificate" "api" {
  count             = var.api_domain == "" ? 0 : 1
  domain_name       = var.api_domain
  validation_method = "DNS"

  lifecycle {
    create_before_destroy = true
  }

  tags = { Name = "${var.name_prefix}-${var.environment}-api-cert" }
}

resource "aws_acm_certificate_validation" "api" {
  count                   = var.api_domain == "" ? 0 : 1
  certificate_arn         = aws_acm_certificate.api[0].arn
  validation_record_fqdns = [for o in aws_acm_certificate.api[0].domain_validation_options : o.resource_record_name]

  timeouts {
    create = "60m"
  }
}

# The CNAME to add at the DNS host to validate the certificate.
output "acm_api_validation" {
  description = "Add this CNAME record at your DNS host (Cloudflare/Porkbun) to validate the API cert."
  value = var.api_domain == "" ? {} : {
    for o in aws_acm_certificate.api[0].domain_validation_options :
    o.domain_name => {
      record_name  = o.resource_record_name
      record_type  = o.resource_record_type
      record_value = o.resource_record_value
    }
  }
}

output "api_certificate_arn" {
  description = "Validated ACM certificate ARN wired into the ALB :443 listener."
  value       = var.api_domain == "" ? null : aws_acm_certificate_validation.api[0].certificate_arn
}
