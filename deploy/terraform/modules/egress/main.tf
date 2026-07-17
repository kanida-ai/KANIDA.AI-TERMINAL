# ============================================================================
# modules/egress — ONE static Elastic IP + tinyproxy box PER USER
# ----------------------------------------------------------------------------
# THE IP-AT-SCALE SOLUTION. SEBI (enforced by Zerodha/Kite and every broker app)
# binds ONE static IP to ONE broker account. A multi-tenant backend therefore
# needs a DISTINCT egress IP per user's broker account.
#
# The APP SIDE already exists and is broker-agnostic:
#   backend/autotrade/broker/egress.py assigns each broker_account a proxy URL
#   from BROKER_EGRESS_POOL (Fernet-encrypted at rest, fail-open to DIRECT).
# THIS MODULE is the INFRA side of that pool: for each user handle it creates
#   * an aws_eip           — the stable, SEBI-registerable IP, and
#   * a t4g.nano tinyproxy — egresses the user's orders from that EIP.
# The module output (egress_ips_by_user) is the list the operator assembles into
# BROKER_EGRESS_POOL and hands to each user to register on their broker profile.
#
# ADDING A USER = ONE LIST ENTRY in var.egress_users. for_each does the rest:
# one more EIP + one more nano box, no code change, no restart of anything.
#
# Cost note: t4g.nano + 1 EIP per user is the cheapest per-user static-IP unit.
# For denser packing, multiple users could share a box with multiple ENIs/EIPs;
# kept 1:1 here for blast-radius isolation (one user's box down != all users).
# ============================================================================

# Latest Amazon Linux 2023 ARM64 AMI (t4g = Graviton/ARM).
data "aws_ami" "al2023_arm" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-arm64"]
  }
  filter {
    name   = "architecture"
    values = ["arm64"]
  }
}

# One tinyproxy box per user handle. Placed round-robin across the public subnets.
resource "aws_instance" "proxy" {
  for_each = toset(var.egress_users)

  ami                    = data.aws_ami.al2023_arm.id
  instance_type          = var.proxy_instance_type
  subnet_id              = var.public_subnet_ids[index(var.egress_users, each.key) % length(var.public_subnet_ids)]
  vpc_security_group_ids = [var.proxy_sg_id]
  key_name               = var.ssh_key_name

  # Minimal tinyproxy bring-up. The proxy listens on 8888 for the app (VPC-only
  # via the SG) and egresses to the broker from this box's Elastic IP. Harden
  # (Basic auth, allowlist the app subnet) before real use — see README.
  user_data = <<-EOF
    #!/bin/bash
    set -e
    dnf install -y tinyproxy
    sed -i 's/^Port .*/Port 8888/' /etc/tinyproxy/tinyproxy.conf
    # Allow only the VPC (the app tier reaches us over the private network).
    echo "Allow ${var.vpc_cidr}" >> /etc/tinyproxy/tinyproxy.conf
    systemctl enable --now tinyproxy
  EOF

  tags = {
    Name       = "${var.name}-egress-${each.key}"
    EgressUser = each.key
  }
}

# One Elastic IP per user — the SEBI-registerable static IP.
resource "aws_eip" "proxy" {
  for_each = toset(var.egress_users)

  domain   = "vpc"
  instance = aws_instance.proxy[each.key].id
  tags = {
    Name       = "${var.name}-egress-eip-${each.key}"
    EgressUser = each.key
  }
}
