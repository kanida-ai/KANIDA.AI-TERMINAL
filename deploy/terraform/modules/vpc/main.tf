# ============================================================================
# modules/vpc — VPC, public/private subnets, IGW, NAT
# WHY: RDS Multi-AZ + resilient compute need >=2 AZs. Public subnets host the
# ALB, NAT, and per-user egress proxies (they need routable IPs). Private
# subnets host Fargate tasks, RDS, and Redis so the data tier is never directly
# internet-reachable. NAT gives the private tier outbound (ECR pulls, broker
# APIs when NOT using a per-user egress proxy). Addresses the "everything on one
# laptop with one IP" SPOF and the flat-network exposure risk.
# ============================================================================

resource "aws_vpc" "this" {
  cidr_block           = var.cidr_block
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags                 = { Name = var.name }
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id
  tags   = { Name = "${var.name}-igw" }
}

# ── Public subnets (ALB / NAT / egress proxies) ─────────────────────────────
resource "aws_subnet" "public" {
  count                   = length(var.public_subnets)
  vpc_id                  = aws_vpc.this.id
  cidr_block              = var.public_subnets[count.index]
  availability_zone       = var.availability_zones[count.index]
  map_public_ip_on_launch = true
  tags                    = { Name = "${var.name}-public-${count.index}", Tier = "public" }
}

# ── Private subnets (Fargate / RDS / Redis) ─────────────────────────────────
resource "aws_subnet" "private" {
  count             = length(var.private_subnets)
  vpc_id            = aws_vpc.this.id
  cidr_block        = var.private_subnets[count.index]
  availability_zone = var.availability_zones[count.index]
  tags              = { Name = "${var.name}-private-${count.index}", Tier = "private" }
}

# ── NAT (single NAT for cost; note the SPOF trade-off) ──────────────────────
# One NAT keeps Phase-0 cost down. It is a per-AZ SPOF for private-tier egress;
# for HA, provision one NAT per AZ (documented trade-off, not done here).
resource "aws_eip" "nat" {
  domain = "vpc"
  tags   = { Name = "${var.name}-nat-eip" }
}

resource "aws_nat_gateway" "this" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public[0].id
  tags          = { Name = "${var.name}-nat" }
  depends_on    = [aws_internet_gateway.this]
}

# ── Route tables ────────────────────────────────────────────────────────────
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this.id
  }
  tags = { Name = "${var.name}-public-rt" }
}

resource "aws_route_table_association" "public" {
  count          = length(aws_subnet.public)
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.this.id
  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.this.id
  }
  tags = { Name = "${var.name}-private-rt" }
}

resource "aws_route_table_association" "private" {
  count          = length(aws_subnet.private)
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}
