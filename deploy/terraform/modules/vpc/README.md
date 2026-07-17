# module: vpc

**Purpose.** Network foundation: a `/16` VPC with public + private subnets across
2 AZs, an internet gateway, a NAT gateway, and route tables.

**SPOF / requirement addressed.**
- The whole system today runs on one laptop with one IP and no network
  segmentation. This gives a multi-AZ, tiered network so the data plane (RDS,
  Redis, Fargate) is never directly internet-exposed.
- 2 AZs are the precondition for RDS Multi-AZ (removes the single-DB SPOF).

**Phase-0 status.** Authored, unverified — never `terraform plan`ned.

**Trade-offs flagged.**
- **Single NAT gateway** (cost). It is a per-AZ egress SPOF; for HA use one NAT
  per AZ. Not done here to keep Phase-0 spend minimal.
- Subnet lists are index-aligned with `availability_zones`; keep their lengths
  equal.

**Inputs:** `name`, `cidr_block`, `availability_zones`, `public_subnets`,
`private_subnets`.
**Outputs:** `vpc_id`, `public_subnet_ids`, `private_subnet_ids`.
