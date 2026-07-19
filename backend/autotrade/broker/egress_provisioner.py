"""ON-DEMAND per-user egress-IP provisioning (boto3, runtime, app-triggered).

WHY: SEBI (enforced by Zerodha/Kite and every broker app) binds ONE static IP to
ONE broker account. A multi-tenant cloud backend therefore needs a DEDICATED
static egress IP per power user's broker account, so THAT user's broker traffic
(data + orders) egresses from the single IP they registered with their broker.

The DATA side of this already exists and is broker-agnostic — `egress.py` stores
each account's proxy URL (Fernet-encrypted) in `broker_accounts.egress_proxy_url_enc`
and the order path resolves it per account. That module gives out URLs from a
STATIC pool the operator hand-created (BROKER_EGRESS_POOL). THIS module is the
automation that CREATES the infrastructure on demand — it mirrors the terraform
`modules/egress` shape (one Elastic IP + one tinyproxy box per user) but at
RUNTIME via boto3, then calls the EXISTING `egress.set_account_proxy(...)`. It
never touches the order-execution path or `resolve_account_proxy`'s read logic.

SAFETY (real-money-adjacent — this is billing + connectivity infra, not trading):
  * Provisioning an IP PLACES NO TRADE. It is fully decoupled from execution.
  * FAIL-SAFE cleanup: a partial failure (EIP allocated, instance launch failed,
    associate failed, …) is torn down (instance terminated, EIP released) BEFORE
    the error is raised — we never leak an orphaned billable resource.
  * The AWS EIP quota (default 5/region) surfaces as a CLEAR typed error
    (`EgressQuotaExceeded`) telling the operator to request a quota increase.
  * The proxy URL carries a password. It is generated per proxy, handed ONLY to
    `set_account_proxy` (encrypted at rest) and is NEVER logged and NEVER put in
    the return value or any API response. Callers get the bare public IP (to
    register with the broker), the instance id, and the EIP allocation id.
  * BROKER-AGNOSTIC: nothing here inspects the broker. Keyed purely on
    broker_account_id (also the AWS tag value), exactly like egress.py.

NO AWS CREDENTIALS in this environment → boto3 is imported lazily and every path
is exercised under mocked boto3 in the tests. The controlled real-AWS validation
(provision → curl a broker API through the proxy → confirm egress IP == the EIP →
deprovision) is an operator follow-up documented in the task hand-off.
"""
from __future__ import annotations

import base64
import logging
import os
import secrets
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from . import egress

log = logging.getLogger("kanida.autotrade.egress.provision")

# ── Tags (every created resource) — so deprovision/status can find them and the
#    operator can bill/audit by project. `kanida:egress-user` is the join key. ──
TAG_PROJECT = "kanida-ai"
TAG_ENV = "prod"
TAG_USER_KEY = "kanida:egress-user"

DEFAULT_PROXY_PORT = 8888
DEFAULT_INSTANCE_TYPE = "t3.nano"
_PROXY_BASIC_USER = "kanida"


# ── Typed errors (endpoints map these to clean JSON, never a 500 stack trace) ─

class EgressProvisioningError(RuntimeError):
    """Base for any provisioning failure. Message is operator-safe (no secrets)."""


class EgressConfigError(EgressProvisioningError):
    """Required config (region/subnet/sg/ami) missing or boto3 not installed."""


class EgressQuotaExceeded(EgressProvisioningError):
    """AWS Elastic-IP quota hit (default 5/region). The operator must request an
    EIP quota increase for the region before more users can be provisioned."""


# ── Config (env-driven, sensible defaults) ───────────────────────────────────

@dataclass(frozen=True)
class EgressProvisionConfig:
    region: str
    subnet_id: str
    sg_id: str
    ami_id: str
    instance_type: str = DEFAULT_INSTANCE_TYPE
    key_name: Optional[str] = None
    proxy_port: int = DEFAULT_PROXY_PORT


def load_config_from_env() -> EgressProvisionConfig:
    """Resolve provisioning config from env. Raises EgressConfigError listing the
    MISSING keys (so the endpoint returns a clear 400, never a stack trace).

    Env:
      AWS_DEFAULT_REGION          — region for the EIP + instance (required)
      KANIDA_EGRESS_SUBNET_ID     — a PUBLIC subnet id (required)
      KANIDA_EGRESS_SG_ID         — the egress_proxy security group id (required)
      KANIDA_EGRESS_AMI_ID        — AMI for the tinyproxy box (required; keeps the
                                    task role's EC2 perms minimal — no DescribeImages/SSM)
      KANIDA_EGRESS_INSTANCE_TYPE — default t3.nano
      KANIDA_EGRESS_KEY_NAME      — optional SSH key (omit for no key)
      KANIDA_EGRESS_PROXY_PORT    — default 8888
    """
    try:  # pick up config/.env exactly like egress.pool_urls() does
        import services.kite_auth as kite_auth
        kite_auth._load_env_file()
    except Exception:
        pass

    region = os.environ.get("AWS_DEFAULT_REGION", "").strip()
    subnet_id = os.environ.get("KANIDA_EGRESS_SUBNET_ID", "").strip()
    sg_id = os.environ.get("KANIDA_EGRESS_SG_ID", "").strip()
    ami_id = os.environ.get("KANIDA_EGRESS_AMI_ID", "").strip()
    instance_type = os.environ.get(
        "KANIDA_EGRESS_INSTANCE_TYPE", DEFAULT_INSTANCE_TYPE).strip() or DEFAULT_INSTANCE_TYPE
    key_name = os.environ.get("KANIDA_EGRESS_KEY_NAME", "").strip() or None
    try:
        proxy_port = int(os.environ.get("KANIDA_EGRESS_PROXY_PORT", DEFAULT_PROXY_PORT))
    except ValueError:
        proxy_port = DEFAULT_PROXY_PORT

    missing = [n for n, v in (
        ("AWS_DEFAULT_REGION", region),
        ("KANIDA_EGRESS_SUBNET_ID", subnet_id),
        ("KANIDA_EGRESS_SG_ID", sg_id),
        ("KANIDA_EGRESS_AMI_ID", ami_id),
    ) if not v]
    if missing:
        raise EgressConfigError(
            "on-demand egress provisioning is not configured on this server "
            "(missing: " + ", ".join(missing) + ")")
    return EgressProvisionConfig(
        region=region, subnet_id=subnet_id, sg_id=sg_id, ami_id=ami_id,
        instance_type=instance_type, key_name=key_name, proxy_port=proxy_port)


# ── boto3 plumbing (lazy — the image may not have boto3; tests mock it) ───────

def _ec2_client(region: str):
    """Build an EC2 client. Raises EgressConfigError (clean 400) if boto3 is not
    installed rather than a raw ImportError 500."""
    try:
        import boto3  # lazy: absent on the laptop image, present on the ECS task
    except Exception as e:  # pragma: no cover - trivial guard
        raise EgressConfigError(
            "boto3 is not installed on this server — cannot provision egress") from e
    return boto3.client("ec2", region_name=region)


def _is_quota_error(err: Exception) -> bool:
    """True for the AWS Elastic-IP quota / address-limit error, however it surfaces
    (botocore ClientError code or plain message text)."""
    aws_code = ""
    try:
        aws_code = (getattr(err, "response", {}) or {}).get("Error", {}).get("Code", "")
    except Exception:
        aws_code = ""
    text = f"{aws_code} {err}"
    return "AddressLimitExceeded" in text or "AddressLimit" in text


def _tag_specs(resource_type: str, broker_account_id: str) -> Dict[str, Any]:
    return {
        "ResourceType": resource_type,
        "Tags": [
            {"Key": "Project", "Value": TAG_PROJECT},
            {"Key": "Env", "Value": TAG_ENV},
            {"Key": TAG_USER_KEY, "Value": broker_account_id},
            {"Key": "Name", "Value": f"kanida-egress-{broker_account_id}"},
        ],
    }


def _tinyproxy_user_data(port: int, basic_user: str, basic_pass: str) -> str:
    """Cloud-init that installs tinyproxy with BASIC AUTH on `port`, config 0600.

    The password lives ONLY in user-data (SG-restricted, task-role-readable) — it
    is never emitted to our logs. IP allow-lines are removed so access is gated by
    BasicAuth + the egress_proxy security group (8888 from the app tier only)."""
    return f"""#!/bin/bash
set -euo pipefail
dnf install -y tinyproxy || yum install -y tinyproxy
conf=/etc/tinyproxy/tinyproxy.conf
sed -i 's/^Port .*/Port {port}/' "$conf"
# Remove default IP allow-lines: gate on BasicAuth + the security group instead.
sed -i '/^Allow /d' "$conf"
echo "BasicAuth {basic_user} {basic_pass}" >> "$conf"
chmod 600 "$conf"
systemctl enable --now tinyproxy
"""


# ── Resource discovery (by the egress-user tag) ──────────────────────────────

def _find_instances(ec2, broker_account_id: str, include_terminated: bool = False) -> List[Dict[str, Any]]:
    filters = [{"Name": f"tag:{TAG_USER_KEY}", "Values": [broker_account_id]}]
    if not include_terminated:
        filters.append({"Name": "instance-state-name",
                        "Values": ["pending", "running", "stopping", "stopped"]})
    resp = ec2.describe_instances(Filters=filters)
    out: List[Dict[str, Any]] = []
    for res in resp.get("Reservations", []):
        out.extend(res.get("Instances", []))
    return out


def _find_addresses(ec2, broker_account_id: str) -> List[Dict[str, Any]]:
    resp = ec2.describe_addresses(
        Filters=[{"Name": f"tag:{TAG_USER_KEY}", "Values": [broker_account_id]}])
    return resp.get("Addresses", [])


# ── Best-effort teardown helpers (used by cleanup + deprovision) ─────────────

def _terminate_instance(ec2, instance_id: Optional[str]) -> None:
    if not instance_id:
        return
    try:
        ec2.terminate_instances(InstanceIds=[instance_id])
        log.info("egress-provision: terminated instance=%s", instance_id)
    except Exception as e:
        log.warning("egress-provision: terminate instance=%s failed (%s)",
                    instance_id, e)


def _release_eip(ec2, allocation_id: Optional[str]) -> None:
    if not allocation_id:
        return
    try:
        ec2.release_address(AllocationId=allocation_id)
        log.info("egress-provision: released EIP alloc=%s", allocation_id)
    except Exception as e:
        log.warning("egress-provision: release EIP alloc=%s failed (%s)",
                    allocation_id, e)


# ── PUBLIC: provision / deprovision / status ─────────────────────────────────

def provision_user_egress(broker_account_id: str, *, region: str, subnet_id: str,
                          sg_id: str, ami_id: str,
                          instance_type: str = DEFAULT_INSTANCE_TYPE,
                          key_name: Optional[str] = None,
                          proxy_port: int = DEFAULT_PROXY_PORT) -> Dict[str, Any]:
    """Allocate a dedicated static egress IP + tinyproxy box for one broker account.

    Steps (fail-safe): allocate EIP → launch a tinyproxy EC2 in the PUBLIC subnet
    with the egress_proxy SG + BasicAuth user-data → tag everything → wait until
    running → associate the EIP → build the proxy URL → `set_account_proxy(...)`.

    Returns {"public_ip", "instance_id", "allocation_id", "provisioned": True}.
    The credential-bearing proxy URL is DELIBERATELY NOT returned or logged — the
    caller gets only the public IP (to register with the broker) and the AWS ids.

    Raises EgressQuotaExceeded (AWS EIP limit) / EgressProvisioningError (any other
    failure, after the partial resources are cleaned up).
    """
    if not broker_account_id:
        raise EgressProvisioningError("broker_account_id required")

    ec2 = _ec2_client(region)
    allocation_id: Optional[str] = None
    public_ip: Optional[str] = None
    instance_id: Optional[str] = None
    private_ip: Optional[str] = None

    # 1) Elastic IP — the SEBI-registerable static IP. Quota error is TYPED.
    try:
        alloc = ec2.allocate_address(
            Domain="vpc", TagSpecifications=[_tag_specs("elastic-ip", broker_account_id)])
        allocation_id = alloc.get("AllocationId")
        public_ip = alloc.get("PublicIp")
    except Exception as e:
        if _is_quota_error(e):
            raise EgressQuotaExceeded(
                "AWS Elastic-IP quota reached for region " + region + " (default 5). "
                "Request an EIP quota increase (Service Quotas: 'EC2-VPC Elastic IPs') "
                "before provisioning more users.") from e
        raise EgressProvisioningError(f"could not allocate Elastic IP: {e}") from e

    # 2) tinyproxy box + associate the EIP. ANY failure past this point tears the
    #    partial resources down before re-raising — never leak billable infra.
    try:
        basic_pass = secrets.token_urlsafe(24)  # URL-safe: no ':' '@' '/' to break the URL
        user_data = _tinyproxy_user_data(proxy_port, _PROXY_BASIC_USER, basic_pass)
        run_kwargs: Dict[str, Any] = dict(
            ImageId=ami_id,
            InstanceType=instance_type,
            MinCount=1,
            MaxCount=1,
            SubnetId=subnet_id,
            SecurityGroupIds=[sg_id],
            UserData=user_data,  # boto3 base64-encodes this for us
            TagSpecifications=[
                _tag_specs("instance", broker_account_id),
                _tag_specs("volume", broker_account_id),
            ],
        )
        if key_name:
            run_kwargs["KeyName"] = key_name

        run = ec2.run_instances(**run_kwargs)
        instances = run.get("Instances", [])
        if not instances:
            raise EgressProvisioningError("RunInstances returned no instance")
        instance_id = instances[0].get("InstanceId")
        if not instance_id:
            raise EgressProvisioningError("RunInstances returned no InstanceId")
        # PRIVATE IP: the app reaches the proxy over the VPC (the egress-proxy SG
        # allows 8888 from the VPC CIDR only). Assigned at launch, present in the
        # RunInstances response.
        private_ip = instances[0].get("PrivateIpAddress")
        if not private_ip:
            raise EgressProvisioningError("RunInstances returned no PrivateIpAddress")

        # Wait until running (DescribeInstances-backed waiter → no extra IAM perm).
        try:
            waiter = ec2.get_waiter("instance_running")
            waiter.wait(InstanceIds=[instance_id])
        except Exception as e:
            raise EgressProvisioningError(
                f"instance {instance_id} did not reach running: {e}") from e

        # Associate the EIP → orders now egress from THIS static IP.
        ec2.associate_address(AllocationId=allocation_id, InstanceId=instance_id)
    except EgressProvisioningError:
        _cleanup_partial(ec2, instance_id, allocation_id)
        raise
    except Exception as e:
        _cleanup_partial(ec2, instance_id, allocation_id)
        raise EgressProvisioningError(f"egress provisioning failed: {e}") from e

    # 3) Persist the proxy URL (encrypted) via the EXISTING egress layer. If this
    #    fails we must NOT leave orphaned infra — tear down then raise.
    # proxy_url targets the PRIVATE IP (app→proxy hop, over the VPC). public_ip is
    # the EIP — the SEBI-registerable egress IP the USER whitelists with their
    # broker — NOT the address the app connects to.
    proxy_url = f"http://{_PROXY_BASIC_USER}:{basic_pass}@{private_ip}:{proxy_port}"
    try:
        egress.set_account_proxy(broker_account_id, proxy_url)
    except Exception as e:
        _cleanup_partial(ec2, instance_id, allocation_id)
        # egress.set_account_proxy never logs the URL; keep it out of ours too.
        raise EgressProvisioningError(
            f"provisioned infra but could not store the egress mapping: {e}") from e

    log.info("egress-provision: account=%s public_ip=%s instance=%s alloc=%s "
             "(proxy stored, creds withheld)", broker_account_id, public_ip,
             instance_id, allocation_id)
    return {
        "public_ip": public_ip,
        "instance_id": instance_id,
        "allocation_id": allocation_id,
        "provisioned": True,
    }


def _cleanup_partial(ec2, instance_id: Optional[str], allocation_id: Optional[str]) -> None:
    """Tear down a partially-provisioned egress: terminate the instance (if any)
    and release the EIP (if any). Best-effort + idempotent — each step is guarded
    so one failure still attempts the other."""
    log.warning("egress-provision: cleaning up partial provision "
                "(instance=%s alloc=%s)", instance_id, allocation_id)
    _terminate_instance(ec2, instance_id)
    _release_eip(ec2, allocation_id)


def deprovision_user_egress(broker_account_id: str, *,
                            region: Optional[str] = None) -> Dict[str, Any]:
    """Tear down a broker account's dedicated egress: terminate its tinyproxy box,
    release its Elastic IP, and clear the account's egress mapping (revert to
    DIRECT on the next order — no restart).

    IDEMPOTENT + SAFE if already gone: missing instance/EIP is fine; the egress
    mapping is always cleared. Returns a summary of what was torn down."""
    if not broker_account_id:
        raise EgressProvisioningError("broker_account_id required")
    region = region or os.environ.get("AWS_DEFAULT_REGION", "").strip()
    if not region:
        raise EgressConfigError("AWS_DEFAULT_REGION not set — cannot deprovision")

    ec2 = _ec2_client(region)
    terminated: List[str] = []
    released: List[str] = []

    try:
        for inst in _find_instances(ec2, broker_account_id):
            iid = inst.get("InstanceId")
            if iid:
                _terminate_instance(ec2, iid)
                terminated.append(iid)
        for addr in _find_addresses(ec2, broker_account_id):
            assoc = addr.get("AssociationId")
            if assoc:
                try:
                    ec2.disassociate_address(AssociationId=assoc)
                except Exception as e:
                    log.warning("egress-provision: disassociate %s failed (%s)", assoc, e)
            alloc = addr.get("AllocationId")
            if alloc:
                _release_eip(ec2, alloc)
                released.append(alloc)
    except Exception as e:
        # Even if AWS discovery/teardown partially fails, ALWAYS clear the mapping
        # so we never keep routing orders through infra we tried to destroy.
        log.warning("egress-provision: deprovision AWS teardown error for "
                    "account=%s (%s) — clearing mapping anyway", broker_account_id, e)

    try:
        egress.clear_account_proxy(broker_account_id)
    except Exception as e:
        log.warning("egress-provision: clear_account_proxy failed for account=%s (%s)",
                    broker_account_id, e)

    log.info("egress-provision: deprovisioned account=%s terminated=%s released=%s",
             broker_account_id, terminated, released)
    return {
        "deprovisioned": True,
        "terminated_instances": terminated,
        "released_allocations": released,
    }


def get_user_egress_status(broker_account_id: str, *,
                           region: Optional[str] = None) -> Dict[str, Any]:
    """Live AWS view of an account's dedicated egress box.

    Returns {"provisioned": bool, "public_ip": str|None, "instance_state": str|None}.
    Never raises on a missing box (provisioned=False). Never returns credentials."""
    if not broker_account_id:
        return {"provisioned": False, "public_ip": None, "instance_state": None}
    region = region or os.environ.get("AWS_DEFAULT_REGION", "").strip()
    if not region:
        raise EgressConfigError("AWS_DEFAULT_REGION not set — cannot query egress")

    ec2 = _ec2_client(region)
    instances = _find_instances(ec2, broker_account_id)
    addresses = _find_addresses(ec2, broker_account_id)
    public_ip = None
    for addr in addresses:
        if addr.get("PublicIp"):
            public_ip = addr.get("PublicIp")
            break
    instance_state = None
    if instances:
        state = instances[0].get("State") or {}
        instance_state = state.get("Name")
        if public_ip is None:
            public_ip = instances[0].get("PublicIpAddress")
    return {
        "provisioned": bool(instances) and instance_state in ("pending", "running"),
        "public_ip": public_ip,
        "instance_state": instance_state,
    }
