"""ON-DEMAND per-user egress-IP provisioning (boto3 → EIP + tinyproxy box).

WHY THIS EXISTS: SEBI binds ONE static IP to ONE broker account, so a cloud
backend must allocate a DEDICATED static egress IP per user's broker account and
route that user's broker traffic through it. This suite pins the automation
(egress_provisioner.py) that CREATES that infra on demand and then hands the
mapping to the existing (already-tested) egress layer.

boto3/moto are NOT installed in this environment, so EC2 is a hand-rolled fake
that records calls and lets each test drive a failure. The invariants pinned:

  * happy path: EIP allocated + instance launched + EIP associated +
    set_account_proxy called with a WELL-FORMED url; the return carries the
    public IP but NEVER the proxy password; the password never hits the logs;
  * partial-failure CLEANUP: instance launch (or associate, or store) failing
    releases the EIP + terminates the instance — no orphaned billable resource,
    and NO egress mapping is stored;
  * the AWS EIP quota surfaces as the typed EgressQuotaExceeded (no instance launched);
  * deprovision is idempotent (safe when already gone) and ALWAYS clears the mapping;
  * status reflects the live box without ever returning credentials.
"""
import logging

import pytest

from autotrade.broker import egress_provisioner as prov


# ── A fake botocore ClientError (moto/botocore absent) ───────────────────────
class _FakeClientError(Exception):
    def __init__(self, code, msg="fake"):
        super().__init__(msg)
        self.response = {"Error": {"Code": code, "Message": msg}}


# ── A configurable fake EC2 client ───────────────────────────────────────────
class FakeEc2:
    def __init__(self, *, alloc_error=None, run_error=None, wait_error=None,
                 assoc_error=None, instances=None, addresses=None):
        self.alloc_error = alloc_error
        self.run_error = run_error
        self.wait_error = wait_error
        self.assoc_error = assoc_error
        self._instances = instances if instances is not None else []
        self._addresses = addresses if addresses is not None else []
        self.calls = []  # list of (name, kwargs)

    def _rec(self, name, **kw):
        self.calls.append((name, kw))

    def names(self):
        return [n for n, _ in self.calls]

    # -- allocate / release / associate / disassociate --
    def allocate_address(self, **kw):
        self._rec("allocate_address", **kw)
        if self.alloc_error:
            raise self.alloc_error
        return {"AllocationId": "eipalloc-abc", "PublicIp": "1.2.3.4"}

    def release_address(self, **kw):
        self._rec("release_address", **kw)
        return {}

    def associate_address(self, **kw):
        self._rec("associate_address", **kw)
        if self.assoc_error:
            raise self.assoc_error
        return {"AssociationId": "eipassoc-1"}

    def disassociate_address(self, **kw):
        self._rec("disassociate_address", **kw)
        return {}

    # -- instances --
    def run_instances(self, **kw):
        self._rec("run_instances", **kw)
        if self.run_error:
            raise self.run_error
        return {"Instances": [{"InstanceId": "i-abc123",
                               "PrivateIpAddress": "10.20.10.50"}]}

    def terminate_instances(self, **kw):
        self._rec("terminate_instances", **kw)
        return {}

    def get_waiter(self, name):
        outer = self

        class _W:
            def wait(self, **kw):
                outer._rec("waiter_wait", waiter=name, **kw)
                if outer.wait_error:
                    raise outer.wait_error
        return _W()

    # -- describe (deprovision / status) --
    def describe_instances(self, **kw):
        self._rec("describe_instances", **kw)
        return {"Reservations": [{"Instances": self._instances}]} if self._instances \
            else {"Reservations": []}

    def describe_addresses(self, **kw):
        self._rec("describe_addresses", **kw)
        return {"Addresses": self._addresses}


@pytest.fixture
def patch_egress(monkeypatch):
    """Stub the existing egress store so no vault/DB is needed. Returns the two
    mocks so tests can assert set/clear were (or were not) called."""
    from unittest.mock import MagicMock
    setp = MagicMock(return_value=True)
    clearp = MagicMock(return_value=True)
    monkeypatch.setattr(prov.egress, "set_account_proxy", setp)
    monkeypatch.setattr(prov.egress, "clear_account_proxy", clearp)
    return setp, clearp


def _patch_client(monkeypatch, fake):
    monkeypatch.setattr(prov, "_ec2_client", lambda region: fake)


_KW = dict(region="ap-south-1", subnet_id="subnet-1", sg_id="sg-1",
           ami_id="ami-1", instance_type="t3.nano")


# ═══════════════════════════════════════════════════════════════════════════
# provision — happy path
# ═══════════════════════════════════════════════════════════════════════════

def test_provision_happy_path(monkeypatch, patch_egress, caplog):
    setp, _ = patch_egress
    fake = FakeEc2()
    _patch_client(monkeypatch, fake)
    caplog.set_level(logging.DEBUG)

    result = prov.provision_user_egress("acct-9", **_KW)

    # returns the PUBLIC IP the user whitelists + the AWS ids, provisioned flag
    assert result["public_ip"] == "1.2.3.4"
    assert result["instance_id"] == "i-abc123"
    assert result["allocation_id"] == "eipalloc-abc"
    assert result["provisioned"] is True

    # EIP allocated, instance launched, waited, EIP associated — in that order
    assert fake.names() == [
        "allocate_address", "run_instances", "waiter_wait", "associate_address"]

    # set_account_proxy got a WELL-FORMED credential URL on the right account
    assert setp.call_count == 1
    acct_arg, url_arg = setp.call_args.args[0], setp.call_args.args[1]
    assert acct_arg == "acct-9"
    assert url_arg.startswith("http://kanida:")
    # proxy_url targets the PRIVATE IP (app→proxy over the VPC); the EIP is only the
    # user-whitelisted egress IP and must NOT be the proxy target.
    assert url_arg.endswith("@10.20.10.50:8888")
    assert "1.2.3.4" not in url_arg
    assert prov.egress.egress_host is not None  # host is parseable

    # extract the generated password and prove it NEVER appears in the return
    password = url_arg.split("kanida:")[1].split("@")[0]
    assert password and password not in str(result)
    # …and never in the logs
    assert password not in caplog.text
    # the EIP association used the right ids
    assoc_kw = dict(fake.calls[3][1])
    assert assoc_kw == {"AllocationId": "eipalloc-abc", "InstanceId": "i-abc123"}


def test_provision_tags_everything_with_egress_user(monkeypatch, patch_egress):
    fake = FakeEc2()
    _patch_client(monkeypatch, fake)
    prov.provision_user_egress("acct-42", **_KW)

    alloc_kw = dict(fake.calls[0][1])
    tags = alloc_kw["TagSpecifications"][0]["Tags"]
    tagmap = {t["Key"]: t["Value"] for t in tags}
    assert tagmap["Project"] == "kanida-ai"
    assert tagmap["Env"] == "prod"
    assert tagmap["kanida:egress-user"] == "acct-42"


# ═══════════════════════════════════════════════════════════════════════════
# provision — fail-safe cleanup (never leak billable infra)
# ═══════════════════════════════════════════════════════════════════════════

def test_provision_instance_launch_fails_releases_eip(monkeypatch, patch_egress):
    setp, _ = patch_egress
    fake = FakeEc2(run_error=_FakeClientError("InternalError", "capacity"))
    _patch_client(monkeypatch, fake)

    with pytest.raises(prov.EgressProvisioningError):
        prov.provision_user_egress("acct-9", **_KW)

    # EIP was allocated then RELEASED; no instance to terminate; NO mapping stored
    assert "release_address" in fake.names()
    rel = [dict(k) for n, k in fake.calls if n == "release_address"][0]
    assert rel == {"AllocationId": "eipalloc-abc"}
    setp.assert_not_called()


def test_provision_associate_fails_cleans_up_both(monkeypatch, patch_egress):
    setp, _ = patch_egress
    fake = FakeEc2(assoc_error=_FakeClientError("Gateway.NotAttached"))
    _patch_client(monkeypatch, fake)

    with pytest.raises(prov.EgressProvisioningError):
        prov.provision_user_egress("acct-9", **_KW)

    # both the instance and the EIP are torn down; mapping never stored
    assert "terminate_instances" in fake.names()
    assert "release_address" in fake.names()
    setp.assert_not_called()


def test_provision_store_fails_tears_down(monkeypatch, patch_egress):
    setp, _ = patch_egress
    setp.side_effect = RuntimeError("vault down")
    fake = FakeEc2()
    _patch_client(monkeypatch, fake)

    with pytest.raises(prov.EgressProvisioningError):
        prov.provision_user_egress("acct-9", **_KW)

    # infra came up but the mapping write failed → tear the infra down
    assert "terminate_instances" in fake.names()
    assert "release_address" in fake.names()


# ═══════════════════════════════════════════════════════════════════════════
# provision — EIP quota
# ═══════════════════════════════════════════════════════════════════════════

def test_provision_quota_exceeded_typed_error(monkeypatch, patch_egress):
    setp, _ = patch_egress
    fake = FakeEc2(alloc_error=_FakeClientError("AddressLimitExceeded"))
    _patch_client(monkeypatch, fake)

    with pytest.raises(prov.EgressQuotaExceeded) as ei:
        prov.provision_user_egress("acct-9", **_KW)
    assert "quota" in str(ei.value).lower()

    # nothing launched, nothing stored
    assert "run_instances" not in fake.names()
    setp.assert_not_called()


# ═══════════════════════════════════════════════════════════════════════════
# deprovision — idempotent + always clears the mapping
# ═══════════════════════════════════════════════════════════════════════════

def test_deprovision_terminates_and_releases(monkeypatch, patch_egress):
    _, clearp = patch_egress
    fake = FakeEc2(
        instances=[{"InstanceId": "i-abc123", "State": {"Name": "running"}}],
        addresses=[{"AllocationId": "eipalloc-abc", "AssociationId": "eipassoc-1",
                    "PublicIp": "1.2.3.4"}])
    _patch_client(monkeypatch, fake)

    out = prov.deprovision_user_egress("acct-9", region="ap-south-1")
    assert out["deprovisioned"] is True
    assert out["terminated_instances"] == ["i-abc123"]
    assert out["released_allocations"] == ["eipalloc-abc"]
    assert "terminate_instances" in fake.names()
    assert "disassociate_address" in fake.names()
    assert "release_address" in fake.names()
    clearp.assert_called_once()


def test_deprovision_idempotent_when_already_gone(monkeypatch, patch_egress):
    _, clearp = patch_egress
    fake = FakeEc2(instances=[], addresses=[])
    _patch_client(monkeypatch, fake)

    out = prov.deprovision_user_egress("acct-9", region="ap-south-1")
    assert out["deprovisioned"] is True
    assert out["terminated_instances"] == []
    assert out["released_allocations"] == []
    # still clears the mapping so we never keep routing through destroyed infra
    clearp.assert_called_once()


def test_deprovision_clears_mapping_even_if_aws_errors(monkeypatch, patch_egress):
    _, clearp = patch_egress

    class BoomEc2(FakeEc2):
        def describe_instances(self, **kw):
            raise _FakeClientError("UnauthorizedOperation")

    fake = BoomEc2()
    _patch_client(monkeypatch, fake)

    out = prov.deprovision_user_egress("acct-9", region="ap-south-1")
    assert out["deprovisioned"] is True
    clearp.assert_called_once()


# ═══════════════════════════════════════════════════════════════════════════
# status
# ═══════════════════════════════════════════════════════════════════════════

def test_status_provisioned(monkeypatch, patch_egress):
    fake = FakeEc2(
        instances=[{"InstanceId": "i-abc123", "State": {"Name": "running"}}],
        addresses=[{"AllocationId": "eipalloc-abc", "PublicIp": "1.2.3.4"}])
    _patch_client(monkeypatch, fake)

    st = prov.get_user_egress_status("acct-9", region="ap-south-1")
    assert st == {"provisioned": True, "public_ip": "1.2.3.4",
                  "instance_state": "running"}


def test_status_not_provisioned(monkeypatch, patch_egress):
    fake = FakeEc2(instances=[], addresses=[])
    _patch_client(monkeypatch, fake)

    st = prov.get_user_egress_status("acct-9", region="ap-south-1")
    assert st == {"provisioned": False, "public_ip": None, "instance_state": None}


# ═══════════════════════════════════════════════════════════════════════════
# config resolution
# ═══════════════════════════════════════════════════════════════════════════

def test_load_config_missing_raises_config_error(monkeypatch):
    import services.kite_auth as kite_auth
    monkeypatch.setattr(kite_auth, "_load_env_file", lambda: None)
    for k in ("AWS_DEFAULT_REGION", "KANIDA_EGRESS_SUBNET_ID",
              "KANIDA_EGRESS_SG_ID", "KANIDA_EGRESS_AMI_ID"):
        monkeypatch.delenv(k, raising=False)
    with pytest.raises(prov.EgressConfigError) as ei:
        prov.load_config_from_env()
    # names the missing keys so the operator knows what to set
    assert "KANIDA_EGRESS_SUBNET_ID" in str(ei.value)


def test_load_config_ok_with_defaults(monkeypatch):
    import services.kite_auth as kite_auth
    monkeypatch.setattr(kite_auth, "_load_env_file", lambda: None)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-south-1")
    monkeypatch.setenv("KANIDA_EGRESS_SUBNET_ID", "subnet-1")
    monkeypatch.setenv("KANIDA_EGRESS_SG_ID", "sg-1")
    monkeypatch.setenv("KANIDA_EGRESS_AMI_ID", "ami-1")
    monkeypatch.delenv("KANIDA_EGRESS_INSTANCE_TYPE", raising=False)
    cfg = prov.load_config_from_env()
    assert cfg.region == "ap-south-1"
    assert cfg.instance_type == "t3.nano"  # default
    assert cfg.proxy_port == 8888          # default
