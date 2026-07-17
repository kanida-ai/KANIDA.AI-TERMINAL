# module: s3

**Purpose.** Private, versioned, KMS-encrypted bucket for reports/artifacts.

**SPOF / requirement addressed.** Durable off-laptop home for the docs/ops
reports + `data/artifacts` that today exist only on local disk. Versioning
protects against accidental overwrite/delete.

**Phase-0 status.** Authored, unverified. No app wiring yet — the app still
writes reports locally. Pointing report writers at S3 is a later phase.
