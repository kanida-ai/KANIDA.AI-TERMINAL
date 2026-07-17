# Phase 1 — AWS CloudShell Runbook (copy-paste, no Terraform knowledge needed)

**What this is:** a dead-simple, numbered guide to run the Phase-0 Terraform
skeleton for the FIRST time in **AWS CloudShell** and see whether it is healthy —
**without creating anything real**. You will run three checks: `init`, `validate`,
and `plan`. You will **NOT** run `apply`. Nothing in AWS is created or charged by
this runbook (except a few pennies of CloudShell, which is free-tier).

> **Honesty note (read this):** this Terraform was written on a machine with **no
> Terraform installed**, so it was **never actually run**. It has been
> **statically reviewed** and the obvious errors fixed, but a first real run can
> still surface residual issues. That is EXPECTED and FINE — that is literally
> what this runbook is for. When something fails, you copy the red text and paste
> it back to Claude (each step tells you exactly what to paste).

> **Golden rule:** if you ever see the word **`apply`** in a command, **STOP** and
> ask Claude first. `validate` and `plan` are safe (they only look/pretend).
> `apply` is the one that builds real, billable cloud resources.

---

## Step 0 — Open CloudShell

1. Log in to the AWS Console in your browser.
2. Top-right, make sure the region says **Asia Pacific (Mumbai) ap-south-1**.
   (This stack is Mumbai-only. Wrong region = wrong/empty results.)
3. Click the **`>_`** terminal icon in the top toolbar (that's CloudShell). Wait
   for the black terminal to say it's ready.

**Success looks like:** a black terminal with a prompt ending in `$`.

---

## Step 1 — Get the `deploy/terraform` files into CloudShell

You need the `terraform` folder inside CloudShell. Pick **ONE** option.

### Option A (easiest — upload a zip)

1. On your **laptop**, in File Explorer, go to the repo's `deploy` folder.
2. Right-click the **`terraform`** folder → **Send to → Compressed (zipped)
   folder**. You now have `terraform.zip`.
3. In CloudShell, top-right **Actions** menu → **Upload file** → pick
   `terraform.zip`.
4. Back in the terminal, paste this to unzip it:
   ```bash
   cd ~ && unzip -o terraform.zip -d kanida && cd kanida/terraform && ls
   ```

**Success looks like:** the `ls` prints `main.tf variables.tf outputs.tf
versions.tf modules ...` and your prompt is now inside `.../terraform`.

### Option B (git clone — only if you have a GitHub token handy)

```bash
cd ~ && git clone https://github.com/<your-org>/<your-repo>.git kanida-repo && cd kanida-repo/deploy/terraform && ls
```
(If it asks for a username/password, use your GitHub username and a **Personal
Access Token** as the password. If you don't have one, use Option A instead.)

**Success looks like:** same as Option A — the `ls` shows `main.tf`, `modules`, etc.

> **If Step 1 fails:** paste back the full terminal output plus which option you
> tried.

---

## Step 2 — Confirm you're in the right folder

```bash
pwd && ls *.tf
```

**Success looks like:** `pwd` ends in `/terraform`, and you see
`main.tf outputs.tf variables.tf versions.tf`.

> **If it doesn't:** you're in the wrong directory — paste the output back.

---

## Step 3 — `terraform init` (download the AWS plugin)

```bash
terraform init
```
This downloads the AWS + random provider plugins from the internet. Takes ~30–60s.

**Success looks like** — the last lines say:
```
Terraform has been successfully initialized!
```

> **If it fails:** copy EVERYTHING from the first red `Error:` line to the bottom
> and paste it to Claude with the note "init failed". Common causes are network
> hiccups (just re-run `terraform init`) or a provider-version issue (Claude
> fixes in `versions.tf`).

---

## Step 4 — `terraform validate` (grammar check, offline, 100% safe)

```bash
terraform validate
```
This checks the files for mistakes **without touching AWS at all**. This is the
single most important safety check and it creates nothing.

**Success looks like — exactly this:**
```
Success! The configuration is valid.
```

> **If it says `Error:`** — this is the expected place for residual issues to show
> up. Copy the WHOLE output (every `Error:` block — there may be more than one)
> and paste it to Claude with "validate failed". Errors here are cheap to fix and
> nothing was created. Do not proceed to Step 5 until validate says `Success!`.

---

## Step 5 — `terraform plan` (a dry-run preview — still creates NOTHING)

```bash
terraform plan
```
`plan` asks AWS "if I applied this, what WOULD change?" and prints a list. It
**does not create anything.** CloudShell already has your AWS login, so you don't
enter any credentials. This can take 1–2 minutes and will read a couple of
look-ups (an Amazon Linux image, your account ID) — that's normal.

**Success looks like** — near the bottom:
```
Plan: NN to add, 0 to change, 0 to destroy.
```
(NN will be a number like 40-something. **Adding** things in a *plan* is fine —
"add" here means "would add IF you applied". You are NOT applying.)

> **Expected, not-a-problem notes:**
> - The compute image shows as `PLACEHOLDER_ECR_IMAGE_URI`. That's intentional
>   for Phase 1 — the real image comes later.
> - `Plan: N to add` with the egress/user boxes at **0** is correct — no users are
>   configured yet (`egress_users = []`).

> **If `plan` fails:** copy the full `Error:` text and paste to Claude with "plan
> failed". The most likely real-world flags (Claude will confirm/fix these):
> - **RDS engine version** `15.7` no longer offered in Mumbai → Claude changes it.
> - **ElastiCache Redis** `7.1` not offered → Claude changes it.
> - An EIP/quota or permissions message → may need an AWS support/limit action.

---

## Step 6 — STOP. Do not apply.

You are done with Phase 1. **Do NOT run `terraform apply`.** Applying builds real,
billable infrastructure and there are AWS-account prerequisites (remote state
bucket, an ACM certificate, real secret values) that must happen first — see
`deploy/PHASE0_README.md` section C and `deploy/SECRETS_MAP.md`.

Tell Claude: "validate = Success, plan = Plan: N to add, 0 change, 0 destroy" and
paste the last ~20 lines of the plan. Claude reviews the plan with you before
anything real is ever created.

---

## Optional — build/test the container (NOT required for Phase 1)

Building the Docker image is a **separate, optional** exercise and it needs a
**copy of the production database** to do the real boot test. **CloudShell is NOT
the place for it** (no Docker, and you should never point it at the live trading
DB). Do this later on a machine with Docker Desktop, following
`deploy/PHASE0_README.md` section A. Skip it for Phase 1.

---

## Quick cheat-sheet of what's safe

| Command | Safe? | What it does |
|---|---|---|
| `terraform init` | ✅ yes | downloads plugins |
| `terraform validate` | ✅ yes (offline) | grammar check, creates nothing |
| `terraform plan` | ✅ yes | dry-run preview, creates nothing |
| `terraform apply` | ⛔ **STOP — ask Claude** | **builds real, billable cloud resources** |
| `terraform destroy` | ⛔ **STOP — ask Claude** | tears down real resources |

## When pasting an error back to Claude, include:
1. Which **step number** you were on.
2. The **whole** red `Error:` block(s) — from the first `Error:` to the end.
3. What you **expected** vs what you **saw** (one line is enough).
