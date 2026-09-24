# Shared layout for the Mac. Sourced by every script in migration/mac/.
# Everything lives in ~/Kanida — deliberately NOT ~/Documents or ~/Desktop, which
# macOS may sync to iCloud (265 GB of databases must never go there).
K="$HOME/Kanida"
FALCON="$K/Kanida_Falcon"                  # was C:\Users\SPS\Documents\Kanida_Falcon
ENGINE="$K/engine"                         # was Desktop\Kanida.ai Terminal Quant Intelligence Engine
TERMINAL="$K/KANIDA.AI_TERMINAL"           # was Desktop\KANIDA.AI_TERMINAL (nightly jobs)
DEPLOY="$K/_kanida_deploy"                 # was Desktop\_kanida_deploy
WT="$K/worktrees"                          # the other engine worktrees
MIG="$K/_migration"                        # env freezes, manifests, logs from the move
LOGS="$K/logs"                             # launchd stdout/stderr

REPO_URL="https://github.com/kanida-ai/KANIDA.AI-TERMINAL.git"
FALCON_BRANCH="wip/mac-move-2026-09-23"
ENGINE_BRANCH="feat/cloud-rupeezy-token-sync"
SNAP="wip/mac-move-terminal"               # origin/$SNAP/branch/<b> and origin/$SNAP/worktree-<name>

# worktree name on Windows -> branch it had checked out
WORKTREES="koptions:agent/options kanida-dev:feat/self-improving-engine _kanida_autotrade:feat/per-account-egress-proxy _kanida_persona:feat/persona-expansion"

export KANIDA_ENGINE_ROOT="$ENGINE"
export KANIDA_AUTH_PYTHON="$ENGINE/.venv/bin/python"

say() { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }
warn() { printf '\033[1;33m!! %s\033[0m\n' "$*"; }
die() { printf '\033[1;31mxx %s\033[0m\n' "$*"; exit 1; }
