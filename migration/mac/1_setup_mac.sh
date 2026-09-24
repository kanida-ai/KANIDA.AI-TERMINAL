#!/usr/bin/env bash
# Step 1 on the Mac: tools, then the code from GitHub with every in-flight change restored.
#   curl -fsSL https://raw.githubusercontent.com/kanida-ai/KANIDA.AI-TERMINAL/wip/mac-move-2026-09-23/migration/mac/1_setup_mac.sh -o /tmp/1.sh
#   (or copy it off the SSD), then:  bash 1_setup_mac.sh
# Idempotent: re-running skips what is already there.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
if [ -f "$HERE/config.sh" ]; then source "$HERE/config.sh"
else curl -fsSL "https://raw.githubusercontent.com/kanida-ai/KANIDA.AI-TERMINAL/wip/mac-move-2026-09-23/migration/mac/config.sh" -o /tmp/kanida-config.sh; source /tmp/kanida-config.sh; fi

say "Xcode command-line tools"
xcode-select -p >/dev/null 2>&1 || { xcode-select --install; die "Finish the Xcode tools installer, then re-run."; }

say "Homebrew"
if ! command -v brew >/dev/null; then
  /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
fi
eval "$(/opt/homebrew/bin/brew shellenv 2>/dev/null || /usr/local/bin/brew shellenv)"
grep -q 'brew shellenv' "$HOME/.zprofile" 2>/dev/null || echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> "$HOME/.zprofile"

say "Packages"
# node@24 matches Windows (v24.14); uv builds the Python 3.12 / 3.13 venvs.
brew install git gh awscli uv node@24 sqlite openssl@3 pandoc coreutils rsync jq cloudflared
brew link --overwrite --force node@24 >/dev/null
uv python install 3.12 3.13

say "Git identity + line endings"
git config --global user.name  >/dev/null || git config --global user.name  "cashcropindia-pixel"
git config --global user.email >/dev/null || git config --global user.email "cashcropindia@gmail.com"
git config --global core.autocrlf input
git config --global init.defaultBranch main

say "GitHub login"
gh auth status >/dev/null 2>&1 || gh auth login --hostname github.com --git-protocol https --web
gh auth setup-git

mkdir -p "$K" "$WT" "$MIG" "$LOGS"

say "Falcon repo -> $FALCON"
if [ ! -d "$FALCON/.git" ]; then
  git clone "$REPO_URL" "$FALCON"
fi
git -C "$FALCON" fetch origin --prune
git -C "$FALCON" switch "$FALCON_BRANCH" 2>/dev/null || git -C "$FALCON" switch -c "$FALCON_BRANCH" --track "origin/$FALCON_BRANCH"

# Recreate a Windows worktree: branch from its pushed tip, then lay the snapshot of its
# uncommitted files on top AS uncommitted changes (exactly the state it was in).
restore_worktree() {  # <dir> <branch> <snapshot-name>
  local dir="$1" br="$2" snap="$3" base
  if git -C "$FALCON" rev-parse -q --verify "refs/remotes/origin/$SNAP/branch/$br" >/dev/null; then base="origin/$SNAP/branch/$br"
  else base="origin/$br"; fi
  if [ ! -e "$dir/.git" ]; then
    if git -C "$FALCON" rev-parse -q --verify "refs/heads/$br" >/dev/null; then git -C "$FALCON" worktree add "$dir" "$br"
    else git -C "$FALCON" worktree add -b "$br" "$dir" "$base"; fi
  fi
  local snapref="origin/$SNAP/worktree-$snap"
  if git -C "$FALCON" rev-parse -q --verify "refs/remotes/$snapref" >/dev/null && [ -z "$(git -C "$dir" status --porcelain)" ]; then
    git -C "$dir" diff --binary HEAD "$snapref" | git -C "$dir" apply --whitespace=nowarn
    echo "   restored $(git -C "$dir" status --porcelain | wc -l | tr -d ' ') uncommitted files into $dir"
  fi
}

say "Engine -> $ENGINE ($ENGINE_BRANCH + its uncommitted work)"
restore_worktree "$ENGINE" "$ENGINE_BRANCH" "main-engine"

say "Other worktrees -> $WT"
for pair in $WORKTREES; do restore_worktree "$WT/${pair%%:*}" "${pair#*:}" "${pair%%:*}"; done

say "Step 1 done. Plug in the SSD and run:  bash $FALCON/migration/mac/2_restore_from_ssd.sh /Volumes/<SSD>"
