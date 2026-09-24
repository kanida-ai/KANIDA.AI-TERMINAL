#!/usr/bin/env bash
# Step 2: data, secrets and Claude config off the SSD.   bash 2_restore_from_ssd.sh /Volumes/<SSD>
#
# Files already in the git checkouts are NEVER overwritten (--ignore-existing): code comes
# from git, and the SSD only fills in what git doesn't hold (databases, data, outputs).
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/config.sh"
SRC="${1:?usage: 2_restore_from_ssd.sh /Volumes/<SSD> [--delta | --claude]}/KanidaMove"
[ -d "$SRC/files" ] || die "$SRC/files not found — is the SSD mounted?"
[ -d "$FALCON/.git" ] || die "run 1_setup_mac.sh first"

claude_step() {
  say "Claude Code config"
  local C="$SRC/claude" d
  [ -d "$C" ] || { warn "no claude/ folder on the drive"; return 0; }
  mkdir -p "$HOME/.claude"
  for d in rules agents skills commands plans scheduled-tasks; do
    if [ -d "$C/$d" ]; then rsync -rt --ignore-existing "$C/$d/" "$HOME/.claude/$d/"; fi
  done
  if [ -f "$C/settings.json" ]; then cp "$C/settings.json" "$MIG/claude-settings.windows.json"; fi
  # project memory is keyed by the folder path; map the Windows keys to the new Mac paths
  key() { printf '%s' "$1" | sed 's/[^A-Za-z0-9]/-/g'; }
  map_mem() {  # a project with no memory folder is normal — never a failure
    if [ -d "$C/projects/$1/memory" ]; then
      mkdir -p "$HOME/.claude/projects/$(key "$2")"
      rsync -rt --ignore-existing "$C/projects/$1/memory/" "$HOME/.claude/projects/$(key "$2")/memory/"
      echo "   memory: $1 -> $2"
    fi
  }
  map_mem "C--Users-SPS-Documents-Kanida-Falcon" "$FALCON"
  map_mem "C--Users-SPS-Desktop-Kanida-ai-Terminal-Quant-Intelligence-Engine" "$ENGINE"
  map_mem "C--Users-SPS-Desktop-koptions" "$WT/koptions"
  map_mem "C--Users-SPS-Desktop-kanida-product" "$K/archive/Desktop/kanida-product"
  rsync -rt "$C/projects/" "$MIG/claude-projects-windows/"      # full transcripts, for reference
  echo "   Windows session transcripts -> ${MIG/#$HOME/~}/claude-projects-windows"
  python3 "$FALCON/migration/mac/fix_paths.py" --write $(find "$HOME/.claude/rules" "$HOME/.claude/agents" "$HOME/.claude/skills" "$HOME/.claude/commands" -type f -name '*.md' 2>/dev/null) || true
}

if [ "${2:-}" = "--claude" ]; then   # re-run only the Claude config step
  claude_step; say "Claude step done."; exit 0
fi

if [ "${2:-}" = "--delta" ]; then
  # Cutover: the Windows export was re-run with its services stopped. Take ONLY the
  # databases that are newer on the SSD; code and everything else is left alone.
  say "Delta: newer databases only"
  launchctl list 2>/dev/null | grep -q com.kanida. && die "Mac services are running — 4_services.sh uninstall first"
  DB=(rsync -rt --update --no-perms --chmod=Du=rwx,Dgo=rx,Fu=rw,Fgo=r --prune-empty-dirs --itemize-changes
      --include='*/' --include='*.db' --include='*.sqlite' --include='*.sqlite3' --exclude='*')
  delta() {  # <src> <dst>: copy newer DBs; drop the -wal/-shm that belonged to each replaced file
    "${DB[@]}" "$1" "$2" | while read -r flags path; do
      case "$flags" in '>f'*) rm -f "$2$path-wal" "$2$path-shm"; echo "   updated ${2#$K/}$path";; esac
    done
  }
  delta "$SRC/files/Kanida_Falcon/" "$FALCON/"
  delta "$SRC/files/engine/" "$ENGINE/"
  for d in KANIDA.AI_TERMINAL _kanida_deploy; do delta "$SRC/files/$d/" "$K/$d/"; done
  cp -R "$SRC/manifest" "$MIG/"
  say "Delta done. Run 5_verify.sh, then 4_services.sh install."
  exit 0
fi

need=$(du -sk "$SRC/files" | cut -f1); have=$(df -k "$HOME" | awk 'NR==2{print $4}')
say "Space: need $((need/1048576)) GB, free $((have/1048576)) GB"
[ "$have" -gt $((need + 52428800)) ] || die "not enough free space (keeping 50 GB headroom)"

RS=(rsync -rt --ignore-existing --no-perms --chmod=Du=rwx,Dgo=rx,Fu=rw,Fgo=r
    --exclude '._*' --exclude '.DS_Store' --exclude '*.snapshot-ok' --exclude '*.partial' --info=progress2)

say "Data -> checkouts"
"${RS[@]}" "$SRC/files/Kanida_Falcon/" "$FALCON/"
"${RS[@]}" "$SRC/files/engine/"        "$ENGINE/"
for d in KANIDA.AI_TERMINAL _kanida_deploy; do "${RS[@]}" "$SRC/files/$d/" "$K/$d/"; done
say "Everything else from Desktop / Documents / Downloads"
# Kanida folders -> ~/Kanida/archive; personal folders -> ~/Desktop, ~/Documents; see route() in config.sh
for side in Desktop Documents; do
  for d in "$SRC/files/archive/$side"/*/; do
    [ -d "$d" ] || continue; name="$(basename "$d")"; dst="$(route "archive/$side/$name")"
    mkdir -p "$dst"; "${RS[@]}" "$d" "$dst/"; echo "   $side/$name -> ${dst/#$HOME/~}"
  done
done
if [ -d "$SRC/files/archive/Downloads" ]; then
  dst="$(route archive/Downloads)"; mkdir -p "$dst"; "${RS[@]}" "$SRC/files/archive/Downloads/" "$dst/"; echo "   Downloads -> ${dst/#$HOME/~}"
fi
# Windows worktrees' untracked files, if any, land beside the new worktrees
for pair in $WORKTREES; do n="${pair%%:*}"; [ -d "$K/archive/Desktop/$n" ] && "${RS[@]}" "$K/archive/Desktop/$n/" "$WT/$n/" || true; done
cp -R "$SRC/env-freeze" "$SRC/manifest" "$MIG/"

say "Secrets (you'll be asked for the passphrase you chose on Windows)"
if [ -f "$SRC/secrets.tar.enc" ]; then
  want=$(tr -d '\r\n ' < "$SRC/manifest/secrets.sha256" | tr 'A-F' 'a-f')
  got=$(shasum -a 256 "$SRC/secrets.tar.enc" | cut -d' ' -f1)
  [ "$want" = "$got" ] || die "secrets.tar.enc checksum mismatch"
  # extracted Windows folders can arrive read-only; make them writable so cleanup can't leave secrets behind
  stage="$(mktemp -d)"; trap 'chmod -R u+w "$stage" 2>/dev/null; rm -rf "$stage"' EXIT
  "$(brew --prefix openssl@3)/bin/openssl" enc -d -aes-256-cbc -pbkdf2 -iter 600000 -in "$SRC/secrets.tar.enc" -out "$stage/s.tar"
  mkdir "$stage/x"; tar -xf "$stage/s.tar" -C "$stage/x"
  # the bundle mirrors the SSD layout: Kanida_Falcon/, engine/, archive/..., _home/
  (cd "$stage/x" && find . -type f ! -path './_home/*') | while read -r f; do
    f="${f#./}"; case "$f" in Kanida_Falcon/*|engine/*|KANIDA.AI_TERMINAL/*|_kanida_deploy/*) dst="$K/$f";; archive/*) dst="$(route "$f")";; *) continue;; esac
    mkdir -p "$(dirname "$dst")"; [ -e "$dst" ] || cp "$stage/x/$f" "$dst"; chmod 600 "$dst"
  done
  if [ -d "$stage/x/_home" ]; then
    for d in .aws .ssh .cloudflared; do [ -d "$stage/x/_home/$d" ] && [ ! -e "$HOME/$d" ] && cp -R "$stage/x/_home/$d" "$HOME/$d" && chmod -R go-rwx "$HOME/$d"; done
    [ -f "$HOME/.cloudflared/config.yml" ] && python3 "$FALCON/migration/mac/fix_paths.py" --write "$HOME/.cloudflared/config.yml"
    [ -f "$stage/x/_home/.gitconfig" ] && cp "$stage/x/_home/.gitconfig" "$MIG/gitconfig.windows"
  fi
  echo "   restored $(cd "$stage/x" && find . -type f | wc -l | tr -d ' ') secret files (chmod 600)"
else
  warn "no secrets.tar.enc on the SSD — .env files must be restored by hand"
fi

say "Rewriting Windows paths inside .env / config files"
python3 "$FALCON/migration/mac/fix_paths.py" --write \
  $(find "$FALCON" "$ENGINE" "$TERMINAL" "$DEPLOY" -maxdepth 4 \( -name '.env' -o -name '.env.*' -o -name '*.env' \) -type f ! -path '*/node_modules/*' 2>/dev/null)

claude_step
say "Step 2 done. Next:  bash $FALCON/migration/mac/3_build_envs.sh"
