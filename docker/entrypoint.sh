#!/bin/sh
set -eu

repo="${VOLCANO_REPO_URL:-https://github.com/volcano-sh/volcano.git}"
dir="${VOLENS_SOURCE_DIR:-/var/lib/volens/volcano}"

if [ -d "$dir/.git" ]; then
  if ! git -C "$dir" rev-parse --git-dir >/dev/null 2>&1; then
    echo "Volcano source seed at $dir is not a valid Git repository" >&2
    exit 1
  fi
else
  if [ -e "$dir" ] && { [ ! -d "$dir" ] || [ -n "$(ls -A "$dir")" ]; }; then
    echo "Volcano source path $dir exists but does not contain a usable image seed" >&2
    exit 1
  fi

  mkdir -p "$(dirname "$dir")"
  git clone --no-single-branch "$repo" "$dir"
fi

if git -C "$dir" remote get-url origin >/dev/null 2>&1; then
  git -C "$dir" remote set-url origin "$repo"
else
  git -C "$dir" remote add origin "$repo"
fi

git -C "$dir" config --local --replace-all \
  remote.origin.fetch '+refs/heads/*:refs/remotes/origin/*'

exec /usr/local/bin/volens
