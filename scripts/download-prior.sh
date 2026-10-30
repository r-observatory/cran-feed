#!/usr/bin/env bash
# Download the previous release's feed.db and manifest.json into the current
# directory. feed.db is carried forward in the release asset, so this download
# IS the pipeline's state, and every workflow that publishes a release runs it.
#
# It used to be `gh release download --pattern "feed.db"` under
# continue-on-error, which cannot tell a repository with no releases from an
# API that is briefly unavailable: either way the next step builds feed.db from
# empty and publishes it as the new latest. cran-queue lost its history to
# exactly that twice (2026-07-16, a swallowed download; 2026-10-03, a swallowed
# release lookup), so the two cases diverge here:
#
#   * a release list that comes back empty is a genuine first run;
#   * a repository that HAS a release must hand over its feed.db, and its
#     manifest.json when it lists one, or the run fails.
#
# gh runs only in plain assignments or bare commands, so under set -e an API
# error fails the run instead of reading as "no releases".
set -euo pipefail

releases="$(gh release list --limit 1 --exclude-drafts)"
if [ -z "$releases" ]; then
  echo "No releases exist yet; starting fresh."
  exit 0
fi
tag="$(gh release view --json tagName -q .tagName)"
echo "Previous release: $tag"

# A missing asset and a failed download look the same to `gh release download`,
# so ask the release what it carries before pulling anything.
assets="$(gh release view "$tag" --json assets -q '.assets[].name')"
if ! printf '%s\n' "$assets" | grep -qx 'feed.db'; then
  echo "::error::${tag} carries no feed.db (assets: ${assets:-none}). Refusing to start from an empty database."
  exit 1
fi

for attempt in 1 2 3 4 5; do
  if gh release download "$tag" --pattern "feed.db" --clobber; then
    downloaded=1
    [ "$attempt" -gt 1 ] && echo "downloaded feed.db (attempt $attempt)"
    break
  fi
  echo "attempt ${attempt}/5 failed to download feed.db from ${tag}"
  [ "$attempt" -lt 5 ] && sleep $((attempt * 10))
done
if [ "${downloaded:-0}" != "1" ]; then
  echo "::error::${tag} exists but its feed.db could not be downloaded. Refusing to start from an empty database, which would discard the event log and the archive history."
  exit 1
fi
# An empty or truncated file would reach update.R as a fresh database.
if [ ! -s feed.db ]; then
  echo "::error::downloaded feed.db from ${tag} is empty"
  exit 1
fi

# The manifest states what that release contained, and the run is held to it
# before publishing. Releases predating it do not have one, so its absence is
# not an error; a release that lists one must hand it over, because without it
# the retention check has nothing to compare against.
if printf '%s\n' "$assets" | grep -qx 'manifest.json'; then
  gh release download "$tag" --pattern "manifest.json" --clobber
else
  echo "::warning::${tag} has no manifest.json; this run cannot be checked against it"
fi
