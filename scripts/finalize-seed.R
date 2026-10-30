#!/usr/bin/env Rscript
# Finish a Backfill Version History run the way update.R finishes its own:
# collapse duplicate "new" events, refuse to publish a database that lost
# history the previous release had, and write the manifest the next run is
# held to.

library(RSQLite)

.script_dir <- {
  cli <- commandArgs(FALSE)
  f <- sub("^--file=", "", grep("^--file=", cli, value = TRUE))
  if (length(f) == 1L && nzchar(f)) dirname(normalizePath(f)) else "scripts"
}
source(file.path(.script_dir, "helpers.R"))

args <- commandArgs(trailingOnly = TRUE)
db_path <- if (length(args) >= 1) args[1] else "feed.db"
manifest_path <- file.path(dirname(db_path), "manifest.json")

prior <- read_prior_manifest(manifest_path)
if (is.null(prior)) {
  cat("No previous manifest alongside the database; nothing to compare against.\n")
}

# seed-versions.R adds events dated to each release's publication, so a "new"
# the tracker recorded later becomes a duplicate. update.R collapses those on
# its next run; doing it here as well keeps that run from seeing the event log
# shrink below this release and refusing to publish.
con <- dbConnect(SQLite(), db_path)
cat("Collapsed", collapse_duplicate_new_events(con), "duplicate 'new' events.\n")
invisible(dbExecute(con, "PRAGMA wal_checkpoint(TRUNCATE)"))
dbDisconnect(con)

core <- publish_manifest(db_path, prior, manifest_path)
cat("Wrote", manifest_path, "with sha256", core$db_sha256, "\n")
