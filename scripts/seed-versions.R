#!/usr/bin/env Rscript
# Seed package_versions with historical data from package_version_history.
#
# This script:
#   1. ATTACHes the seed DB (containing package_version_history)
#   2. Copies package_version_history into the main feed.db
#   3. Converts historical versions into package_versions events:
#      - First version per package -> "new" event
#      - Subsequent versions -> "updated" event (with previous_version)
#   4. Sets first_published from the earliest version date
#   5. Deduplicates against existing package_versions entries

library(RSQLite)

args <- commandArgs(trailingOnly = TRUE)
db_path   <- args[1] %||% "feed.db"
seed_path <- args[2] %||% "feed-seed.db"

`%||%` <- function(x, y) if (is.null(x) || length(x) == 0) y else x

cat("=== Seeding package_versions from version history ===\n")
cat("Main DB: ", db_path, "\n")
cat("Seed DB: ", seed_path, "\n")

con <- dbConnect(SQLite(), db_path)
on.exit(dbDisconnect(con), add = TRUE)

dbExecute(con, "PRAGMA journal_mode=WAL")
dbExecute(con, "PRAGMA synchronous=NORMAL")

# Check current state
pv_before <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM package_versions")$n
cat("package_versions before: ", pv_before, " rows\n")

# ---------------------------------------------------------------------------
# Step 1: ATTACH seed DB and copy package_version_history
# ---------------------------------------------------------------------------
cat("\nStep 1: Importing package_version_history from seed DB...\n")
dbExecute(con, paste0("ATTACH DATABASE '", seed_path, "' AS seed"))

# Create table if needed
dbExecute(con, "
CREATE TABLE IF NOT EXISTS package_version_history (
  package   TEXT NOT NULL,
  version   TEXT NOT NULL,
  published TEXT,
  size_kb   REAL,
  source    TEXT DEFAULT 'cran',
  PRIMARY KEY (package, version)
)")
dbExecute(con, "
CREATE INDEX IF NOT EXISTS idx_pvh_package   ON package_version_history (package)")
dbExecute(con, "
CREATE INDEX IF NOT EXISTS idx_pvh_published ON package_version_history (published)")

dbExecute(con, "
  INSERT OR IGNORE INTO package_version_history (package, version, published, size_kb, source)
  SELECT package, version, published, size_kb, source
  FROM seed.package_version_history
")

pvh_count <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM package_version_history")$n
cat("  -> package_version_history now has", pvh_count, "rows\n")

dbExecute(con, "DETACH DATABASE seed")

# ---------------------------------------------------------------------------
# Step 2: Convert historical versions into package_versions events
# ---------------------------------------------------------------------------
cat("\nStep 2: Converting version history into package_versions events...\n")

# Get all historical versions ordered by package and date
history <- dbGetQuery(con, "
  SELECT package, version, published
  FROM package_version_history
  WHERE published IS NOT NULL AND published != ''
  ORDER BY package, published ASC, version ASC
")
cat("  -> Processing", nrow(history), "historical version records\n")

# Get existing package_versions to avoid duplicates
existing <- dbGetQuery(con, "
  SELECT package, version, event_type
  FROM package_versions
")
existing_key <- paste(existing$package, existing$version, existing$event_type, sep = "|")

# Build events from history
events <- list()
prev_pkg <- ""
prev_ver <- ""
skipped <- 0L
added <- 0L

for (i in seq_len(nrow(history))) {
  pkg <- history$package[i]
  ver <- history$version[i]
  pub <- history$published[i]

  if (pkg != prev_pkg) {
    # First version of this package = "new" event
    event_type <- "new"
    prev_version <- NA_character_
    prev_pkg <- pkg
    prev_ver <- ver
  } else {
    # Subsequent version = "updated" event
    event_type <- "updated"
    prev_version <- prev_ver
    prev_ver <- ver
  }

  # Skip if this exact event already exists
  key <- paste(pkg, ver, event_type, sep = "|")
  if (key %in% existing_key) {
    skipped <- skipped + 1L
    next
  }

  events[[length(events) + 1]] <- list(
    package = pkg,
    version = ver,
    event_type = event_type,
    previous_version = prev_version,
    removal_reason = NA_character_,
    detected_at = paste0(pub, "T00:00:00Z"),
    published = pub
  )
  added <- added + 1L
}

cat("  -> ", added, " new events to insert (", skipped, " already existed)\n")

# Insert in batches
if (added > 0) {
  events_df <- do.call(rbind, lapply(events, as.data.frame, stringsAsFactors = FALSE))

  batch_size <- 5000
  n_batches <- ceiling(nrow(events_df) / batch_size)

  dbBegin(con)
  tryCatch({
    for (b in seq_len(n_batches)) {
      start_idx <- (b - 1) * batch_size + 1
      end_idx <- min(b * batch_size, nrow(events_df))
      batch <- events_df[start_idx:end_idx, ]

      for (j in seq_len(nrow(batch))) {
        dbExecute(con, "
          INSERT INTO package_versions
            (package, version, event_type, previous_version, removal_reason, detected_at, published)
          VALUES (?, ?, ?, ?, ?, ?, ?)",
          params = list(
            batch$package[j], batch$version[j], batch$event_type[j],
            batch$previous_version[j], batch$removal_reason[j],
            batch$detected_at[j], batch$published[j]
          ))
      }

      if (b %% 10 == 0 || b == n_batches) {
        cat(sprintf("    Batch %d/%d (%d rows)\n", b, n_batches, end_idx))
      }
    }
    dbCommit(con)
  }, error = function(e) {
    dbRollback(con)
    stop(e)
  })
}

# ---------------------------------------------------------------------------
# Step 3: Set first_published from earliest version
# ---------------------------------------------------------------------------
cat("\nStep 3: Setting first_published dates...\n")

dbExecute(con, "
  UPDATE packages
  SET first_published = COALESCE(
    (SELECT MIN(published)
     FROM package_version_history
     WHERE package_version_history.package = packages.name
       AND published IS NOT NULL AND published != ''),
    (SELECT MIN(detected_at)
     FROM package_versions
     WHERE package_versions.package = packages.name
       AND package_versions.event_type = 'new'),
    packages.first_published
  )
")

fp_count <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM packages WHERE first_published IS NOT NULL AND first_published != ''")$n
cat("  ->", fp_count, "packages now have first_published dates\n")

# ---------------------------------------------------------------------------
# Step 4: Run ANALYZE for query optimization
# ---------------------------------------------------------------------------
cat("\nStep 4: Running ANALYZE...\n")
dbExecute(con, "ANALYZE")

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
pv_after <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM package_versions")$n
pkg_count <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM packages")$n
revdep_count <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM reverse_dependencies")$n
pvh_final <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM package_version_history")$n

event_stats <- dbGetQuery(con, "
  SELECT event_type, COUNT(*) AS n FROM package_versions GROUP BY event_type ORDER BY n DESC
")

db_size <- round(file.info(db_path)$size / 1024 / 1024, 2)

cat("\n=== Seed Complete ===\n")
cat("Tables:\n")
cat(sprintf("  packages:                %d\n", pkg_count))
cat(sprintf("  package_versions:        %d (was %d)\n", pv_after, pv_before))
cat(sprintf("  reverse_dependencies:    %d\n", revdep_count))
cat(sprintf("  package_version_history: %d\n", pvh_final))
cat("\nEvent types:\n")
for (i in seq_len(nrow(event_stats))) {
  cat(sprintf("  %-10s %d\n", event_stats$event_type[i], event_stats$n[i]))
}
cat(sprintf("\nDB size: %s MB\n", db_size))

# Write release notes
fp_range <- dbGetQuery(con, "
  SELECT MIN(first_published) AS earliest, MAX(first_published) AS latest
  FROM packages WHERE first_published IS NOT NULL AND first_published != ''
")

notes <- paste0(
  "## CRAN Feed — Seeded Database\n\n",
  "**", format(Sys.time(), "%Y-%m-%d %H:%M UTC", tz = "UTC"), "**\n\n",
  "This release contains the full CRAN feed database seeded with historical\n",
  "version data from the CRAN archive.\n\n",
  "| Table | Rows |\n",
  "|-------|------|\n",
  "| packages | ", pkg_count, " |\n",
  "| package_versions | ", pv_after, " |\n",
  "| reverse_dependencies | ", revdep_count, " |\n",
  "| package_version_history | ", pvh_final, " |\n\n",
  "### Event Breakdown\n\n",
  "| Type | Count |\n",
  "|------|-------|\n"
)
for (i in seq_len(nrow(event_stats))) {
  notes <- paste0(notes, "| ", event_stats$event_type[i], " | ", event_stats$n[i], " |\n")
}
notes <- paste0(notes, "\n",
  "Package history spans **", fp_range$earliest, "** to **", fp_range$latest, "**.\n\n",
  "DB size: **", db_size, " MB**\n"
)
writeLines(notes, "release_notes.md")
cat("\nRelease notes written to release_notes.md\n")
