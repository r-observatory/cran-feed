#!/usr/bin/env Rscript
# CRAN Feed — detect package additions, updates, and removals on CRAN
# Writes results to a SQLite database (feed.db)

options(timeout = 60)

library(RSQLite)

# ---------------------------------------------------------------------------
# CLI argument: path to the SQLite database
# ---------------------------------------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
db_path <- if (length(args) >= 1) args[1] else "feed.db"

cat("Database path:", db_path, "\n")

# ---------------------------------------------------------------------------
# Connect and configure SQLite
# ---------------------------------------------------------------------------
con <- dbConnect(SQLite(), db_path)
on.exit(dbDisconnect(con), add = TRUE)

dbExecute(con, "PRAGMA journal_mode=WAL")
dbExecute(con, "PRAGMA synchronous=NORMAL")

# ---------------------------------------------------------------------------
# Create tables if they don't exist
# ---------------------------------------------------------------------------
dbExecute(con, "
CREATE TABLE IF NOT EXISTS packages (
  name              TEXT PRIMARY KEY,
  version           TEXT,
  title             TEXT,
  description       TEXT,
  maintainer        TEXT,
  maintainer_email  TEXT,
  license           TEXT,
  depends           TEXT,
  imports           TEXT,
  suggests          TEXT,
  linking_to        TEXT,
  needs_compilation TEXT,
  published         TEXT,
  cran_url          TEXT,
  first_published   TEXT,
  is_archived       INTEGER DEFAULT 0,
  updated_at        TEXT
)")

dbExecute(con, "
CREATE TABLE IF NOT EXISTS package_versions (
  id                INTEGER PRIMARY KEY AUTOINCREMENT,
  package           TEXT NOT NULL,
  version           TEXT,
  event_type        TEXT NOT NULL,
  previous_version  TEXT,
  removal_reason    TEXT,
  detected_at       TEXT NOT NULL,
  published         TEXT
)")

dbExecute(con, "
CREATE INDEX IF NOT EXISTS idx_pv_package  ON package_versions (package)")
dbExecute(con, "
CREATE INDEX IF NOT EXISTS idx_pv_detected ON package_versions (detected_at)")
dbExecute(con, "
CREATE INDEX IF NOT EXISTS idx_pv_event    ON package_versions (event_type)")

dbExecute(con, "
CREATE TABLE IF NOT EXISTS reverse_dependencies (
  package     TEXT NOT NULL,
  rev_package TEXT NOT NULL,
  type        TEXT NOT NULL,
  PRIMARY KEY (package, rev_package, type)
)")

dbExecute(con, "
CREATE INDEX IF NOT EXISTS idx_revdep_pkg ON reverse_dependencies (package)")

# ---------------------------------------------------------------------------
# Fetch current CRAN state
# ---------------------------------------------------------------------------
cat("Fetching available.packages() ...\n")
ap <- available.packages(repos = "https://cloud.r-project.org")
cat("  ->", nrow(ap), "packages from available.packages()\n")

cat("Fetching tools::CRAN_package_db() ...\n")
cran_db <- tools::CRAN_package_db()
cat("  ->", nrow(cran_db), "packages from CRAN_package_db()\n")

# Index CRAN_package_db by package name for quick lookup
rownames(cran_db) <- cran_db$Package

# Current package names from available.packages
current_names <- as.character(ap[, "Package"])

# ---------------------------------------------------------------------------
# Load previous state from DB
# ---------------------------------------------------------------------------
if (dbExistsTable(con, "packages")) {
  prev <- dbGetQuery(con, "SELECT name, version FROM packages WHERE is_archived = 0")
  prev_names    <- prev$name
  prev_versions <- setNames(prev$version, prev$name)
} else {
  prev_names    <- character(0)
  prev_versions <- character(0)
}

cat("Previous packages:", length(prev_names), "\n")

# ---------------------------------------------------------------------------
# Detect changes
# ---------------------------------------------------------------------------
new_pkgs     <- setdiff(current_names, prev_names)
removed_pkgs <- setdiff(prev_names, current_names)
common_pkgs  <- intersect(current_names, prev_names)

# Updated = same name but different version
current_versions <- setNames(as.character(ap[, "Version"]), current_names)
updated_pkgs <- common_pkgs[current_versions[common_pkgs] != prev_versions[common_pkgs]]

cat("New packages:     ", length(new_pkgs), "\n")
cat("Removed packages: ", length(removed_pkgs), "\n")
cat("Updated packages: ", length(updated_pkgs), "\n")

# ---------------------------------------------------------------------------
# Helper: parse maintainer field into name and email
# ---------------------------------------------------------------------------
parse_maintainer <- function(m) {
  m <- trimws(m)
  if (is.na(m) || m == "") return(list(name = NA_character_, email = NA_character_))
  # Pattern: "Name <email>"
  match <- regmatches(m, regexec("^(.+?)\\s*<([^>]+)>", m))[[1]]
  if (length(match) == 3) {
    list(name = trimws(match[2]), email = trimws(match[3]))
  } else {
    list(name = m, email = NA_character_)
  }
}

# ---------------------------------------------------------------------------
# Record events in package_versions (append-only)
# ---------------------------------------------------------------------------
now <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")

insert_event <- function(pkg, version, event_type, previous_version = NA,
                         removal_reason = NA, published = NA) {
  dbExecute(con, "
    INSERT INTO package_versions (package, version, event_type, previous_version,
                                  removal_reason, detected_at, published)
    VALUES (?, ?, ?, ?, ?, ?, ?)",
    params = list(pkg, version, event_type, previous_version,
                  removal_reason, now, published))
}

# New packages
for (pkg in new_pkgs) {
  ver <- current_versions[pkg]
  pub <- if (pkg %in% rownames(cran_db)) as.character(cran_db[pkg, "Published"]) else NA
  insert_event(pkg, ver, "new", published = pub)
}

# Updated packages
for (pkg in updated_pkgs) {
  ver <- current_versions[pkg]
  prev_ver <- prev_versions[pkg]
  pub <- if (pkg %in% rownames(cran_db)) as.character(cran_db[pkg, "Published"]) else NA
  insert_event(pkg, ver, "updated", previous_version = prev_ver, published = pub)
}

# Removed packages
for (pkg in removed_pkgs) {
  prev_ver <- prev_versions[pkg]
  insert_event(pkg, prev_ver, "removed", removal_reason = "no longer on CRAN")
}

cat("Events recorded.\n")

# ---------------------------------------------------------------------------
# Rebuild packages table from current state
# ---------------------------------------------------------------------------
cat("Rebuilding packages table ...\n")

dbExecute(con, "DELETE FROM packages")

# Safe accessor for cran_db columns
cran_val <- function(pkg, col) {
  if (pkg %in% rownames(cran_db) && col %in% colnames(cran_db)) {
    val <- cran_db[pkg, col]
    if (is.null(val) || length(val) == 0) NA_character_ else as.character(val)
  } else {
    NA_character_
  }
}

# Build data.frame for bulk insert
pkg_rows <- lapply(current_names, function(pkg) {
  ver   <- current_versions[pkg]
  maint <- cran_val(pkg, "Maintainer")
  parsed <- parse_maintainer(maint)

  data.frame(
    name              = pkg,
    version           = ver,
    title             = cran_val(pkg, "Title"),
    description       = cran_val(pkg, "Description"),
    maintainer        = parsed$name,
    maintainer_email  = parsed$email,
    license           = cran_val(pkg, "License"),
    depends           = cran_val(pkg, "Depends"),
    imports           = cran_val(pkg, "Imports"),
    suggests          = cran_val(pkg, "Suggests"),
    linking_to        = cran_val(pkg, "LinkingTo"),
    needs_compilation = as.character(ap[pkg, "NeedsCompilation"]),
    published         = cran_val(pkg, "Published"),
    cran_url          = paste0("https://CRAN.R-project.org/package=", pkg),
    first_published   = NA_character_,
    is_archived       = 0L,
    updated_at        = now,
    stringsAsFactors  = FALSE
  )
})
pkg_df <- do.call(rbind, pkg_rows)

dbWriteTable(con, "packages", pkg_df, append = TRUE)
cat("  -> Inserted", nrow(pkg_df), "packages.\n")

# ---------------------------------------------------------------------------
# Set first_published from earliest "new" event in package_versions
# ---------------------------------------------------------------------------
cat("Setting first_published dates ...\n")
dbExecute(con, "
  UPDATE packages
  SET first_published = (
    SELECT MIN(detected_at)
    FROM package_versions
    WHERE package_versions.package = packages.name
      AND package_versions.event_type = 'new'
  )
")

# ---------------------------------------------------------------------------
# Rebuild reverse_dependencies
# ---------------------------------------------------------------------------
cat("Rebuilding reverse_dependencies ...\n")
dbExecute(con, "DELETE FROM reverse_dependencies")

dep_types <- c("Depends", "Imports", "Suggests", "LinkingTo")
dep_labels <- c("depends", "imports", "suggests", "linking_to")

all_revdeps <- list()

for (i in seq_along(dep_types)) {
  col <- dep_types[i]
  label <- dep_labels[i]
  if (!(col %in% colnames(ap))) next

  for (pkg in current_names) {
    raw <- ap[pkg, col]
    if (is.na(raw) || raw == "") next
    # Split on comma, strip version constraints and whitespace
    deps <- trimws(unlist(strsplit(raw, ",")))
    deps <- sub("\\s*\\(.*\\)\\s*$", "", deps)
    deps <- trimws(deps)
    deps <- deps[deps != "" & deps != "R"]

    for (dep in deps) {
      all_revdeps[[length(all_revdeps) + 1]] <- data.frame(
        package = dep, rev_package = pkg, type = label,
        stringsAsFactors = FALSE
      )
    }
  }
}

if (length(all_revdeps) > 0) {
  revdep_df <- do.call(rbind, all_revdeps)
  # Remove duplicates (composite PK)
  revdep_df <- revdep_df[!duplicated(revdep_df), ]
  dbWriteTable(con, "reverse_dependencies", revdep_df, append = TRUE)
  cat("  -> Inserted", nrow(revdep_df), "reverse dependency rows.\n")
} else {
  cat("  -> No reverse dependencies found.\n")
}

# ---------------------------------------------------------------------------
# Release notes
# ---------------------------------------------------------------------------
cat("Writing release notes ...\n")

total_pkgs   <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM packages")$n
total_events <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM package_versions")$n
revdep_count <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM reverse_dependencies")$n
db_size_mb   <- round(file.info(db_path)$size / 1024 / 1024, 2)

notes <- paste0(
  "## CRAN Feed Update\n\n",
  "**", format(Sys.time(), "%Y-%m-%d %H:%M UTC", tz = "UTC"), "**\n\n",
  "| Metric | Count |\n",
  "|--------|-------|\n",
  "| New packages | ", length(new_pkgs), " |\n",
  "| Updated packages | ", length(updated_pkgs), " |\n",
  "| Removed packages | ", length(removed_pkgs), " |\n",
  "| Total packages | ", total_pkgs, " |\n",
  "| Total events | ", total_events, " |\n",
  "| Reverse dependencies | ", revdep_count, " |\n",
  "| DB size | ", db_size_mb, " MB |\n"
)

# List individual new and removed packages if <= 20
if (length(new_pkgs) > 0 && length(new_pkgs) <= 20) {
  notes <- paste0(notes, "\n### New packages\n\n",
                  paste0("- ", sort(new_pkgs), collapse = "\n"), "\n")
}

if (length(removed_pkgs) > 0 && length(removed_pkgs) <= 20) {
  notes <- paste0(notes, "\n### Removed packages\n\n",
                  paste0("- ", sort(removed_pkgs), collapse = "\n"), "\n")
}

writeLines(notes, "release_notes.md")

cat("Done. DB size:", db_size_mb, "MB\n")
