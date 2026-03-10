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

invisible(dbExecute(con, "PRAGMA journal_mode=WAL"))
invisible(dbExecute(con, "PRAGMA synchronous=NORMAL"))

# ---------------------------------------------------------------------------
# Create tables if they don't exist
# ---------------------------------------------------------------------------
invisible(dbExecute(con, "
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
)"))

invisible(dbExecute(con, "
CREATE TABLE IF NOT EXISTS package_versions (
  id                INTEGER PRIMARY KEY AUTOINCREMENT,
  package           TEXT NOT NULL,
  version           TEXT,
  event_type        TEXT NOT NULL,
  previous_version  TEXT,
  removal_reason    TEXT,
  detected_at       TEXT NOT NULL,
  published         TEXT
)"))

invisible(dbExecute(con, "
CREATE INDEX IF NOT EXISTS idx_pv_package  ON package_versions (package)"))
invisible(dbExecute(con, "
CREATE INDEX IF NOT EXISTS idx_pv_detected ON package_versions (detected_at)"))
invisible(dbExecute(con, "
CREATE INDEX IF NOT EXISTS idx_pv_event    ON package_versions (event_type)"))

invisible(dbExecute(con, "
CREATE TABLE IF NOT EXISTS reverse_dependencies (
  package     TEXT NOT NULL,
  rev_package TEXT NOT NULL,
  type        TEXT NOT NULL,
  PRIMARY KEY (package, rev_package, type)
)"))

invisible(dbExecute(con, "
CREATE INDEX IF NOT EXISTS idx_revdep_pkg ON reverse_dependencies (package)"))

# ---------------------------------------------------------------------------
# Fetch current CRAN state (with error handling)
# ---------------------------------------------------------------------------
cat("Fetching available.packages() ...\n")
ap <- tryCatch(
  available.packages(repos = "https://cloud.r-project.org"),
  error = function(e) {
    message("ERROR: Failed to fetch available.packages(): ", conditionMessage(e))
    quit(status = 1)
  }
)
cat("  ->", nrow(ap), "packages from available.packages()\n")

cat("Fetching tools::CRAN_package_db() ...\n")
cran_db <- tryCatch(
  tools::CRAN_package_db(),
  error = function(e) {
    message("ERROR: Failed to fetch tools::CRAN_package_db(): ", conditionMessage(e))
    quit(status = 1)
  }
)
cat("  ->", nrow(cran_db), "packages from CRAN_package_db()\n")

# CRAN_package_db() can return duplicate rows for recommended packages;
# keep only the first occurrence of each package name.
cran_db <- cran_db[!duplicated(cran_db$Package), ]
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
# Helper: parse maintainer field into name and email (vectorized)
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
# Record events in package_versions (append-only, wrapped in transaction)
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

dbBegin(con)
tryCatch({
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

  dbCommit(con)
}, error = function(e) {
  dbRollback(con)
  stop(e)
})

cat("Events recorded.\n")

# ---------------------------------------------------------------------------
# Rebuild packages table from current state (vectorized construction)
# ---------------------------------------------------------------------------
cat("Rebuilding packages table ...\n")

# Safe vectorized accessor for cran_db columns
cran_col <- function(pkgs, col) {
  if (!(col %in% colnames(cran_db))) return(rep(NA_character_, length(pkgs)))
  idx <- match(pkgs, rownames(cran_db))
  vals <- rep(NA_character_, length(pkgs))
  found <- !is.na(idx)
  vals[found] <- as.character(cran_db[[col]][idx[found]])
  vals
}

# Parse all maintainers at once
raw_maintainers <- cran_col(current_names, "Maintainer")
maint_names  <- character(length(current_names))
maint_emails <- character(length(current_names))
for (k in seq_along(raw_maintainers)) {
  parsed <- parse_maintainer(raw_maintainers[k])
  maint_names[k]  <- parsed$name
  maint_emails[k] <- parsed$email
}

pkg_df <- data.frame(
  name              = current_names,
  version           = as.character(current_versions[current_names]),
  title             = cran_col(current_names, "Title"),
  description       = cran_col(current_names, "Description"),
  maintainer        = maint_names,
  maintainer_email  = maint_emails,
  license           = cran_col(current_names, "License"),
  depends           = cran_col(current_names, "Depends"),
  imports           = cran_col(current_names, "Imports"),
  suggests          = cran_col(current_names, "Suggests"),
  linking_to        = cran_col(current_names, "LinkingTo"),
  needs_compilation = as.character(ap[current_names, "NeedsCompilation"]),
  published         = cran_col(current_names, "Published"),
  cran_url          = paste0("https://CRAN.R-project.org/package=", current_names),
  first_published   = NA_character_,
  is_archived       = 0L,
  updated_at        = now,
  stringsAsFactors  = FALSE
)

dbBegin(con)
tryCatch({
  dbExecute(con, "DELETE FROM packages")
  dbWriteTable(con, "packages", pkg_df, append = TRUE)
  dbCommit(con)
}, error = function(e) {
  dbRollback(con)
  stop(e)
})
cat("  -> Inserted", nrow(pkg_df), "packages.\n")

# ---------------------------------------------------------------------------
# Set first_published from earliest "new" event in package_versions
# ---------------------------------------------------------------------------
cat("Setting first_published dates ...\n")
invisible(dbExecute(con, "
  UPDATE packages
  SET first_published = (
    SELECT MIN(detected_at)
    FROM package_versions
    WHERE package_versions.package = packages.name
      AND package_versions.event_type = 'new'
  )
"))

# ---------------------------------------------------------------------------
# Rebuild reverse_dependencies (pre-allocated vectors, no O(n^2) growth)
# ---------------------------------------------------------------------------
cat("Rebuilding reverse_dependencies ...\n")

dep_types <- c("Depends", "Imports", "Suggests", "LinkingTo")
dep_labels <- c("depends", "imports", "suggests", "linking_to")

pkg_vec  <- character(0)
rev_vec  <- character(0)
type_vec <- character(0)

for (i in seq_along(dep_types)) {
  col <- dep_types[i]
  label <- dep_labels[i]
  if (!(col %in% colnames(ap))) next
  raw_col <- ap[, col]
  for (j in seq_along(current_names)) {
    raw <- raw_col[j]
    if (is.na(raw) || raw == "") next
    deps <- trimws(unlist(strsplit(raw, ",")))
    deps <- sub("\\s*\\(.*\\)\\s*$", "", deps)
    deps <- deps[deps != "" & deps != "R"]
    n <- length(deps)
    if (n > 0) {
      pkg_vec  <- c(pkg_vec, deps)
      rev_vec  <- c(rev_vec, rep(current_names[j], n))
      type_vec <- c(type_vec, rep(label, n))
    }
  }
}

dbBegin(con)
tryCatch({
  dbExecute(con, "DELETE FROM reverse_dependencies")
  if (length(pkg_vec) > 0) {
    revdep_df <- data.frame(package = pkg_vec, rev_package = rev_vec, type = type_vec,
                            stringsAsFactors = FALSE)
    # Remove duplicates (composite PK)
    revdep_df <- revdep_df[!duplicated(revdep_df), ]
    dbWriteTable(con, "reverse_dependencies", revdep_df, append = TRUE)
    cat("  -> Inserted", nrow(revdep_df), "reverse dependency rows.\n")
  } else {
    cat("  -> No reverse dependencies found.\n")
  }
  dbCommit(con)
}, error = function(e) {
  dbRollback(con)
  stop(e)
})

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
