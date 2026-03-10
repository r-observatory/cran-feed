#!/usr/bin/env Rscript
# Fetch version history for CRAN packages from the CRAN archive.
#
# Sources (in priority order):
#   1. CRAN src/contrib/ — single page listing all current tarballs with
#      version, date, and size. Downloaded once.
#   2. CRAN src/contrib/Archive/{package}/ — per-package listing of old
#      tarballs with version, date, and size.
#   3. GitHub CRAN mirror — last-resort fallback via blobless bare clone.
#      For R-* tags (R compatibility snapshots), reads DESCRIPTION DCF
#      to extract the actual package version.
#
# The script is incremental: it skips packages already in the DB.
# Use --limit=N to cap packages per run (default 500).
#
# Usage:
#   Rscript fetch-version-history.R feed.db
#   Rscript fetch-version-history.R feed.db --limit=1000
#   Rscript fetch-version-history.R feed.db --packages=ggplot2,dplyr,shiny

options(timeout = 300)

library(RSQLite)

`%||%` <- function(x, y) if (is.null(x) || length(x) == 0) y else x

# ---------------------------------------------------------------------------
# CLI arguments
# ---------------------------------------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
positional <- args[!grepl("^--", args)]
flags <- args[grepl("^--", args)]

db_path <- positional[1] %||% "feed.db"
limit <- 500L
specific_packages <- NULL

for (flag in flags) {
  if (grepl("^--limit=", flag)) {
    limit <- as.integer(sub("^--limit=", "", flag))
  } else if (grepl("^--packages=", flag)) {
    specific_packages <- strsplit(sub("^--packages=", "", flag), ",")[[1]]
  }
}

cat("=== Fetching CRAN version history ===\n")
cat("Database:", db_path, "\n")
cat("Limit:", limit, "packages per run\n")

# ---------------------------------------------------------------------------
# Connect and configure SQLite
# ---------------------------------------------------------------------------
con <- dbConnect(SQLite(), db_path)
on.exit(dbDisconnect(con), add = TRUE)

dbExecute(con, "PRAGMA journal_mode=WAL")
dbExecute(con, "PRAGMA synchronous=NORMAL")

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

# ---------------------------------------------------------------------------
# Helper: parse size string ("4.7K", "901K", "6.1M") to numeric KB
# ---------------------------------------------------------------------------
parse_size_kb <- function(s) {
  s <- trimws(s)
  if (grepl("M$", s)) return(as.numeric(sub("M$", "", s)) * 1024)
  if (grepl("K$", s)) return(as.numeric(sub("K$", "", s)))
  as.numeric(s) / 1024
}

# ---------------------------------------------------------------------------
# Helper: parse CRAN Apache directory listing into a data frame
# Works for both src/contrib/ and src/contrib/Archive/{pkg}/
# Each HTML row: <a href="pkg_ver.tar.gz">...</a></td><td>date</td><td>size</td>
# ---------------------------------------------------------------------------
parse_cran_listing <- function(html, pkg_filter = NULL) {
  if (!is.null(pkg_filter)) {
    pattern <- paste0(
      "(", pkg_filter, ")_([^\"]+)\\.tar\\.gz</a>",
      ".*?(\\d{4}-\\d{2}-\\d{2})\\s+\\d{2}:\\d{2}\\s*",
      "</td>\\s*<td[^>]*>\\s*([0-9.]+[KMG]?)")
  } else {
    pattern <- paste0(
      "([A-Za-z][A-Za-z0-9.]*[A-Za-z0-9])_([^\"]+)\\.tar\\.gz</a>",
      ".*?(\\d{4}-\\d{2}-\\d{2})\\s+\\d{2}:\\d{2}\\s*",
      "</td>\\s*<td[^>]*>\\s*([0-9.]+[KMG]?)")
  }

  packages <- character()
  versions <- character()
  dates    <- character()
  sizes    <- numeric()

  for (line in html) {
    parts <- regmatches(line, regexec(pattern, line, perl = TRUE))[[1]]
    if (length(parts) == 0) next
    packages <- c(packages, parts[2])
    versions <- c(versions, parts[3])
    dates    <- c(dates,    parts[4])
    sizes    <- c(sizes,    parse_size_kb(parts[5]))
  }

  if (length(packages) == 0) return(NULL)

  data.frame(
    package = packages, version = versions,
    published = dates, size_kb = round(sizes, 1),
    stringsAsFactors = FALSE
  )
}

# ---------------------------------------------------------------------------
# Source 1: Download main CRAN src/contrib/ listing (once for all packages)
# ---------------------------------------------------------------------------
cat("Downloading CRAN src/contrib/ listing ...\n")
contrib_file <- tempfile(fileext = ".html")
dl_ok <- tryCatch({
  download.file("https://cran.r-project.org/src/contrib/",
                contrib_file, quiet = TRUE, mode = "w")
  TRUE
}, error = function(e) {
  cat("  Download error:", conditionMessage(e), "\n")
  FALSE
})

current_versions <- NULL
if (dl_ok) {
  contrib_html <- readLines(contrib_file, warn = FALSE)
  current_versions <- parse_cran_listing(contrib_html)
  if (!is.null(current_versions)) {
    cat("  -> Parsed", nrow(current_versions), "current tarballs\n")
  }
}
unlink(contrib_file)

# ---------------------------------------------------------------------------
# Determine which packages to process
# ---------------------------------------------------------------------------
cran_packages <- if (!is.null(current_versions)) {
  sort(unique(current_versions$package))
} else {
  cat("  Falling back to available.packages() ...\n")
  sort(rownames(available.packages(repos = "https://cloud.r-project.org")))
}
cat("CRAN has", length(cran_packages), "packages\n")

already_done <- dbGetQuery(con,
  "SELECT DISTINCT package FROM package_version_history")$package

if (!is.null(specific_packages)) {
  todo <- intersect(specific_packages, cran_packages)
  cat("Processing", length(todo), "specified packages\n")
} else {
  todo <- setdiff(cran_packages, already_done)
  if (length(todo) > limit) {
    todo <- head(sort(todo), limit)
  }
  cat("To process:", length(todo), "(", length(already_done), "already done,",
      length(cran_packages) - length(already_done), "remaining)\n")
}

if (length(todo) == 0) {
  cat("Nothing to do.\n")
  quit(status = 0)
}

# Build lookup for current versions from contrib page
current_lookup <- list()
if (!is.null(current_versions)) {
  for (i in seq_len(nrow(current_versions))) {
    current_lookup[[current_versions$package[i]]] <- current_versions[i, ]
  }
}

# ---------------------------------------------------------------------------
# Source 2: CRAN archive per package
# ---------------------------------------------------------------------------
fetch_cran_archive <- function(pkg) {
  archive_url <- paste0(
    "https://cran.r-project.org/src/contrib/Archive/", pkg, "/")
  html <- tryCatch(
    suppressWarnings(readLines(archive_url, warn = FALSE)),
    error = function(e) NULL
  )
  if (is.null(html)) return(NULL)
  parse_cran_listing(html, pkg_filter = pkg)
}

# ---------------------------------------------------------------------------
# Source 3: GitHub CRAN mirror (last resort fallback)
# ---------------------------------------------------------------------------
clone_dir <- file.path(tempdir(), "cran-tags")
dir.create(clone_dir, showWarnings = FALSE, recursive = TRUE)

fetch_github_mirror <- function(pkg) {
  mirror_url <- paste0("https://github.com/cran/", pkg, ".git")
  repo_dir <- file.path(clone_dir, paste0(pkg, ".git"))

  clone_status <- system2("git",
    c("clone", "--bare", "--filter=blob:none", "--quiet",
      mirror_url, repo_dir),
    stdout = FALSE, stderr = FALSE, timeout = 30)
  if (clone_status != 0) return(NULL)
  on.exit(unlink(repo_dir, recursive = TRUE), add = TRUE)

  fmt <- "%(refname:short)\t%(creatordate:short)"
  output <- system(
    paste0("git -C ", shQuote(repo_dir),
           " for-each-ref --format=", shQuote(fmt),
           " refs/tags"),
    intern = TRUE)
  if (length(output) == 0) return(NULL)

  versions <- character()
  dates    <- character()

  for (line in output) {
    parts <- strsplit(line, "\t")[[1]]
    if (length(parts) < 2) next
    tag  <- parts[1]
    date <- parts[2]

    if (grepl("^R-", tag)) {
      # R compatibility snapshot — read DESCRIPTION for real version
      desc_url <- paste0(
        "https://raw.githubusercontent.com/cran/", pkg, "/", tag,
        "/DESCRIPTION")
      desc_text <- tryCatch(
        suppressWarnings(readLines(desc_url, warn = FALSE)),
        error = function(e) NULL)
      if (!is.null(desc_text)) {
        dcf <- tryCatch(
          read.dcf(textConnection(paste(desc_text, collapse = "\n"))),
          error = function(e) NULL)
        if (!is.null(dcf) && "Version" %in% colnames(dcf)) {
          real_ver <- dcf[1, "Version"]
          if (!real_ver %in% versions) {
            versions <- c(versions, real_ver)
            dates    <- c(dates, date)
          }
        }
      }
    } else {
      versions <- c(versions, tag)
      dates    <- c(dates, date)
    }
  }

  if (length(versions) == 0) return(NULL)

  data.frame(
    package = pkg, version = versions,
    published = dates, size_kb = NA_real_,
    stringsAsFactors = FALSE
  )
}

# ---------------------------------------------------------------------------
# Process packages in batches
# ---------------------------------------------------------------------------
batch_size <- 50
n_batches  <- ceiling(length(todo) / batch_size)
total_records <- 0L
src_counts <- c(contrib = 0L, archive = 0L, mirror = 0L)

for (b in seq_len(n_batches)) {
  start_idx <- (b - 1) * batch_size + 1
  end_idx   <- min(b * batch_size, length(todo))
  batch     <- todo[start_idx:end_idx]

  cat(sprintf("Batch %d/%d (%s ... %s)\n",
      b, n_batches, batch[1], batch[length(batch)]))

  batch_results <- list()

  for (pkg in batch) {
    rows <- list()

    # Current version from contrib page (already downloaded)
    cv <- current_lookup[[pkg]]
    if (!is.null(cv)) {
      cv$source <- "cran"
      rows[[1]] <- cv
      src_counts["contrib"] <- src_counts["contrib"] + 1L
    }

    # Archive for old versions
    archive <- tryCatch(fetch_cran_archive(pkg), error = function(e) NULL)
    if (!is.null(archive) && nrow(archive) > 0) {
      archive$source <- "cran"
      rows[[length(rows) + 1]] <- archive
      src_counts["archive"] <- src_counts["archive"] + 1L
    }

    # Combine
    if (length(rows) > 0) {
      result <- do.call(rbind, rows)
      result <- result[!duplicated(result$version), ]
    } else {
      # Nothing from CRAN — fallback to GitHub mirror
      result <- tryCatch(fetch_github_mirror(pkg), error = function(e) NULL)
      if (!is.null(result)) {
        result$source <- "github-mirror"
        src_counts["mirror"] <- src_counts["mirror"] + 1L
      }
    }

    if (!is.null(result) && nrow(result) > 0) {
      batch_results[[length(batch_results) + 1]] <- result
    }

    Sys.sleep(0.25)
  }

  if (length(batch_results) > 0) {
    batch_df <- do.call(rbind, batch_results)

    dbBegin(con)
    tryCatch({
      for (i in seq_len(nrow(batch_df))) {
        dbExecute(con,
          "INSERT OR REPLACE INTO package_version_history
           (package, version, published, size_kb, source)
           VALUES (?, ?, ?, ?, ?)",
          params = list(
            batch_df$package[i], batch_df$version[i],
            batch_df$published[i], batch_df$size_kb[i],
            batch_df$source[i]))
      }
      dbCommit(con)
      total_records <- total_records + nrow(batch_df)
      cat(sprintf("  -> %d version records for %d packages\n",
          nrow(batch_df), length(batch_results)))
    }, error = function(e) {
      dbRollback(con)
      cat(sprintf("  ERROR: %s\n", conditionMessage(e)))
    })
  }
}

unlink(clone_dir, recursive = TRUE)

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
dbExecute(con, "ANALYZE")
stats <- dbGetQuery(con,
  "SELECT COUNT(*) AS versions, COUNT(DISTINCT package) AS packages,
          MIN(published) AS earliest, MAX(published) AS latest
   FROM package_version_history")

cat(sprintf("\n=== Done ===\n"))
cat(sprintf("Total: %d versions across %d packages\n",
    stats$versions, stats$packages))
cat(sprintf("Date range: %s to %s\n",
    stats$earliest %||% "unknown", stats$latest %||% "unknown"))
cat(sprintf("New records this run: %d\n", total_records))
cat(sprintf("Sources: %d from contrib, %d with archive, %d from GitHub mirror\n",
    src_counts["contrib"], src_counts["archive"], src_counts["mirror"]))
