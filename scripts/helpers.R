# scripts/helpers.R: testable helpers for the cran-feed pipeline.
#
# Two groups:
#
#   * Release manifest (manifest.json), shipping alongside feed.db. No side
#     effects beyond writing the file the caller names; sourced by
#     scripts/update.R after the database is finalized and its connection
#     closed.
#   * package_version_history parsing and refresh, sourced by both
#     scripts/update.R (the cheap current-version refresh that runs every cycle)
#     and scripts/fetch-version-history.R (the archive backfill). They live here
#     rather than inside the flat scripts so tests/testthat can reach them,
#     which is the same reason the manifest helpers do.

#' Compute the lowercase hex SHA-256 of a file's exact on-disk bytes.
#'
#' Uses whatever the runner already provides, in preference order:
#'   1. digest  package        (if installed)
#'   2. openssl package        (if installed)
#'   3. sha256sum (coreutils)  - present on the ubuntu-latest CI runner
#'   4. shasum -a 256 (BSD)    - macOS/local fallback
#' No heavy dependency is declared: on CI (which installs RSQLite, DBI,
#' jsonlite, testthat) the coreutils `sha256sum` path is used unless a sibling
#' package pulls in digest/openssl, in which case that path wins automatically.
file_sha256 <- function(path) {
  if (requireNamespace("digest", quietly = TRUE)) {
    return(tolower(digest::digest(file = path, algo = "sha256")))
  }
  if (requireNamespace("openssl", quietly = TRUE)) {
    con <- file(path, open = "rb")
    on.exit(close(con), add = TRUE)
    return(tolower(as.character(openssl::sha256(con))))
  }
  sha_tool <- Sys.which("sha256sum")
  if (nzchar(sha_tool)) {
    out <- system2(sha_tool, shQuote(path), stdout = TRUE)
    return(tolower(sub("\\s.*$", "", out[1])))
  }
  shasum_tool <- Sys.which("shasum")
  if (nzchar(shasum_tool)) {
    out <- system2(shasum_tool, c("-a", "256", shQuote(path)), stdout = TRUE)
    return(tolower(sub("\\s.*$", "", out[1])))
  }
  stop("No SHA-256 backend found (need one of: digest, openssl, sha256sum, shasum)")
}

#' Whether a SQLite database file contains a table with the given name.
#'
#' Used by the update.R call site to derive the manifest's `complete` field
#' honestly instead of hardcoding it: `package_version_history` is seeded
#' incrementally (and possibly only partially, via a manual `--limit`-capped
#' run) by seed-version-history.yml, not by update.R. If a previous release's
#' feed.db carrying that table is downloaded and carried forward, update.R has
#' no way to verify it is fully seeded, so its presence must drive `complete`
#' to FALSE rather than being ignored.
db_has_table <- function(db_path, table_name) {
  con <- DBI::dbConnect(RSQLite::SQLite(), db_path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  nrow(DBI::dbGetQuery(con, "
    SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
    params = list(table_name))) > 0
}

#' Build the integrity / completeness core describing a finalized SQLite file.
#'
#' Returns a named list of TOP-LEVEL manifest fields computed from the exact
#' on-disk bytes of `db_path` (call this only after the file is finalized and
#' its DB connection closed, so no open handle or -wal/-shm sidecar skews the
#' size/hash):
#'   * db_filename - basename of the file
#'   * db_bytes    - byte size of the file as a double. Deliberately NOT cast
#'                   to integer: R's integer range is 32-bit and overflows to
#'                   NA (serialized as the string "NA") for files >= ~2 GiB.
#'                   As a double it always serializes as a JSON number.
#'   * db_sha256   - lowercase hex sha256 of the file's exact bytes
#'   * tables      - named list mapping each user table to its row count
#'   * complete    - passed through by the caller. complete = the DB holds the
#'                   full, non-partial dataset (full-not-partial), NOT freshness:
#'                   freshness is tracked separately via generated_at and the
#'                   db_sha256 fingerprint. A pipeline with a genuine
#'                   partial/bootstrap state would DERIVE this instead of
#'                   hardcoding it; the caller documents its choice.
#' Lets a downstream merge content-verify the asset it pulls and confirm the
#' expected tables/rows are present.
summary_integrity_core <- function(db_path, complete = TRUE) {
  stopifnot(file.exists(db_path))

  con <- DBI::dbConnect(RSQLite::SQLite(), db_path)
  tables <- tryCatch({
    tbl_names <- DBI::dbGetQuery(con, "
      SELECT name FROM sqlite_master
       WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
       ORDER BY name")$name

    stats::setNames(
      lapply(tbl_names, function(t) {
        DBI::dbGetQuery(con, sprintf('SELECT count(*) AS n FROM "%s"', t))$n
      }),
      tbl_names
    )
  }, finally = DBI::dbDisconnect(con))

  # db_bytes/db_sha256 read the raw on-disk file only after the connection
  # above is closed, so no open handle or journal file skews the hash/size.
  list(
    db_filename = basename(db_path),
    db_bytes    = file.size(db_path),
    db_sha256   = file_sha256(db_path),
    tables      = tables,
    complete    = complete
  )
}

#' Write the release manifest.json describing the finalized primary DB.
#'
#' Top-level fields: generated_at plus the integrity/completeness core produced
#' by summary_integrity_core(). `core` is merged as TOP-LEVEL fields (not nested)
#' so a downstream merge can read db_filename/db_bytes/db_sha256/tables/complete
#' directly. generated_at records freshness independently of `complete`.
write_manifest <- function(path, core,
                           generated_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ",
                                                 tz = "UTC")) {
  obj <- c(list(generated_at = generated_at), core)
  json <- jsonlite::toJSON(obj, auto_unbox = TRUE, pretty = TRUE, null = "null")
  writeLines(json, path)
  invisible(path)
}

# ---------------------------------------------------------------------------
# package_version_history
#
# This table is the org's only record of a CRAN package's COMPRESSED tarball
# size, which is what the viewer prints as "Download size". Nothing downstream
# can reconstruct it: cran-code-metrics measures uncompressed source bytes, a
# different quantity (median ~2545x size_kb across the keys the two share, where
# a unit conversion would be a flat 1024x). So the table has to stay, and it has
# to stay current.
#
# It has not been. It froze on 2026-03-10 for two compounding reasons: its only
# writer was seed-version-history.yml, which is workflow_dispatch with no
# schedule, AND the fetcher skips work at PACKAGE granularity, so a package that
# has any row at all is never revisited no matter how often that button is
# pressed. The split below separates the two kinds of work:
#
#   refresh_current_versions()  cheap, complete, every cycle. One HTTP request
#                               to /src/contrib/ already yields the version,
#                               date and size of the current tarball for every
#                               package CRAN ships. Applied unconditionally.
#   archive_backfill_todo()     expensive, incremental, occasional. Walking
#                               /src/contrib/Archive/<pkg>/ is a request plus a
#                               rate-limit sleep per package. Old releases are
#                               immutable, so skipping already-crawled packages
#                               is right here and only here.
# ---------------------------------------------------------------------------

#' Parse a size as CRAN's directory index writes it ("4.7K", "901K", "6.1M")
#' into numeric kilobytes. A bare number is bytes.
parse_size_kb <- function(s) {
  s <- trimws(s)
  if (grepl("M$", s)) return(as.numeric(sub("M$", "", s)) * 1024)
  if (grepl("K$", s)) return(as.numeric(sub("K$", "", s)))
  as.numeric(s) / 1024
}

#' Parse a CRAN Apache directory listing into a data frame of
#' package / version / published / size_kb.
#'
#' Works for both /src/contrib/ (every current tarball) and
#' /src/contrib/Archive/<pkg>/ (one package's old tarballs); pass `pkg_filter`
#' for the latter so a package whose name is a prefix of another's cannot bleed
#' in. Returns NULL when the page holds no tarball rows, which is how a fetch
#' that returned an error page is told apart from one that returned nothing.
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

  packages <- character(); versions <- character()
  dates    <- character(); sizes    <- numeric()

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
    stringsAsFactors = FALSE)
}

#' Create package_version_history and its indexes if they are not there yet.
ensure_version_history <- function(con) {
  DBI::dbExecute(con, "
    CREATE TABLE IF NOT EXISTS package_version_history (
      package   TEXT NOT NULL,
      version   TEXT NOT NULL,
      published TEXT,
      size_kb   REAL,
      source    TEXT DEFAULT 'cran',
      PRIMARY KEY (package, version))")
  DBI::dbExecute(con, "
    CREATE INDEX IF NOT EXISTS idx_pvh_package   ON package_version_history (package)")
  DBI::dbExecute(con, "
    CREATE INDEX IF NOT EXISTS idx_pvh_published ON package_version_history (published)")
  invisible(TRUE)
}

#' Fold a /src/contrib/ snapshot into package_version_history.
#'
#' Applied to EVERY package in the snapshot, including ones already in the
#' table: that is the whole point, since the current release of a
#' long-established package is exactly the row the per-package archive skip can
#' never add. Keyed on (package, version), so it inserts the releases that are
#' new and corrects the size of ones already recorded, and touches nothing else
#' - old archive rows are left exactly as they were.
#'
#' An empty or NULL snapshot writes nothing and returns 0. A failed download
#' must never be read as "CRAN has no packages": there is no delete here, so the
#' worst case of a bad fetch is a cycle that changes nothing.
#'
#' Returns the number of rows written.
refresh_current_versions <- function(con, current) {
  if (is.null(current) || nrow(current) == 0) return(0L)
  ensure_version_history(con)

  src <- if ("source" %in% names(current)) current$source else rep("cran", nrow(current))

  DBI::dbBegin(con)
  n <- tryCatch({
    DBI::dbExecute(con, "
      INSERT OR REPLACE INTO package_version_history
        (package, version, published, size_kb, source)
      VALUES (?, ?, ?, ?, ?)",
      params = list(current$package, current$version,
                    current$published, current$size_kb, src))
    DBI::dbCommit(con)
    nrow(current)
  }, error = function(e) {
    DBI::dbRollback(con)
    stop(e)
  })
  as.integer(n)
}

#' Which packages the expensive archive crawl should visit this run.
#'
#' Skips packages already represented in the table (their old releases are
#' immutable, so one crawl is enough) and caps the run. Sorting before the cap
#' makes successive runs walk the alphabet deterministically instead of
#' re-drawing an arbitrary subset.
archive_backfill_todo <- function(cran_packages, already_done, limit = 500L) {
  todo <- setdiff(cran_packages, already_done)
  todo <- sort(todo)
  if (length(todo) > limit) todo <- utils::head(todo, limit)
  todo
}

#' Create the archive-backfill state table, seeding it once on first sight.
#'
#' "Which packages have I walked the archive for" used to be inferred from
#' "which packages have a row in package_version_history". That inference held
#' only while the backfill was the table's sole writer. update.R now records the
#' current release of every package on CRAN every cycle, so the inference would
#' shortly report that all ~33k packages are crawled when only ~23k ever were,
#' and the backfill would stall silently having missed the rest.
#'
#' The seed reproduces exactly what the old inference would have said at the
#' moment of migration, and only when the table is empty, so a later call cannot
#' re-widen it from a package_version_history the refresh has since grown.
ensure_backfill_state <- function(con) {
  DBI::dbExecute(con, "
    CREATE TABLE IF NOT EXISTS version_history_backfill (
      package    TEXT PRIMARY KEY,
      crawled_at TEXT)")
  seeded <- DBI::dbGetQuery(con, "
    SELECT COUNT(*) AS n FROM version_history_backfill")$n
  if (seeded == 0 && DBI::dbExistsTable(con, "package_version_history")) {
    DBI::dbExecute(con, "
      INSERT OR IGNORE INTO version_history_backfill (package, crawled_at)
      SELECT DISTINCT package, NULL FROM package_version_history")
  }
  invisible(TRUE)
}

#' Packages whose CRAN archive has been walked.
backfill_crawled <- function(con) {
  if (!DBI::dbExistsTable(con, "version_history_backfill")) return(character())
  DBI::dbGetQuery(con, "SELECT package FROM version_history_backfill")$package
}

#' Record that these packages' archives have now been walked.
#'
#' Called for every package the crawl visited, including ones that turned out to
#' have no archive at all: the expensive part is the request, and a package with
#' nothing behind it is exactly the one that would otherwise be re-requested
#' every run forever.
mark_backfilled <- function(con, packages,
                            at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")) {
  packages <- unique(packages[!is.na(packages) & nzchar(packages)])
  if (length(packages) == 0) return(0L)
  ensure_backfill_state(con)
  DBI::dbExecute(con, "
    INSERT OR REPLACE INTO version_history_backfill (package, crawled_at)
    VALUES (?, ?)",
    params = list(packages, rep(at, length(packages))))
  length(packages)
}
