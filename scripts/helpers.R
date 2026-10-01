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
# The live CRAN list
# ---------------------------------------------------------------------------

#' CRAN's PACKAGES index as available.packages() reads it, with only the
#' duplicates filter. The default filters would drop OS_type: windows packages
#' and any package needing a newer R than this runner, and the next run would
#' record them as removed. A Recommended package listed twice keeps one row.
cran_available <- function(repos = "https://cloud.r-project.org") {
  utils::available.packages(repos = repos, type = "source", filters = "duplicates")
}

# ---------------------------------------------------------------------------
# package_versions events
# ---------------------------------------------------------------------------

#' Delete "new" events that have an earlier row for the same (package, version).
#' The tracker's first run and the later history seed both recorded some
#' versions; a genuine "new" is the first row for its pair. Real releases and
#' "removed" events are untouched. Returns the number of rows deleted.
collapse_duplicate_new_events <- function(con) {
  DBI::dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_pv_pkg_ver ON package_versions (package, version)")
  DBI::dbExecute(con, "
    DELETE FROM package_versions
     WHERE event_type = 'new'
       AND EXISTS (
         SELECT 1 FROM package_versions p2
          WHERE p2.package = package_versions.package
            AND p2.version = package_versions.version
            AND p2.detected_at < package_versions.detected_at)")
}

#' Which of `pkgs` still have a "new" event detected at `detected_at`, that is,
#' which were really new this run once the collapse has run.
surviving_new <- function(con, pkgs, detected_at) {
  if (length(pkgs) == 0L) return(character(0))
  kept <- DBI::dbGetQuery(con, "
    SELECT DISTINCT package FROM package_versions
     WHERE event_type = 'new' AND detected_at = ?", params = list(detected_at))$package
  intersect(pkgs, kept)
}

#' Seed events for packages listed for the first time that are not new to CRAN.
#'
#' `pkgs` maps package to its current CRAN version (a named character vector).
#' A package with no package_versions row whose first release in
#' package_version_history is more than `min_age_days` before `today` was
#' hidden, not new: a listing filter kept it out. Its history becomes events
#' the way seed-versions.R made them, first release "new" and later ones
#' "updated", each dated to its publication day. A current version the history
#' does not have yet is added as "updated" from the newest one it has.
#' `read_archive` first completes the history of a package never walked.
#' Returns the seeded package names, which the caller must not record as new.
seed_events_for_first_sighted <- function(con, pkgs, today, min_age_days = 30L,
                                          read_archive = read_cran_archive) {
  pkgs <- pkgs[!is.na(names(pkgs)) & nzchar(names(pkgs))]
  if (length(pkgs) == 0L || !DBI::dbExistsTable(con, "package_version_history")) {
    return(character(0))
  }
  seen <- DBI::dbGetQuery(con, "SELECT DISTINCT package FROM package_versions")$package
  pkgs <- pkgs[!(names(pkgs) %in% seen)]
  if (length(pkgs) == 0L) return(character(0))
  walk_unwalked_archives(con, names(pkgs), read_archive)
  day    <- format(as.Date(today), "%Y-%m-%d")
  cutoff <- format(as.Date(today) - min_age_days, "%Y-%m-%d")
  seeded <- character(0)
  DBI::dbBegin(con)
  tryCatch({
    for (p in names(pkgs)) {
      h <- DBI::dbGetQuery(con, "
        SELECT version, published FROM package_version_history
         WHERE package = ? AND published IS NOT NULL AND published != ''
         ORDER BY published ASC, version ASC", params = list(p))
      if (nrow(h) == 0L || min(h$published) >= cutoff) next
      ev <- data.frame(
        version = h$version, previous_version = c(NA_character_, utils::head(h$version, -1L)),
        detected_at = paste0(h$published, "T00:00:00Z"), published = h$published,
        stringsAsFactors = FALSE)
      current <- unname(pkgs[[p]])
      if (!is.na(current) && !(current %in% h$version)) {
        ev <- rbind(ev, data.frame(version = current, previous_version = utils::tail(h$version, 1L),
                                   detected_at = paste0(day, "T00:00:00Z"),
                                   published = NA_character_, stringsAsFactors = FALSE))
      }
      ev$event_type <- ifelse(is.na(ev$previous_version), "new", "updated")
      DBI::dbExecute(con, "
        INSERT INTO package_versions (package, version, event_type, previous_version,
                                      removal_reason, detected_at, published)
        VALUES (?, ?, ?, ?, NULL, ?, ?)",
        params = list(rep(p, nrow(ev)), ev$version, ev$event_type, ev$previous_version,
                      ev$detected_at, ev$published))
      seeded <- c(seeded, p)
    }
    DBI::dbCommit(con)
  }, error = function(e) {
    DBI::dbRollback(con)
    stop(e)
  })
  seeded
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

#' The key under which version_history_backfill_state records the seed.
BACKFILL_SEEDED_KEY <- "seeded_at"

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
#' moment of migration, and it must run exactly once. Whether it has run is
#' recorded as its own fact, in version_history_backfill_state, because a row
#' count cannot tell "nobody has seeded yet" from "the seed ran and the table it
#' copies was empty". Those two look identical after a from-scratch rebuild,
#' which is a live possibility: update.yml's "Download previous database" step is
#' continue-on-error, and a failed release-asset download is exactly what reset
#' cran-queue and cost it 323k snapshots. With no marker the next cycle would
#' re-seed from a package_version_history the refresh had meanwhile filled with
#' every package on CRAN, mark all of them crawled, and leave
#' archive_backfill_todo() returning nothing for good, silently.
#'
#' A version_history_backfill that already holds packages is itself evidence
#' that a seed or a crawl happened before the marker existed, so it takes the
#' marker rather than a second seed.
#'
#' Returns TRUE (invisibly) if this call performed the seed.
ensure_backfill_state <- function(con,
                                  at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")) {
  DBI::dbExecute(con, "
    CREATE TABLE IF NOT EXISTS version_history_backfill (
      package    TEXT PRIMARY KEY,
      crawled_at TEXT)")
  DBI::dbExecute(con, "
    CREATE TABLE IF NOT EXISTS version_history_backfill_state (
      key   TEXT PRIMARY KEY,
      value TEXT)")

  marked <- DBI::dbGetQuery(con, "
    SELECT COUNT(*) AS n FROM version_history_backfill_state WHERE key = ?",
    params = list(BACKFILL_SEEDED_KEY))$n
  if (marked > 0) return(invisible(FALSE))

  known <- DBI::dbGetQuery(con, "
    SELECT COUNT(*) AS n FROM version_history_backfill")$n
  seeded <- FALSE
  if (known == 0 && DBI::dbExistsTable(con, "package_version_history")) {
    DBI::dbExecute(con, "
      INSERT OR IGNORE INTO version_history_backfill (package, crawled_at)
      SELECT DISTINCT package, NULL FROM package_version_history")
    seeded <- TRUE
  }
  DBI::dbExecute(con, "
    INSERT OR REPLACE INTO version_history_backfill_state (key, value) VALUES (?, ?)",
    params = list(BACKFILL_SEEDED_KEY, at))
  invisible(seeded)
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

#' Seconds to wait before each further attempt at an archive listing.
ARCHIVE_RETRY_WAITS_S <- c(5, 15, 45)

#' One package's archived releases as CRAN's /src/contrib/Archive/<pkg>/ index
#' lists them, or NULL when it has none (a 404, or a page with no tarball rows).
#' Any other failure is tried again after each of `waits`, one more attempt
#' than there are waits, and is an error once they are used up, so a listing
#' that could not be read is never taken for a package with no earlier release.
#' `read_lines` and `sleep` are for the tests.
read_cran_archive <- function(pkg,
                              url = paste0("https://cran.r-project.org/src/contrib/Archive/",
                                           pkg, "/"),
                              read_lines = readLines,
                              waits = ARCHIVE_RETRY_WAITS_S, sleep = Sys.sleep) {
  attempts <- length(waits) + 1L
  for (i in seq_len(attempts)) {
    why <- ""
    html <- withCallingHandlers(
      tryCatch(read_lines(url, warn = FALSE), error = function(e) {
        if (!nzchar(why)) why <<- conditionMessage(e)
        NULL
      }),
      warning = function(w) {
        why <<- conditionMessage(w)
        invokeRestart("muffleWarning")
      })
    if (!is.null(html)) return(parse_cran_listing(html, pkg_filter = pkg))
    if (grepl("'404 Not Found'", why, fixed = TRUE)) return(NULL)
    if (i < attempts) {
      message("Could not read ", url, ": ", why, ". Trying again in ", waits[[i]], "s.")
      sleep(waits[[i]])
    }
  }
  stop("Could not read ", url, " after ", attempts,
       if (attempts == 1L) " attempt: " else " attempts: ", why, call. = FALSE)
}

#' Walk the archive of each of `pkgs` the backfill has not walked: its archived
#' releases join package_version_history and it is marked walked, as the
#' backfill would have done. An error from `read_archive` is passed on.
#' Returns the packages walked.
walk_unwalked_archives <- function(con, pkgs, read_archive = read_cran_archive) {
  todo <- setdiff(pkgs, backfill_crawled(con))
  for (p in todo) {
    refresh_current_versions(con, read_archive(p))
    mark_backfilled(con, p)
  }
  invisible(todo)
}
