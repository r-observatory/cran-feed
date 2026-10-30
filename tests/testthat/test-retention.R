# Retention: a run may not publish less than the release it started from.

# A feed.db carrying the three accumulating tables, finalized the way update.R
# finalizes it (WAL checkpointed into the single file, connection closed).
build_retention_db <- function(events = c("2026-03-09T00:00:00Z", "2026-07-01T00:00:00Z"),
                               published = c("1997-10-08", "2026-07-01"),
                               crawled = c("pkgA", "pkgB")) {
  tmp <- tempfile(fileext = ".db")
  con <- dbConnect(SQLite(), tmp)
  dbExecute(con, "PRAGMA journal_mode=WAL")
  dbExecute(con, "
    CREATE TABLE package_versions (
      id INTEGER PRIMARY KEY AUTOINCREMENT, package TEXT NOT NULL, version TEXT,
      event_type TEXT NOT NULL, previous_version TEXT, removal_reason TEXT,
      detected_at TEXT NOT NULL, published TEXT)")
  ensure_version_history(con)
  dbExecute(con, "CREATE TABLE version_history_backfill (package TEXT PRIMARY KEY, crawled_at TEXT)")
  if (length(events)) {
    dbWriteTable(con, "package_versions", data.frame(
      package = paste0("p", seq_along(events)), version = "1.0", event_type = "new",
      detected_at = events, stringsAsFactors = FALSE), append = TRUE)
  }
  if (length(published)) {
    dbWriteTable(con, "package_version_history", data.frame(
      package = paste0("p", seq_along(published)), version = "1.0",
      published = published, stringsAsFactors = FALSE), append = TRUE)
  }
  if (length(crawled)) {
    dbWriteTable(con, "version_history_backfill", data.frame(
      package = crawled, crawled_at = "2026-08-15", stringsAsFactors = FALSE), append = TRUE)
  }
  dbExecute(con, "PRAGMA wal_checkpoint(TRUNCATE)")
  dbDisconnect(con)
  tmp
}

test_that("feed_coverage reports rows and reach for each accumulating table", {
  db <- build_retention_db()
  on.exit(unlink(db))

  cov <- feed_coverage(db)

  expect_setequal(names(cov), names(ACCUMULATING_TABLES))
  expect_equal(cov$package_versions,
               list(rows = 2L, min = "2026-03-09T00:00:00Z", max = "2026-07-01T00:00:00Z"))
  expect_equal(cov$package_version_history,
               list(rows = 2L, min = "1997-10-08", max = "2026-07-01"))
  # Nothing dates the backfill's record, so only its size is declared.
  expect_equal(cov$version_history_backfill, list(rows = 2L))
})

test_that("feed_coverage skips accumulating tables the database does not have", {
  tmp <- tempfile(fileext = ".db")
  on.exit(unlink(tmp))
  con <- dbConnect(SQLite(), tmp)
  dbExecute(con, "CREATE TABLE package_versions (id INTEGER PRIMARY KEY, detected_at TEXT NOT NULL)")
  dbDisconnect(con)

  expect_equal(names(feed_coverage(tmp)), "package_versions")
})

test_that("a genuine cold start has nothing to be checked against", {
  db <- build_retention_db(events = character(0), published = character(0), crawled = character(0))
  on.exit(unlink(db))

  expect_identical(retention_violations(feed_coverage(db), NULL), character(0))
})

test_that("a run that keeps or adds to everything passes", {
  before <- build_retention_db()
  after <- build_retention_db(events = c("2026-03-09T00:00:00Z", "2026-07-01T00:00:00Z",
                                         "2026-10-30T00:00:00Z"))
  on.exit(unlink(c(before, after)))

  prior <- list(coverage = feed_coverage(before))
  expect_identical(retention_violations(feed_coverage(after), prior), character(0))
})

test_that("shrinking, sliding forward and vanishing are each reported", {
  before <- build_retention_db()
  on.exit(unlink(before))
  prior <- list(coverage = feed_coverage(before))

  shrunk <- build_retention_db(events = "2026-03-09T00:00:00Z")
  slid <- build_retention_db(published = c("2001-01-01", "2026-07-01"))
  on.exit(unlink(c(shrunk, slid)), add = TRUE)

  expect_match(retention_violations(feed_coverage(shrunk), prior),
               "package_versions fell from 2 rows to 1", fixed = TRUE)
  expect_match(retention_violations(feed_coverage(slid), prior),
               "earliest package_version_history.published moved forward from 1997-10-08 to 2001-01-01",
               fixed = TRUE)

  gone <- feed_coverage(before)
  gone$version_history_backfill <- NULL
  expect_match(retention_violations(gone, prior),
               "version_history_backfill fell from 2 rows to 0", fixed = TRUE)
})

test_that("manifests without coverage are checked through their table counts", {
  # Every release before this change carries `tables` only. A run rebuilt from
  # empty must still be caught against them, not pass because `coverage` is
  # absent.
  db <- build_retention_db(events = rep("2026-10-30T00:00:00Z", 3L), published = "2026-10-30",
                           crawled = character(0))
  on.exit(unlink(db))
  prior <- list(tables = list(package_versions = 177517L, package_version_history = 173615L,
                              version_history_backfill = 23796L, packages = 25606L))

  v <- retention_violations(feed_coverage(db), prior)

  expect_length(v, 3L)
  expect_match(v[1], "package_versions fell from 177517 rows to 3", fixed = TRUE)
  expect_match(v[3], "version_history_backfill fell from 23796 rows to 0", fixed = TRUE)
})

test_that("publish_manifest writes coverage for the next run to be held to", {
  db <- build_retention_db()
  manifest <- tempfile(fileext = ".json")
  on.exit(unlink(c(db, manifest)))

  core <- publish_manifest(db, NULL, manifest)

  m <- jsonlite::fromJSON(manifest, simplifyVector = FALSE)
  expect_equal(m$coverage$package_versions$rows, 2L)
  expect_equal(m$coverage$version_history_backfill$rows, 2L)
  expect_false(m$complete)
  expect_equal(m$db_sha256, core$db_sha256)
  # What it writes is what the next run reads back as its prior.
  expect_identical(retention_violations(feed_coverage(db), read_prior_manifest(manifest)),
                   character(0))
})

test_that("publish_manifest refuses a run that lost history and writes nothing", {
  before <- build_retention_db()
  after <- build_retention_db(events = character(0))
  manifest <- tempfile(fileext = ".json")
  on.exit(unlink(c(before, after, manifest)))

  expect_error(
    capture.output(publish_manifest(after, list(coverage = feed_coverage(before)), manifest)),
    "retention check failed")
  expect_false(file.exists(manifest))
})

test_that("read_prior_manifest returns NULL when absent and fails when unreadable", {
  missing <- tempfile(fileext = ".json")
  expect_null(read_prior_manifest(missing))

  broken <- tempfile(fileext = ".json")
  on.exit(unlink(broken))
  writeLines("{not json", broken)
  expect_error(read_prior_manifest(broken))
})
