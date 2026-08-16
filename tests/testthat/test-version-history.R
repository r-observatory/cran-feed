# package_version_history: what keeps it current.
#
# The table froze at 2026-03-10. The obvious reading is "nobody pressed the
# workflow_dispatch button", but that is only half of it: the fetcher decides
# what to do by PACKAGE, not by (package, version) -
#
#   todo <- setdiff(cran_packages, already_done)
#
# so once a package has any row at all it is never looked at again. Pressing the
# button daily for five months would still not have recorded a single new
# release of an existing package. These tests pin both halves: the per-package
# skip is right for the expensive archive crawl and wrong for everything else,
# and the cheap src/contrib snapshot has to be applied unconditionally.

test_that("parse_size_kb reads the units CRAN's listing actually uses", {
  expect_equal(parse_size_kb("204K"), 204)
  expect_equal(parse_size_kb("6.1M"), 6.1 * 1024)
  expect_equal(parse_size_kb("1.5M"), 1536)
  # A bare number is bytes.
  expect_equal(parse_size_kb("2048"), 2)
  expect_equal(parse_size_kb("  901K  "), 901)
})

# One row of the real Apache autoindex CRAN serves at /src/contrib/.
contrib_line <- function(pkg, ver, date, size) {
  sprintf(paste0('<tr><td><a href="%s_%s.tar.gz">%s_%s.tar.gz</a></td>',
                 '<td align="right">%s 09:41  </td>',
                 '<td align="right">%s</td><td>&nbsp;</td></tr>'),
          pkg, ver, pkg, ver, date, size)
}

test_that("parse_cran_listing pulls package, version, date and size off a listing", {
  html <- c(contrib_line("abc", "2.2.2", "2024-06-01", "204K"),
            contrib_line("AATtools", "0.0.3", "2025-02-01", "247K"),
            contrib_line("bigpkg", "1.0", "2026-01-01", "6.1M"))
  got <- parse_cran_listing(html)

  expect_equal(nrow(got), 3L)
  expect_equal(got$package, c("abc", "AATtools", "bigpkg"))
  expect_equal(got$version, c("2.2.2", "0.0.3", "1.0"))
  expect_equal(got$published, c("2024-06-01", "2025-02-01", "2026-01-01"))
  expect_equal(got$size_kb, c(204, 247, round(6.1 * 1024, 1)))
})

test_that("parse_cran_listing returns NULL when nothing on the page matches", {
  expect_null(parse_cran_listing(c("<html>", "<h1>Index of /src/contrib</h1>", "</html>")))
})

# A feed.db carrying the state the real one is in: abc was crawled long ago and
# is recorded up to 2.2.1, AATtools up to 0.0.2. Both have since released.
vh_db <- function() {
  tmp <- tempfile(fileext = ".db")
  con <- DBI::dbConnect(RSQLite::SQLite(), tmp)
  ensure_version_history(con)
  DBI::dbExecute(con, "
    INSERT INTO package_version_history (package, version, published, size_kb, source) VALUES
      ('abc','2.2.0','2022-05-01',176.0,'cran'),
      ('abc','2.2.1','2023-01-01',180.0,'cran'),
      ('AATtools','0.0.1','2024-11-01',201.4,'cran'),
      ('AATtools','0.0.2','2025-01-15',233.9,'cran')")
  DBI::dbDisconnect(con)
  tmp
}

test_that("the archive backfill still skips packages it has already crawled", {
  # The per-package skip is CORRECT here: re-walking src/contrib/Archive/<pkg>/
  # is one HTTP request per package plus a 0.25s sleep, and old releases are
  # immutable, so there is nothing to gain by doing it twice.
  todo <- archive_backfill_todo(
    cran_packages = c("abc", "AATtools", "brandNew", "alsoNew"),
    already_done  = c("abc", "AATtools"),
    limit         = 500L)
  expect_setequal(todo, c("alsoNew", "brandNew"))
})

test_that("the archive backfill honours its per-run cap deterministically", {
  todo <- archive_backfill_todo(
    cran_packages = c("d", "c", "b", "a"), already_done = character(), limit = 2L)
  expect_equal(todo, c("a", "b"))
})

test_that("refreshing current versions records a new release of a KNOWN package", {
  # The regression the freeze is made of. abc 2.2.2 and AATtools 0.0.3 are on
  # CRAN today; both packages are already in the table, so the archive path will
  # never revisit them. The contrib snapshot must be applied regardless.
  db <- vh_db()
  on.exit(unlink(db))
  con <- DBI::dbConnect(RSQLite::SQLite(), db)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  current <- parse_cran_listing(c(contrib_line("abc", "2.2.2", "2024-06-01", "204K"),
                                  contrib_line("AATtools", "0.0.3", "2025-02-01", "247K")))
  n <- refresh_current_versions(con, current)

  expect_equal(n, 2L)
  got <- DBI::dbGetQuery(con, "
    SELECT version, size_kb FROM package_version_history
     WHERE package = 'abc' ORDER BY version")
  expect_equal(got$version, c("2.2.0", "2.2.1", "2.2.2"))
  expect_equal(got$size_kb[3], 204)

  aat <- DBI::dbGetQuery(con, "
    SELECT size_kb FROM package_version_history
     WHERE package = 'AATtools' AND version = '0.0.3'")
  expect_equal(aat$size_kb, 247)
})

test_that("refreshing leaves already-recorded archive releases alone", {
  db <- vh_db()
  on.exit(unlink(db))
  con <- DBI::dbConnect(RSQLite::SQLite(), db)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  before <- DBI::dbGetQuery(con, "
    SELECT package, version, published, size_kb FROM package_version_history
     WHERE version <> '2.2.2' ORDER BY package, version")
  refresh_current_versions(con, parse_cran_listing(
    contrib_line("abc", "2.2.2", "2024-06-01", "204K")))
  after <- DBI::dbGetQuery(con, "
    SELECT package, version, published, size_kb FROM package_version_history
     WHERE version <> '2.2.2' ORDER BY package, version")

  expect_equal(after, before)
})

test_that("refreshing is idempotent and corrects a size CRAN has since changed", {
  db <- vh_db()
  on.exit(unlink(db))
  con <- DBI::dbConnect(RSQLite::SQLite(), db)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  snap <- parse_cran_listing(contrib_line("abc", "2.2.1", "2023-01-01", "191K"))
  refresh_current_versions(con, snap)
  refresh_current_versions(con, snap)

  got <- DBI::dbGetQuery(con, "
    SELECT COUNT(*) AS n FROM package_version_history WHERE package = 'abc'")
  expect_equal(got$n, 2L)   # still 2.2.0 and 2.2.1, no duplicate row
  sz <- DBI::dbGetQuery(con, "
    SELECT size_kb FROM package_version_history WHERE package='abc' AND version='2.2.1'")
  expect_equal(sz$size_kb, 191)
})

test_that("refreshing an empty or absent snapshot is a no-op, not a wipe", {
  # A failed CRAN download must never be read as "CRAN has no packages".
  db <- vh_db()
  on.exit(unlink(db))
  con <- DBI::dbConnect(RSQLite::SQLite(), db)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  expect_equal(refresh_current_versions(con, NULL), 0L)
  expect_equal(refresh_current_versions(con, data.frame()), 0L)
  n <- DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM package_version_history")$n
  expect_equal(n, 4L)
})

test_that("ensure_version_history is safe to call on a database that already has it", {
  db <- vh_db()
  on.exit(unlink(db))
  con <- DBI::dbConnect(RSQLite::SQLite(), db)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  expect_silent(ensure_version_history(con))
  expect_equal(
    DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM package_version_history")$n, 4L)
})

# --- which archives have been crawled is its own fact ----------------------
#
# The backfill used to infer "already crawled this package's archive" from "this
# package has a row in package_version_history". That inference only held while
# the ONLY writer was the backfill itself. Now that update.R records the current
# release of every package every cycle, the inference would say every package is
# crawled and the backfill would stall forever, having walked ~23k of CRAN's
# ~33k. So the crawl records what it walked, explicitly.

test_that("backfill state seeds itself once from the packages already in the table", {
  # Migration: at the moment the state table appears, the set it must hold is
  # exactly the set the old inference would have produced.
  db <- vh_db()
  on.exit(unlink(db))
  con <- DBI::dbConnect(RSQLite::SQLite(), db)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  ensure_backfill_state(con)
  expect_setequal(backfill_crawled(con), c("abc", "AATtools"))
})

test_that("a refresh after the seed does not make new packages look crawled", {
  # The regression this table exists to prevent. brandNew appears in the
  # contrib snapshot, so it gains a package_version_history row, but its ARCHIVE
  # has never been walked and it must stay on the todo list.
  db <- vh_db()
  on.exit(unlink(db))
  con <- DBI::dbConnect(RSQLite::SQLite(), db)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  ensure_backfill_state(con)
  refresh_current_versions(con, parse_cran_listing(c(
    contrib_line("abc", "2.2.2", "2024-06-01", "204K"),
    contrib_line("brandNew", "1.0", "2026-08-01", "12K"))))

  expect_true("brandNew" %in%
    DBI::dbGetQuery(con, "SELECT DISTINCT package FROM package_version_history")$package)
  expect_false("brandNew" %in% backfill_crawled(con))
  expect_equal(
    archive_backfill_todo(c("abc", "AATtools", "brandNew"), backfill_crawled(con), 500L),
    "brandNew")
})

test_that("the seed happens once and does not re-widen on later calls", {
  db <- vh_db()
  on.exit(unlink(db))
  con <- DBI::dbConnect(RSQLite::SQLite(), db)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  ensure_backfill_state(con)
  refresh_current_versions(con, parse_cran_listing(
    contrib_line("brandNew", "1.0", "2026-08-01", "12K")))
  ensure_backfill_state(con)   # a later run must not re-seed from the wider table

  expect_setequal(backfill_crawled(con), c("abc", "AATtools"))
})

test_that("mark_backfilled records a crawl and is idempotent", {
  db <- vh_db()
  on.exit(unlink(db))
  con <- DBI::dbConnect(RSQLite::SQLite(), db)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  ensure_backfill_state(con)
  mark_backfilled(con, c("brandNew", "alsoNew"))
  mark_backfilled(con, c("brandNew"))

  expect_setequal(backfill_crawled(con), c("abc", "AATtools", "brandNew", "alsoNew"))
  expect_equal(
    archive_backfill_todo(c("abc", "brandNew", "alsoNew", "third"), backfill_crawled(con), 500L),
    "third")
})

test_that("mark_backfilled tolerates an empty crawl", {
  db <- vh_db()
  on.exit(unlink(db))
  con <- DBI::dbConnect(RSQLite::SQLite(), db)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  ensure_backfill_state(con)
  expect_equal(mark_backfilled(con, character()), 0L)
  expect_setequal(backfill_crawled(con), c("abc", "AATtools"))
})

test_that("backfill state on a database with no history yet starts empty", {
  tmp <- tempfile(fileext = ".db")
  on.exit(unlink(tmp))
  con <- DBI::dbConnect(RSQLite::SQLite(), tmp)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  ensure_backfill_state(con)
  expect_equal(length(backfill_crawled(con)), 0L)
})
