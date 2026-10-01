# A package can appear in the listing for the first time without being new to
# CRAN: the OS_type filter hid hespdiv and RDesk for months. Such a package gets
# its history as events, the way seed-versions.R made them, so RSS, the growth
# figures and first_published never announce it as new. Fixture rows are the
# real package_version_history and package_versions rows of 2026-09-28. The
# history is whole only for a package whose archive has been walked, so the
# archive of one that has not is read first: RDesk 1.0.4 was only there.

.feed_db <- function() {
  con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  DBI::dbExecute(con, "
    CREATE TABLE package_versions (
      id INTEGER PRIMARY KEY AUTOINCREMENT, package TEXT NOT NULL, version TEXT,
      event_type TEXT NOT NULL, previous_version TEXT, removal_reason TEXT,
      detected_at TEXT NOT NULL, published TEXT)")
  ensure_version_history(con)
  ensure_backfill_state(con)
  DBI::dbExecute(con, "
    INSERT INTO package_version_history (package, version, published, size_kb, source) VALUES
      ('RDesk', '1.0.5', '2026-04-22', 3481.6, 'cran'),
      ('RDesk', '1.0.7', '2026-09-03', 3993.6, 'cran'),
      ('hespdiv', '1.2.10', '2026-05-21', 2867.2, 'cran'),
      ('blatr', '1.0', '2015-03-10', 3.9, 'cran'),
      ('blatr', '1.0.1', '2015-03-11', 3.8, 'cran'),
      ('freshpkg', '0.1.0', '2026-09-25', 12.0, 'cran')")
  DBI::dbExecute(con, "
    INSERT INTO package_versions (package, version, event_type, previous_version,
                                  removal_reason, detected_at, published) VALUES
      ('blatr', '1.0', 'new', NULL, NULL, '2015-03-10T00:00:00Z', '2015-03-10'),
      ('blatr', '1.0.1', 'updated', '1.0', NULL, '2015-03-11T00:00:00Z', '2015-03-11')")
  mark_backfilled(con, "blatr")   # the backfill walked blatr, not the other three
  con
}

# One row of the index CRAN serves at /src/contrib/Archive/<pkg>/.
.archive_line <- function(pkg, ver, date, time, size) {
  sprintf(paste0('<tr><td valign="top"><img src="/icons/compressed.gif" alt="[   ]"></td>',
                 '<td><a href="%s_%s.tar.gz">%s_%s.tar.gz</a></td>',
                 '<td align="right">%s %s  </td><td align="right">%s</td><td>&nbsp;</td></tr>'),
          pkg, ver, pkg, ver, date, time, size)
}

# RDesk's archive listing of 2026-09-30. hespdiv has no archive directory.
.rdesk_archive <- c(.archive_line("RDesk", "1.0.4", "2026-03-31", "17:50", "3.4M"),
                    .archive_line("RDesk", "1.0.5", "2026-04-22", "15:00", "3.4M"))

# Stands in for read_cran_archive() and records which packages were asked for.
.archive <- function() {
  asked <- character(0)
  list(asked = function() asked,
       read = function(pkg) {
         asked <<- c(asked, pkg)
         if (pkg != "RDesk") return(NULL)
         parse_cran_listing(.rdesk_archive, pkg_filter = pkg)
       })
}

.seed <- function(con, pkgs, today, read_archive = .archive()$read) {
  seed_events_for_first_sighted(con, pkgs, as.Date(today), read_archive = read_archive)
}

# What update.R does after the guard: a live "new" for each remaining package,
# then the collapse of duplicate "new" events.
.live_new <- function(con, pkgs, now = "2026-10-01T06:00:12Z") {
  for (p in names(pkgs)) {
    DBI::dbExecute(con, "
      INSERT INTO package_versions (package, version, event_type, detected_at)
      VALUES (?, ?, 'new', ?)", params = list(p, unname(pkgs[[p]]), now))
  }
  collapse_duplicate_new_events(con)
}

.events <- function(con, p) {
  DBI::dbGetQuery(con, "
    SELECT version, event_type, previous_version, detected_at, published
      FROM package_versions WHERE package = ? ORDER BY detected_at, id", params = list(p))
}

test_that("packages hidden by the listing filter are seeded from their history", {
  con <- .feed_db(); on.exit(DBI::dbDisconnect(con))
  seeded <- .seed(con, c(hespdiv = "1.2.10", RDesk = "1.0.7"), "2026-10-01")
  expect_setequal(seeded, c("hespdiv", "RDesk"))
  r <- .events(con, "RDesk")
  expect_equal(r$version, c("1.0.4", "1.0.5", "1.0.7"))
  expect_equal(r$event_type, c("new", "updated", "updated"))
  expect_equal(r$previous_version, c(NA, "1.0.4", "1.0.5"))
  expect_equal(r$detected_at, c("2026-03-31T00:00:00Z", "2026-04-22T00:00:00Z",
                                "2026-09-03T00:00:00Z"))
  expect_equal(r$published, c("2026-03-31", "2026-04-22", "2026-09-03"))
  expect_equal(.events(con, "hespdiv")$event_type, "new")
})

test_that("no new event survives for a seeded package, and none is dated today", {
  con <- .feed_db(); on.exit(DBI::dbDisconnect(con))
  pkgs <- c(hespdiv = "1.2.10", RDesk = "1.0.7")
  seeded <- .seed(con, pkgs, "2026-10-01")
  .live_new(con, pkgs[setdiff(names(pkgs), seeded)])
  n <- DBI::dbGetQuery(con, "
    SELECT COUNT(*) AS n FROM package_versions
     WHERE package IN ('hespdiv', 'RDesk') AND detected_at >= '2026-10-01'")$n
  expect_equal(n, 0L)
  first <- DBI::dbGetQuery(con, "
    SELECT MIN(detected_at) AS d FROM package_versions
     WHERE package = 'RDesk' AND event_type = 'new'")$d
  expect_equal(first, "2026-03-31T00:00:00Z")
})

test_that("a genuinely new package keeps its new event", {
  con <- .feed_db(); on.exit(DBI::dbDisconnect(con))
  pkgs <- c(freshpkg = "0.1.0", brandnew = "1.0.0")   # history 6 days old, and none
  seeded <- .seed(con, pkgs, "2026-10-01")
  expect_equal(seeded, character(0))
  .live_new(con, pkgs)
  expect_equal(.events(con, "freshpkg")$event_type, "new")
  expect_equal(.events(con, "brandnew")$event_type, "new")
})

test_that("the age test uses the first release, not the newest", {
  # RDesk 1.0.7 is 28 days old on 2026-10-01; its first release, 1.0.4 of
  # 2026-03-31, is six months old, and was 25 days old on 2026-04-25.
  con <- .feed_db(); on.exit(DBI::dbDisconnect(con))
  expect_equal(.seed(con, c(RDesk = "1.0.7"), "2026-10-01"), "RDesk")
  con2 <- .feed_db(); on.exit(DBI::dbDisconnect(con2), add = TRUE)
  expect_equal(.seed(con2, c(RDesk = "1.0.7"), "2026-04-25"), character(0))
})

test_that("a package that already has events is left to the collapse", {
  con <- .feed_db(); on.exit(DBI::dbDisconnect(con))
  a <- .archive()
  seeded <- .seed(con, c(blatr = "1.0.1"), "2026-10-01", a$read)
  expect_equal(seeded, character(0))
  expect_equal(a$asked(), character(0))
  removed <- .live_new(con, c(blatr = "1.0.1"))
  expect_equal(removed, 1L)
  expect_equal(nrow(.events(con, "blatr")), 2L)
})

test_that("a current version newer than the history is recorded as an update", {
  # A release in the hours before the listing refresh has no history row yet.
  con <- .feed_db(); on.exit(DBI::dbDisconnect(con))
  seeded <- .seed(con, c(RDesk = "1.0.8"), "2026-10-01")
  expect_equal(seeded, "RDesk")
  r <- .events(con, "RDesk")
  expect_equal(r$version, c("1.0.4", "1.0.5", "1.0.7", "1.0.8"))
  expect_equal(r$event_type, c("new", "updated", "updated", "updated"))
  expect_equal(r$previous_version[4], "1.0.7")
  expect_equal(r$detected_at[4], "2026-10-01T00:00:00Z")
  expect_true(is.na(r$published[4]))
})

test_that("seeding twice adds nothing the second time", {
  con <- .feed_db(); on.exit(DBI::dbDisconnect(con))
  pkgs <- c(hespdiv = "1.2.10", RDesk = "1.0.7")
  a <- .archive()
  .seed(con, pkgs, "2026-10-01", a$read)
  expect_equal(.seed(con, pkgs, "2026-10-01", a$read), character(0))
  expect_equal(DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM package_versions")$n, 6L)
  expect_equal(a$asked(), c("hespdiv", "RDesk"))   # each archive is read once
})

test_that("nothing to seed and no history table are both no-ops", {
  con <- .feed_db(); on.exit(DBI::dbDisconnect(con))
  a <- .archive()
  expect_equal(.seed(con, character(0), "2026-10-01", a$read), character(0))
  bare <- DBI::dbConnect(RSQLite::SQLite(), ":memory:"); on.exit(DBI::dbDisconnect(bare), add = TRUE)
  expect_equal(.seed(bare, c(hespdiv = "1.2.10"), "2026-10-01", a$read), character(0))
  expect_equal(a$asked(), character(0))
})

test_that("the archive of a package the backfill has not walked joins the history", {
  con <- .feed_db(); on.exit(DBI::dbDisconnect(con))
  a <- .archive()
  .seed(con, c(hespdiv = "1.2.10", RDesk = "1.0.7"), "2026-10-01", a$read)
  expect_equal(a$asked(), c("hespdiv", "RDesk"))
  h <- DBI::dbGetQuery(con, "
    SELECT version, published FROM package_version_history
     WHERE package = 'RDesk' ORDER BY published")
  expect_equal(h$version, c("1.0.4", "1.0.5", "1.0.7"))
  expect_equal(h$published, c("2026-03-31", "2026-04-22", "2026-09-03"))
  expect_true(all(c("hespdiv", "RDesk", "blatr") %in% backfill_crawled(con)))
})

test_that("a first release found only in the archive decides the age test", {
  # Had 1.0.7 replaced 1.0.5 before the history began recording current
  # releases, the history would hold one 28-day-old row.
  con <- .feed_db(); on.exit(DBI::dbDisconnect(con))
  DBI::dbExecute(con, "
    DELETE FROM package_version_history WHERE package = 'RDesk' AND version = '1.0.5'")
  expect_equal(.seed(con, c(RDesk = "1.0.7"), "2026-10-01"), "RDesk")
  r <- .events(con, "RDesk")
  expect_equal(r$version, c("1.0.4", "1.0.5", "1.0.7"))
  expect_equal(r$event_type, c("new", "updated", "updated"))
})

test_that("a genuinely new package is marked walked, so its archive is asked for once", {
  con <- .feed_db(); on.exit(DBI::dbDisconnect(con))
  a <- .archive()
  .seed(con, c(freshpkg = "0.1.0", brandnew = "1.0.0"), "2026-10-01", a$read)
  expect_equal(a$asked(), c("freshpkg", "brandnew"))
  expect_true(all(c("freshpkg", "brandnew") %in% backfill_crawled(con)))
  expect_equal(DBI::dbGetQuery(con, "
    SELECT COUNT(*) AS n FROM package_version_history WHERE package = 'brandnew'")$n, 0L)
})

test_that("a package the backfill has walked is seeded without another read", {
  con <- .feed_db(); on.exit(DBI::dbDisconnect(con))
  mark_backfilled(con, "RDesk")
  seeded <- .seed(con, c(RDesk = "1.0.7"), "2026-10-01",
                  function(pkg) stop("the archive of ", pkg, " was read"))
  expect_equal(seeded, "RDesk")
  expect_equal(.events(con, "RDesk")$version, c("1.0.5", "1.0.7"))
})

test_that("an archive that cannot be read stops the run and records no event", {
  con <- .feed_db(); on.exit(DBI::dbDisconnect(con))
  down <- function(pkg) if (pkg == "RDesk") stop("Could not read the archive of RDesk") else NULL
  expect_error(.seed(con, c(hespdiv = "1.2.10", RDesk = "1.0.7"), "2026-10-01", down),
               "Could not read the archive of RDesk")
  expect_equal(DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM package_versions")$n, 2L)
  expect_false("RDesk" %in% backfill_crawled(con))
  # The next run reads it and seeds both.
  expect_setequal(.seed(con, c(hespdiv = "1.2.10", RDesk = "1.0.7"), "2026-10-01"),
                  c("hespdiv", "RDesk"))
  expect_equal(.events(con, "RDesk")$version, c("1.0.4", "1.0.5", "1.0.7"))
})

test_that("the archive backfill's event seed has nothing left to add for a seeded package", {
  # seed-versions.R turns the history into events and skips one whose
  # (package, version, event_type) is already there.
  con <- .feed_db(); on.exit(DBI::dbDisconnect(con))
  .seed(con, c(RDesk = "1.0.7"), "2026-10-01")
  h <- DBI::dbGetQuery(con, "
    SELECT version FROM package_version_history
     WHERE package = 'RDesk' ORDER BY published ASC, version ASC")$version
  ev <- .events(con, "RDesk")
  expect_setequal(paste(h, c("new", rep("updated", length(h) - 1L))),
                  paste(ev$version, ev$event_type))
  expect_equal(sum(ev$event_type == "new"), 1L)
})

test_that("read_cran_archive parses a listing and stops on one it cannot read", {
  listing <- tempfile(fileext = ".html"); on.exit(unlink(listing))
  writeLines(c("<html><body><table>", .rdesk_archive, "</table></body></html>"), listing)
  got <- read_cran_archive("RDesk", url = paste0("file://", listing))
  expect_equal(got$package, c("RDesk", "RDesk"))
  expect_equal(got$version, c("1.0.4", "1.0.5"))
  expect_equal(got$published, c("2026-03-31", "2026-04-22"))
  expect_error(suppressMessages(read_cran_archive("RDesk", url = paste0("file://", listing, ".gone"),
                                                  sleep = function(s) NULL)),
               "Could not read")
  writeLines("<html><body><h1>Index of /src/contrib/Archive/RDesk</h1></body></html>", listing)
  expect_null(read_cran_archive("RDesk", url = paste0("file://", listing)))
})

# What url() signals for a status of 400 or more: a warning, then an error.
.status <- function(status) function(url, ...) {
  warning("cannot open URL '", url, "': HTTP status was '", status, "'")
  stop("cannot open the connection to '", url, "'")
}

# Stands in for readLines(): gives each of `answers` in turn and the last one
# from then on, and counts the reads. An answer is a page or a failing function.
.reads <- function(answers) {
  n <- 0L
  list(count = function() n,
       read = function(url, ...) {
         n <<- n + 1L
         a <- answers[[min(n, length(answers))]]
         if (is.function(a)) a(url, ...) else a
       })
}

# Stands in for Sys.sleep() and records the waits asked for.
.naps <- function() {
  slept <- numeric(0)
  list(slept = function() slept, sleep = function(s) slept <<- c(slept, s))
}

test_that("read_cran_archive takes a 404 for no archive and any other status for a failure", {
  quiet <- function(s) NULL
  expect_null(read_cran_archive("hespdiv", read_lines = .status("404 Not Found"), sleep = quiet))
  expect_error(suppressMessages(read_cran_archive(
                 "hespdiv", read_lines = .status("503 Service Unavailable"), sleep = quiet)),
               "Archive/hespdiv/.*503 Service Unavailable")
  expect_error(suppressMessages(read_cran_archive(
                 "hespdiv", read_lines = function(url, ...) stop("timed out"), sleep = quiet)),
               "Could not read .*timed out")
})

test_that("read_cran_archive tries a failed listing again and returns it once it answers", {
  r <- .reads(list(.status("503 Service Unavailable"), function(url, ...) stop("timed out"),
                   c("<html><body><table>", .rdesk_archive, "</table></body></html>")))
  z <- .naps()
  said <- testthat::capture_messages(
    got <- read_cran_archive("RDesk", read_lines = r$read, sleep = z$sleep))
  expect_equal(got$version, c("1.0.4", "1.0.5"))
  expect_equal(got$published, c("2026-03-31", "2026-04-22"))
  expect_equal(r$count(), 3L)
  expect_equal(z$slept(), c(5, 15))   # each wait is longer than the one before
  expect_length(said, 2L)
  expect_match(said[1], "Archive/RDesk/.*503 Service Unavailable.*5s")
  expect_match(said[2], "timed out.*15s")
})

test_that("read_cran_archive stops after four attempts at a listing that keeps failing", {
  r <- .reads(list(.status("503 Service Unavailable")))
  z <- .naps()
  expect_error(suppressMessages(read_cran_archive("RDesk", read_lines = r$read, sleep = z$sleep)),
               "Could not read .*Archive/RDesk/ after 4 attempts: .*503 Service Unavailable")
  expect_equal(r$count(), 4L)
  expect_equal(z$slept(), c(5, 15, 45))   # no wait after the last attempt
})

test_that("read_cran_archive makes one more attempt than it is given waits", {
  r <- .reads(list(.status("500 Internal Server Error")))
  z <- .naps()
  expect_error(suppressMessages(read_cran_archive("RDesk", read_lines = r$read,
                                                  waits = c(1, 2), sleep = z$sleep)),
               "after 3 attempts")
  expect_equal(r$count(), 3L)
  expect_equal(z$slept(), c(1, 2))
  once <- .reads(list(.status("500 Internal Server Error")))
  expect_error(read_cran_archive("RDesk", read_lines = once$read, waits = numeric(0),
                                 sleep = z$sleep), "after 1 attempt: ")
  expect_equal(once$count(), 1L)
  expect_equal(z$slept(), c(1, 2))
})

test_that("read_cran_archive does not try a 404 again", {
  r <- .reads(list(.status("404 Not Found")))
  z <- .naps()
  expect_null(read_cran_archive("hespdiv", read_lines = r$read, sleep = z$sleep))
  expect_equal(r$count(), 1L)
  expect_equal(z$slept(), numeric(0))
})

test_that("a 404 on a later attempt still means the package has no archive", {
  r <- .reads(list(.status("503 Service Unavailable"), .status("404 Not Found")))
  z <- .naps()
  expect_null(suppressMessages(read_cran_archive("hespdiv", read_lines = r$read, sleep = z$sleep)))
  expect_equal(r$count(), 2L)
  expect_equal(z$slept(), 5)
})

test_that("a listing that answers on a later attempt does not stop the seeding", {
  con <- .feed_db(); on.exit(DBI::dbDisconnect(con))
  r <- .reads(list(.status("503 Service Unavailable"),
                   c("<html><body><table>", .rdesk_archive, "</table></body></html>")))
  z <- .naps()
  flaky <- function(pkg) read_cran_archive(pkg, read_lines = r$read, sleep = z$sleep)
  expect_equal(suppressMessages(.seed(con, c(RDesk = "1.0.7"), "2026-10-01", flaky)), "RDesk")
  expect_equal(.events(con, "RDesk")$version, c("1.0.4", "1.0.5", "1.0.7"))
  expect_equal(z$slept(), 5)
  expect_true("RDesk" %in% backfill_crawled(con))
})

test_that("a listing that never answers stops the seeding after the last attempt", {
  con <- .feed_db(); on.exit(DBI::dbDisconnect(con))
  r <- .reads(list(.status("503 Service Unavailable")))
  z <- .naps()
  down <- function(pkg) read_cran_archive(pkg, read_lines = r$read, sleep = z$sleep)
  expect_error(suppressMessages(.seed(con, c(RDesk = "1.0.7"), "2026-10-01", down)),
               "after 4 attempts")
  expect_equal(r$count(), 4L)
  expect_equal(DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM package_versions")$n, 2L)
  expect_false("RDesk" %in% backfill_crawled(con))
})

test_that("surviving_new reports only packages whose new event outlived the collapse", {
  con <- .feed_db(); on.exit(DBI::dbDisconnect(con))
  .live_new(con, c(blatr = "1.0.1", freshpkg = "0.1.0"))
  expect_equal(surviving_new(con, c("blatr", "freshpkg"), "2026-10-01T06:00:12Z"), "freshpkg")
  expect_equal(surviving_new(con, character(0), "2026-10-01T06:00:12Z"), character(0))
})
