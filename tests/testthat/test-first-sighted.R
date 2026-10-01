# A package can appear in the listing for the first time without being new to
# CRAN: the OS_type filter hid hespdiv and RDesk for months. Such a package gets
# its history as events, the way seed-versions.R made them, so RSS, the growth
# figures and first_published never announce it as new. Fixture rows are the
# real package_version_history and package_versions rows of 2026-09-28.

.feed_db <- function() {
  con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  DBI::dbExecute(con, "
    CREATE TABLE package_versions (
      id INTEGER PRIMARY KEY AUTOINCREMENT, package TEXT NOT NULL, version TEXT,
      event_type TEXT NOT NULL, previous_version TEXT, removal_reason TEXT,
      detected_at TEXT NOT NULL, published TEXT)")
  ensure_version_history(con)
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
  con
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
  seeded <- seed_events_for_first_sighted(con, c(hespdiv = "1.2.10", RDesk = "1.0.7"),
                                          as.Date("2026-10-01"))
  expect_setequal(seeded, c("hespdiv", "RDesk"))
  r <- .events(con, "RDesk")
  expect_equal(r$version, c("1.0.5", "1.0.7"))
  expect_equal(r$event_type, c("new", "updated"))
  expect_equal(r$previous_version, c(NA, "1.0.5"))
  expect_equal(r$detected_at, c("2026-04-22T00:00:00Z", "2026-09-03T00:00:00Z"))
  expect_equal(r$published, c("2026-04-22", "2026-09-03"))
  expect_equal(.events(con, "hespdiv")$event_type, "new")
})

test_that("no new event survives for a seeded package, and none is dated today", {
  con <- .feed_db(); on.exit(DBI::dbDisconnect(con))
  pkgs <- c(hespdiv = "1.2.10", RDesk = "1.0.7")
  seeded <- seed_events_for_first_sighted(con, pkgs, as.Date("2026-10-01"))
  .live_new(con, pkgs[setdiff(names(pkgs), seeded)])
  n <- DBI::dbGetQuery(con, "
    SELECT COUNT(*) AS n FROM package_versions
     WHERE package IN ('hespdiv', 'RDesk') AND detected_at >= '2026-10-01'")$n
  expect_equal(n, 0L)
  first <- DBI::dbGetQuery(con, "
    SELECT MIN(detected_at) AS d FROM package_versions
     WHERE package = 'RDesk' AND event_type = 'new'")$d
  expect_equal(first, "2026-04-22T00:00:00Z")
})

test_that("a genuinely new package keeps its new event", {
  con <- .feed_db(); on.exit(DBI::dbDisconnect(con))
  pkgs <- c(freshpkg = "0.1.0", brandnew = "1.0.0")   # history 6 days old, and none
  seeded <- seed_events_for_first_sighted(con, pkgs, as.Date("2026-10-01"))
  expect_equal(seeded, character(0))
  .live_new(con, pkgs)
  expect_equal(.events(con, "freshpkg")$event_type, "new")
  expect_equal(.events(con, "brandnew")$event_type, "new")
})

test_that("the age test uses the first release, not the newest", {
  # RDesk 1.0.7 is 28 days old on 2026-10-01; its first release is five months old.
  con <- .feed_db(); on.exit(DBI::dbDisconnect(con))
  expect_equal(seed_events_for_first_sighted(con, c(RDesk = "1.0.7"), as.Date("2026-10-01")),
               "RDesk")
  con2 <- .feed_db(); on.exit(DBI::dbDisconnect(con2), add = TRUE)
  expect_equal(seed_events_for_first_sighted(con2, c(RDesk = "1.0.7"), as.Date("2026-05-15")),
               character(0))
})

test_that("a package that already has events is left to the collapse", {
  con <- .feed_db(); on.exit(DBI::dbDisconnect(con))
  seeded <- seed_events_for_first_sighted(con, c(blatr = "1.0.1"), as.Date("2026-10-01"))
  expect_equal(seeded, character(0))
  removed <- .live_new(con, c(blatr = "1.0.1"))
  expect_equal(removed, 1L)
  expect_equal(nrow(.events(con, "blatr")), 2L)
})

test_that("a current version newer than the history is recorded as an update", {
  # A release in the hours before the listing refresh has no history row yet.
  con <- .feed_db(); on.exit(DBI::dbDisconnect(con))
  seeded <- seed_events_for_first_sighted(con, c(RDesk = "1.0.8"), as.Date("2026-10-01"))
  expect_equal(seeded, "RDesk")
  r <- .events(con, "RDesk")
  expect_equal(r$version, c("1.0.5", "1.0.7", "1.0.8"))
  expect_equal(r$event_type, c("new", "updated", "updated"))
  expect_equal(r$previous_version[3], "1.0.7")
  expect_equal(r$detected_at[3], "2026-10-01T00:00:00Z")
  expect_true(is.na(r$published[3]))
})

test_that("seeding twice adds nothing the second time", {
  con <- .feed_db(); on.exit(DBI::dbDisconnect(con))
  pkgs <- c(hespdiv = "1.2.10", RDesk = "1.0.7")
  seed_events_for_first_sighted(con, pkgs, as.Date("2026-10-01"))
  expect_equal(seed_events_for_first_sighted(con, pkgs, as.Date("2026-10-01")), character(0))
  expect_equal(DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM package_versions")$n, 5L)
})

test_that("nothing to seed and no history table are both no-ops", {
  con <- .feed_db(); on.exit(DBI::dbDisconnect(con))
  expect_equal(seed_events_for_first_sighted(con, character(0), as.Date("2026-10-01")),
               character(0))
  bare <- DBI::dbConnect(RSQLite::SQLite(), ":memory:"); on.exit(DBI::dbDisconnect(bare), add = TRUE)
  expect_equal(seed_events_for_first_sighted(bare, c(hespdiv = "1.2.10"), as.Date("2026-10-01")),
               character(0))
})

test_that("surviving_new reports only packages whose new event outlived the collapse", {
  con <- .feed_db(); on.exit(DBI::dbDisconnect(con))
  .live_new(con, c(blatr = "1.0.1", freshpkg = "0.1.0"))
  expect_equal(surviving_new(con, c("blatr", "freshpkg"), "2026-10-01T06:00:12Z"), "freshpkg")
  expect_equal(surviving_new(con, character(0), "2026-10-01T06:00:12Z"), character(0))
})
