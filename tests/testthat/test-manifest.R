# Integrity / completeness core for the primary published DB (feed.db).

# Build a tiny, real feed.db on disk using the canonical cran-feed schema and
# the exact finalize path scripts/update.R uses: WAL journal, checkpoint back
# into the single file, then close. This proves the core is computed against
# genuine, sidecar-free SQLite bytes.
build_feed_db <- function(n_pkgs = 2L, n_events = 3L, n_revdeps = 1L) {
  tmp <- tempfile(fileext = ".db")
  con <- dbConnect(SQLite(), tmp)

  dbExecute(con, "PRAGMA journal_mode=WAL")
  dbExecute(con, "PRAGMA synchronous=NORMAL")

  dbExecute(con, "
    CREATE TABLE packages (
      name TEXT PRIMARY KEY, version TEXT, title TEXT, description TEXT,
      maintainer TEXT, maintainer_email TEXT, license TEXT, depends TEXT,
      imports TEXT, suggests TEXT, linking_to TEXT, needs_compilation TEXT,
      published TEXT, cran_url TEXT, first_published TEXT,
      is_archived INTEGER DEFAULT 0, updated_at TEXT)")
  dbExecute(con, "
    CREATE TABLE package_versions (
      id INTEGER PRIMARY KEY AUTOINCREMENT, package TEXT NOT NULL, version TEXT,
      event_type TEXT NOT NULL, previous_version TEXT, removal_reason TEXT,
      detected_at TEXT NOT NULL, published TEXT)")
  dbExecute(con, "
    CREATE TABLE reverse_dependencies (
      package TEXT NOT NULL, rev_package TEXT NOT NULL, type TEXT NOT NULL,
      PRIMARY KEY (package, rev_package, type))")

  pkgs <- data.frame(
    name = paste0("pkg", seq_len(n_pkgs)), version = "1.0",
    title = "T", description = "D", maintainer = "M", maintainer_email = NA_character_,
    license = "MIT", depends = NA_character_, imports = NA_character_,
    suggests = NA_character_, linking_to = NA_character_, needs_compilation = "no",
    published = "2026-07-01", cran_url = "https://CRAN.R-project.org/",
    first_published = "2026-07-01", is_archived = 0L, updated_at = "2026-07-01",
    stringsAsFactors = FALSE)
  dbWriteTable(con, "packages", pkgs, append = TRUE)

  events <- data.frame(
    package = paste0("pkg", rep(seq_len(n_pkgs), length.out = n_events)),
    version = "1.0", event_type = "new", previous_version = NA_character_,
    removal_reason = NA_character_, detected_at = "2026-07-01T00:00:00Z",
    published = "2026-07-01", stringsAsFactors = FALSE)[seq_len(n_events), , drop = FALSE]
  dbWriteTable(con, "package_versions", events, append = TRUE)

  if (n_revdeps > 0L) {
    revdeps <- data.frame(
      package = "pkg1", rev_package = paste0("dep", seq_len(n_revdeps)),
      type = "imports", stringsAsFactors = FALSE)
    dbWriteTable(con, "reverse_dependencies", revdeps, append = TRUE)
  }

  # Fold the WAL back into the file and close, exactly like update.R does before
  # writing the manifest.
  dbExecute(con, "PRAGMA wal_checkpoint(TRUNCATE)")
  dbDisconnect(con)
  tmp
}

test_that("summary_integrity_core reports filename, bytes, sha256, tables, complete", {
  db <- build_feed_db(n_pkgs = 2L, n_events = 3L, n_revdeps = 1L)
  on.exit(unlink(db))

  core <- summary_integrity_core(db, complete = TRUE)

  expect_equal(core$db_filename, basename(db))
  # db_bytes is a double (not cast to integer) so files >= ~2 GiB do not
  # overflow to NA; compare against the uncast file.size() directly.
  expect_type(core$db_bytes, "double")
  expect_equal(core$db_bytes, file.size(db))
  # sha256 is lowercase 64-char hex of the exact file bytes
  expect_match(core$db_sha256, "^[0-9a-f]{64}$")
  # tables maps every user table to its row count (no sqlite_% internals)
  expect_setequal(names(core$tables),
                  c("packages", "package_versions", "reverse_dependencies"))
  expect_equal(core$tables$packages, 2L)
  expect_equal(core$tables$package_versions, 3L)
  expect_equal(core$tables$reverse_dependencies, 1L)
  # the AUTOINCREMENT table introduces sqlite_sequence; it must be excluded
  expect_false("sqlite_sequence" %in% names(core$tables))
  expect_true(core$complete)
})

test_that("summary_integrity_core sha256 matches an independent digest of the bytes", {
  # Compute the expected hash via an external CLI tool, independent of
  # file_sha256()'s own preferred backend (digest/openssl), so this test
  # genuinely cross-checks the code path instead of re-running the same
  # library. Skip only if neither tool is on PATH (both are expected on CI).
  sha256sum_bin <- Sys.which("sha256sum")
  shasum_bin    <- Sys.which("shasum")
  if (!nzchar(sha256sum_bin) && !nzchar(shasum_bin)) {
    skip("neither sha256sum nor shasum is on PATH")
  }

  db <- build_feed_db(n_pkgs = 3L, n_events = 5L, n_revdeps = 2L)
  on.exit(unlink(db))

  core <- summary_integrity_core(db)

  if (nzchar(sha256sum_bin)) {
    out <- system2(sha256sum_bin, shQuote(db), stdout = TRUE)
  } else {
    out <- system2(shasum_bin, c("-a", "256", shQuote(db)), stdout = TRUE)
  }
  independent <- tolower(sub("\\s.*$", "", out[1]))

  expect_equal(core$db_sha256, independent)
})

test_that("finalized feed.db leaves no -wal/-shm sidecar and hashes stably", {
  db <- build_feed_db()
  on.exit(unlink(db))
  # After checkpoint(TRUNCATE) + disconnect the DB is a single file.
  expect_false(file.exists(paste0(db, "-wal")))
  expect_false(file.exists(paste0(db, "-shm")))
  # The hash of the finalized bytes is stable across repeated reads.
  expect_equal(file_sha256(db), file_sha256(db))
})

test_that("write_manifest emits generated_at plus the integrity core as top-level fields", {
  db <- build_feed_db(n_pkgs = 4L, n_events = 4L, n_revdeps = 1L)
  on.exit(unlink(db), add = TRUE)
  core <- summary_integrity_core(db, complete = TRUE)

  tmp <- tempfile(fileext = ".json")
  on.exit(unlink(tmp), add = TRUE)

  write_manifest(tmp, core)

  parsed <- jsonlite::fromJSON(tmp)
  # freshness field present and non-empty
  expect_true(nzchar(parsed$generated_at))
  # integrity/completeness core lives at the TOP level, not nested
  expect_equal(parsed$db_filename, basename(db))
  # db_bytes is emitted as a bare JSON number, never the quoted string "NA"
  # (which is what an integer overflow on a >2 GiB file would serialize as).
  raw <- paste(readLines(tmp), collapse = "\n")
  expect_match(raw, sprintf('"db_bytes":\\s*%d(\\D|$)', as.integer(file.size(db))))
  expect_true(is.numeric(parsed$db_bytes))
  expect_equal(parsed$db_bytes, file.size(db))
  expect_match(parsed$db_sha256, "^[0-9a-f]{64}$")
  expect_equal(parsed$db_sha256, file_sha256(db))
  expect_equal(parsed$tables$packages, 4L)
  expect_equal(parsed$tables$package_versions, 4L)
  expect_true(parsed$complete)
})
