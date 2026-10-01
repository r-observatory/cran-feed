# cran-feed lists what CRAN's PACKAGES index lists, with only the duplicates
# filter: a package declaring OS_type: windows, or needing a newer R than this
# runner, is on CRAN all the same, and a Recommended package listed twice is
# one package.

.fixture_repo <- function() {
  paste0("file://", normalizePath(testthat::test_path("fixtures", "cran-repo")))
}

test_that("a Windows-only package and one needing a newer R are both listed", {
  ap <- cran_available(.fixture_repo())
  expect_setequal(rownames(ap), c("boot", "cli", "hespdiv", "laterR"))
  expect_equal(unname(ap["hespdiv", "OS_type"]), "windows")
})

test_that("a Recommended package listed twice is one row, so packages gets it once", {
  ap <- cran_available(.fixture_repo())
  expect_equal(sum(ap[, "Package"] == "boot"), 1L)
  expect_false(anyDuplicated(ap[, "Package"]) > 0L)
})

test_that("the columns update.R reads are all there", {
  ap <- cran_available(.fixture_repo())
  for (col in c("Package", "Version", "NeedsCompilation", "Depends", "Imports",
                "Suggests", "LinkingTo")) {
    expect_true(col %in% colnames(ap), info = col)
  }
  expect_equal(unname(ap["laterR", "Imports"]), "cli")
})

test_that("the default filters drop both packages on this runner", {
  skip_on_os("windows")
  ap <- utils::available.packages(repos = .fixture_repo())
  expect_setequal(rownames(ap), c("boot", "cli"))
})
