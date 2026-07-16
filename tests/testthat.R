library(testthat)
library(RSQLite)

# cran-feed is a script repo, not an R package. scripts/update.R is a flat,
# side-effecting script (it calls available.packages() at top level), so only
# the pure manifest helpers are sourced here for testing.
source(file.path(getwd(), "scripts", "helpers.R"))

test_dir("tests/testthat", stop_on_failure = TRUE)
