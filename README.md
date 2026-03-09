# CRAN Feed

Tracks new, updated, and removed packages on [CRAN](https://cran.r-project.org) every 6 hours. Each run produces a SQLite database (`feed.db`) published as a GitHub release asset.

The `packages` table holds the current snapshot of all CRAN packages. The `package_versions` table is an append-only event log recording every addition, update, and removal. The `reverse_dependencies` table maps each package to the packages that depend on it.

## Data Access

### Download the latest database

**CLI:**

```bash
gh release download latest --repo r-observatory/cran-feed --pattern "feed.db"
```

**R:**

```r
download.file(
  "https://github.com/r-observatory/cran-feed/releases/latest/download/feed.db",
  "feed.db", mode = "wb"
)
```

**Python:**

```python
import urllib.request
urllib.request.urlretrieve(
    "https://github.com/r-observatory/cran-feed/releases/latest/download/feed.db",
    "feed.db"
)
```

### Example R Queries

```r
library(RSQLite)
con <- dbConnect(SQLite(), "feed.db")

# Recent events (last 50)
dbGetQuery(con, "
  SELECT package, version, event_type, detected_at
  FROM package_versions
  ORDER BY detected_at DESC
  LIMIT 50
")

# Current packages (first 10)
dbGetQuery(con, "
  SELECT name, version, published, maintainer
  FROM packages
  ORDER BY name
  LIMIT 10
")

# Reverse dependencies for a package
dbGetQuery(con, "
  SELECT rev_package, type
  FROM reverse_dependencies
  WHERE package = 'ggplot2'
  ORDER BY type, rev_package
")

dbDisconnect(con)
```

## Schema

### packages

| Column | Type | Notes |
|--------|------|-------|
| name | TEXT | PRIMARY KEY |
| version | TEXT | |
| title | TEXT | |
| description | TEXT | |
| maintainer | TEXT | Parsed name |
| maintainer_email | TEXT | Parsed email |
| license | TEXT | |
| depends | TEXT | |
| imports | TEXT | |
| suggests | TEXT | |
| linking_to | TEXT | |
| needs_compilation | TEXT | |
| published | TEXT | |
| cran_url | TEXT | |
| first_published | TEXT | Earliest "new" event |
| is_archived | INTEGER | DEFAULT 0 |
| updated_at | TEXT | |

### package_versions

| Column | Type | Notes |
|--------|------|-------|
| id | INTEGER | PRIMARY KEY AUTOINCREMENT |
| package | TEXT | NOT NULL |
| version | TEXT | |
| event_type | TEXT | NOT NULL (new, updated, removed) |
| previous_version | TEXT | |
| removal_reason | TEXT | |
| detected_at | TEXT | NOT NULL |
| published | TEXT | |

Indexes: `idx_pv_package`, `idx_pv_detected`, `idx_pv_event`

### reverse_dependencies

| Column | Type | Notes |
|--------|------|-------|
| package | TEXT | NOT NULL |
| rev_package | TEXT | NOT NULL |
| type | TEXT | NOT NULL (depends, imports, suggests, linking_to) |

PRIMARY KEY: (package, rev_package, type)
Index: `idx_revdep_pkg`

## Update Schedule

The database is rebuilt every 6 hours via GitHub Actions (`0 */6 * * *`). Each run creates a new release tagged with a timestamp.

## License

The data originates from CRAN. This pipeline code is available under the MIT License.
