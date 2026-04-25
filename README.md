# StockMonitor.jl

End-of-day (EOD) scanner for the Russell 3000 — daily gainers meeting
liquidity thresholds. Pure Julia port of
[daily-stock-monitor](https://github.com/Jeepee-Liu/daily-stock-monitor)
(Python/pandas). Same outputs (partitioned Parquet + static HTML
dashboard), same scheduling approach, idiomatic Julia.

## Install

### 1. Install Julia

```bash
curl -fsSL https://install.julialang.org | sh -s -- --yes
source ~/.bashrc      # or open a new terminal
julia --version       # 1.12+
```

### 2. Install package dependencies

```bash
git clone https://github.com/a-serious-organization/StockMonitor.jl
cd StockMonitor.jl
julia --project -e 'import Pkg; Pkg.instantiate()'
```

## Run

```bash
# Full scan (uses config/config.toml by default)
julia --project scripts/scan.jl

# Dev: limit to first N tickers, no file writes
julia --project scripts/scan.jl --limit 20 --dry-run

# Force re-fetch of the Russell 3000 universe (bypass 30-day cache)
julia --project scripts/scan.jl --refresh-universe
```

### CLI flags

| Flag | Default | Description |
|---|---|---|
| `--config PATH` | `config/config.toml` | Config file |
| `--refresh-universe` | false | Force IWV re-fetch |
| `--dry-run` | false | Compute but write nothing |
| `--limit N` | (all) | Use first N tickers only |

Exit codes: `0` success, `1` fatal error.

## Output

Every scan writes:

- `data/results/gainers_YYYY-MM-DD.parquet` — dated snapshot
- `data/results/latest.parquet` — overwritten each run
- `data/history/date=YYYY-MM-DD/results.parquet` — partitioned history
- `data/site/index.html` — HTML dashboard (see below)

Columns: `rank, ticker, date, close, prev_close, pct_change, pct_change_2d, pct_change_5d, pct_change_1m, volume, notional_volume`.

## Dashboard

`data/site/index.html` is regenerated after every scan:

- Header with scan date and active criteria
- Today's gainers table
- 14-day bar chart of daily gainer counts (Chart.js via CDN)

Open with:

```bash
xdg-open data/site/index.html
# or serve it:
python -m http.server 8000 -d data/site
# → http://localhost:8000
```

## Tests

All tests are offline (synthetic DataFrames, no network).

```bash
julia --project test/runtests.jl
```

## Config (`config/config.toml`)

```toml
[criteria]
min_pct_change = 5.0       # %
min_price = 2.00           # USD
min_volume = 100_000       # shares
min_notional_volume = 1_000_000  # USD
direction = "gainers"      # gainers | losers | both
```

Full config: see `config/config.toml`.

## Scheduling

### Primary: Windows Task Scheduler → WSL  (recommended)

Fires even when WSL is not currently running. Windows wakes WSL on demand.

1. Open Task Scheduler → **Create Task…**
2. **Triggers**: Daily, 4:15 PM, Mon–Fri
3. **Actions**: Start `run_scan.bat` (Start in: project directory)
4. **Settings**: "Run task as soon as possible after a missed start"

Edit `run_scan.bat` → `WSL_DISTRO` if not using default Ubuntu distro.

### Secondary: Linux crontab

For users who keep WSL running during market hours:

```bash
crontab crontab.example
crontab -l   # verify
```

Uses `CRON_TZ=America/New_York` — tracks DST automatically.

**Gotcha:** cron only fires while WSL is running. For reliability across
reboots/suspend, prefer the Task Scheduler path.

## Data source

Universe: iShares IWV ETF holdings CSV (Russell 3000 proxy, updated daily).
Prices: Yahoo Finance v8 chart API via HTTP.jl (no API key required).

## Known quirk

iShares IWV now emits dotless share-class tickers (e.g. `BRKB` instead of
`BRK.B`). Yahoo Finance expects `BRK-B`. That one ticker logs a 404 warning
and is skipped. Impact: ~1 ticker out of ~2581.

## Dependencies

All managed by Julia's built-in `Pkg` — no conda/pip needed.

| Package | Purpose |
|---|---|
| DataFrames.jl | tabular data |
| CSV.jl | read universe source CSV (IWV holdings) |
| HTTP.jl | Yahoo Finance API |
| JSON3.jl | parse API responses |
| Parquet2.jl | parquet files |
| ArgParse.jl | CLI args |
| LoggingExtras.jl | TeeLogger (stdout + file) |
| TOML, Dates, Logging | Julia stdlib |
