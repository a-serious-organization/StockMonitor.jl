module StockMonitor

using DataFrames
using Dates
using Logging
using HTTP
using JSON3
using CSV
using Parquet2
using ArgParse
using LoggingExtras
# CSV is kept for Universe.jl (iShares IWV holdings cache is a CSV source)

include("Screener.jl")
include("Fetcher.jl")
include("Universe.jl")
include("Storage.jl")
include("BarsDB.jl")
include("Site.jl")
include("CLI.jl")

export screen, compute_daily_metrics, fetch_daily_bars,
       load_universe, write_results, load_history, render_site, main,
       bar_partition_path, existing_bar_dates, write_bars_partitions, load_bars

end
