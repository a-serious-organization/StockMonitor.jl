"""
CLI orchestration: wires universe → fetcher → screener → storage → site.
Entry point: StockMonitor.main(ARGS).
"""

using Dates
using TOML
using Logging
using LoggingExtras

function _parse_args(args)
    s = ArgParseSettings(description="Daily stock gainers scan.")
    @add_arg_table! s begin
        "--config"
            default = "config/config.toml"
            help    = "Path to TOML config file"
        "--refresh-universe"
            action  = :store_true
            help    = "Force IWV re-fetch (bypass cache)"
        "--dry-run"
            action  = :store_true
            help    = "Compute but do not write files"
        "--limit"
            arg_type = Int
            default  = nothing
            help     = "Use first N tickers only"
        "-d", "--dates"
            action   = :append_arg
            arg_type = String
            help     = "Ensure dates are cached (missing-only). Repeatable: YYYYMMDD or YYYYMMDD:YYYYMMDD"
        "--force"
            action  = :store_true
            help    = "Re-download all in-scope dates, overwriting existing partitions"
    end
    return parse_args(args, s)
end


function _parse_date_flags(vals::AbstractVector)::Vector{Date}
    out = Date[]
    for v in vals
        append!(out, _parse_date_flag(v))
    end
    return sort(unique(out))
end


function _parse_date_flag(val::AbstractString)::Vector{Date}
    if occursin(":", val)
        parts = split(val, ":")
        length(parts) == 2 || error("--dates: expected YYYYMMDD:YYYYMMDD, got: $val")
        s_str, e_str = parts
        length(s_str) == 8 && length(e_str) == 8 ||
            error("--dates: each part must be YYYYMMDD, got: $val")
        s = tryparse(Date, s_str, dateformat"yyyymmdd")
        e = tryparse(Date, e_str, dateformat"yyyymmdd")
        (isnothing(s) || isnothing(e)) && error("--dates: invalid date in range: $val")
        s <= e || error("--dates: start must be ≤ end, got: $val")
        return collect(s:Day(1):e)
    else
        length(val) == 8 || error("--dates: expected YYYYMMDD, got: $val")
        d = tryparse(Date, val, dateformat"yyyymmdd")
        isnothing(d) && error("--dates: invalid date: $val")
        return [d]
    end
end


function _setup_logging(level_str::AbstractString, log_dir::AbstractString, scan_date::Date)
    mkpath(log_dir)
    level = Dict(
        "debug"   => Logging.Debug,
        "info"    => Logging.Info,
        "warn"    => Logging.Warn,
        "error"   => Logging.Error,
    )[lowercase(level_str)]

    log_path = joinpath(log_dir, "scan_$(scan_date).log")
    file_io  = open(log_path, "a")

    logger = TeeLogger(
        ConsoleLogger(stdout, level),
        SimpleLogger(file_io, level),
    )
    Logging.global_logger(logger)
end


function run_scan(config::Dict;
                  refresh_universe::Bool=false,
                  dry_run::Bool=false,
                  limit::Union{Int,Nothing}=nothing,
                  requested_dates::Vector{Date}=Date[],
                  force::Bool=false)::Int

    # --- 1-2: window bounds
    window_days  = get(get(config, "data", Dict()), "window_days", 40)
    window_end   = today()
    window_start = window_end - Day(window_days)

    # --- storage paths
    scfg     = config["storage"]
    bars_dir = get(scfg, "bars_dir", "data/bars")

    # --- tickers
    tickers = load_universe(config["universe"]; force_refresh=refresh_universe)
    if !isnothing(limit)
        tickers = tickers[1:min(limit, length(tickers))]
    end
    @info "scanning $(length(tickers)) tickers"

    # --- 3-6: determine which dates to fetch
    existing      = existing_bar_dates(bars_dir)
    window_dates  = collect(window_start:Day(1):window_end)

    if force
        # --force: re-fetch the trailing window plus any --dates entries, overwriting all.
        dates_to_fetch = sort(unique(vcat(window_dates, requested_dates)))
        overwrite_set  = Set(dates_to_fetch)
    else
        # Incremental: trailing window's missing dates + missing --dates entries.
        # today is always re-fetched (intraday data may be partial).
        missing_window    = setdiff(window_dates, existing)
        missing_requested = setdiff(requested_dates, existing)
        dates_to_fetch    = sort(unique(vcat(missing_window, missing_requested, [window_end])))
        overwrite_set     = Set([window_end])
    end

    # --- 7: fetch and write partitions
    if !isempty(dates_to_fetch)
        fetch_span_start = minimum(dates_to_fetch)
        fetch_span_end   = maximum(dates_to_fetch)
        @info "fetching bars for $(length(dates_to_fetch)) date(s): $fetch_span_start … $fetch_span_end"

        fetched = fetch_daily_bars(tickers, config["data"];
                                   start_date=fetch_span_start,
                                   end_date=fetch_span_end)

        if isempty(fetched)
            @error "fetch produced no bars; aborting"
            return 1
        end

        # Keep only the dates we actually want
        dates_set = Set(dates_to_fetch)
        fetched = filter(row -> row.date in dates_set, fetched)

        if !dry_run
            incremental = filter(row -> !(row.date in overwrite_set), fetched)
            forced      = filter(row ->   row.date in overwrite_set,  fetched)

            isempty(incremental) || write_bars_partitions(incremental, bars_dir; overwrite=false)
            isempty(forced)      || write_bars_partitions(forced,      bars_dir; overwrite=true)
        end
    else
        @info "all in-scope dates cached; skipping fetch"
    end

    # --- 8: load the full window from cache (or from fetched if dry-run with cold cache)
    bars = load_bars(bars_dir, window_start, window_end)

    if dry_run && isempty(bars) && !isempty(dates_to_fetch)
        # Cache is cold and --dry-run prevents writes; use the already-fetched data
        bars = fetched
    end

    if isempty(bars)
        @error "no bars available for the window; aborting"
        return 1
    end

    # --- 9-10: metrics + screen
    metrics = compute_daily_metrics(bars)
    @info "metrics: $(nrow(metrics)) tickers with 2+ sessions"

    results = screen(metrics, config["criteria"])

    top10 = first(results, 10)
    if nrow(top10) > 0
        @info "top $(nrow(top10)) gainers:\n$(top10[:, [:rank,:ticker,:close,:pct_change,:volume]])"
    else
        @info "no tickers passed filters today"
    end

    dry_run && (@info "--dry-run set; skipping output"; return 0)

    # --- 11: write results + render site
    history_dir = scfg["history_dir"]
    write_results(results, window_end,
                  scfg["results_dir"], history_dir, scfg["latest_parquet"])

    render_site(results, load_history(history_dir),
                config, window_end, get(scfg, "site_dir", "data/site"))

    return 0
end


function main(args=ARGS)
    parsed = _parse_args(args)
    cfg_path = parsed["config"]

    if !isfile(cfg_path)
        @error "config file not found: $cfg_path"
        return 1
    end

    config   = TOML.parsefile(cfg_path)
    log_cfg  = get(config, "logging", Dict())
    scan_date = today()

    _setup_logging(
        get(log_cfg, "level", "Info"),
        get(log_cfg, "log_dir", "logs"),
        scan_date,
    )

    requested_dates = Date[]
    try
        requested_dates = _parse_date_flags(parsed["dates"])
    catch e
        @error string(e)
        return 1
    end

    try
        return run_scan(config;
            refresh_universe = parsed["refresh-universe"],
            dry_run          = parsed["dry-run"],
            limit            = parsed["limit"],
            requested_dates  = requested_dates,
            force            = parsed["force"],
        )
    catch e
        @error "scan failed" exception=(e, catch_backtrace())
        return 1
    end
end
