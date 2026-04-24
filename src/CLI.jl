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
    end
    return parse_args(args, s)
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
                  limit::Union{Int,Nothing}=nothing)::Int

    scan_date = today()

    tickers = load_universe(config["universe"]; force_refresh=refresh_universe)
    if !isnothing(limit)
        tickers = tickers[1:min(limit, length(tickers))]
    end
    @info "scanning $(length(tickers)) tickers"

    bars = fetch_daily_bars(tickers, config["data"])
    if isempty(bars)
        @error "fetch produced no bars; aborting"
        return 1
    end

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

    scfg = config["storage"]
    history_dir = scfg["history_dir"]
    write_results(results, scan_date,
                  scfg["results_dir"], history_dir, scfg["latest_parquet"])

    render_site(results, load_history(history_dir),
                config, scan_date, get(scfg, "site_dir", "data/site"))

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

    try
        return run_scan(config;
            refresh_universe = parsed["refresh-universe"],
            dry_run          = parsed["dry-run"],
            limit            = parsed["limit"],
        )
    catch e
        @error "scan failed" exception=(e, catch_backtrace())
        return 1
    end
end
