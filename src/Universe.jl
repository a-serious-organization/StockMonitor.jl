"""
Russell 3000 universe loader via iShares IWV ETF holdings CSV.

The holdings CSV has a preamble before the "Ticker," header row.
Tickers are filtered to Asset Class=="Equity", normalised (. → -),
and validated against a conservative regex. Results are cached on disk
with a 30-day TTL.
"""

using CSV

const IWV_HOLDINGS_URL = (
    "https://www.ishares.com/us/products/239714/" *
    "ishares-russell-3000-etf/1467271812596.ajax" *
    "?fileType=csv&fileName=IWV_holdings&dataType=fund"
)

const _TICKER_RE = r"^[A-Z][A-Z0-9\-]{0,6}$"


function load_universe(cfg::Dict; force_refresh::Bool=false)::Vector{String}
    cache_path  = cfg["cache_path"]
    max_age     = get(cfg, "cache_max_age_days", 30)

    if !force_refresh && _cache_is_fresh(cache_path, max_age)
        @info "universe: using cache $cache_path"
        return _read_cache(cache_path)
    end

    # Support injected CSV for testing (avoids network in unit tests)
    csv_text = if haskey(cfg, "_test_csv")
        cfg["_test_csv"]
    else
        @info "universe: fetching IWV holdings from iShares"
        _fetch_iwv_holdings()
    end

    tickers = _parse_iwv_holdings(csv_text)
    mkpath(dirname(cache_path))
    open(cache_path, "w") do io
        println(io, "ticker")
        for t in tickers
            println(io, t)
        end
    end
    @info "universe: cached $(length(tickers)) tickers to $cache_path"
    return tickers
end


function _cache_is_fresh(cache_path::AbstractString, max_age_days::Real)::Bool
    isfile(cache_path) || return false
    age_seconds = time() - mtime(cache_path)
    return age_seconds < max_age_days * 86400
end


function _read_cache(cache_path::AbstractString)::Vector{String}
    df = CSV.read(cache_path, DataFrames.DataFrame)
    return Vector{String}(df.ticker)
end


function _fetch_iwv_holdings()::String
    resp = HTTP.get(IWV_HOLDINGS_URL;
                    headers=["User-Agent" => "Mozilla/5.0"],
                    readtimeout=60)
    return String(resp.body)
end


function _parse_iwv_holdings(csv_text::AbstractString)::Vector{String}
    lines = split(csv_text, "\n")
    header_idx = findfirst(l -> startswith(strip(l), "Ticker,"), lines)
    isnothing(header_idx) && error("IWV CSV missing 'Ticker,' header row")

    body = join(lines[header_idx:end], "\n")
    df = CSV.read(IOBuffer(body), DataFrames.DataFrame; missingstring="")

    # Filter to equity rows
    if "Asset Class" in names(df)
        equity_mask = [occursin("Equity", coalesce(string(v), ""))
                       for v in df[!, "Asset Class"]]
        df = df[equity_mask, :]
    end

    tickers = String[]
    for raw in df.Ticker
        t = _normalize_ticker(string(raw))
        if !isempty(t) && !isnothing(match(_TICKER_RE, t))
            push!(tickers, t)
        end
    end
    return unique(tickers)
end


function _normalize_ticker(t::AbstractString)::String
    return replace(strip(t), "." => "-")
end
