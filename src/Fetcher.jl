"""
Fetcher: Yahoo Finance data download + daily metrics computation.

compute_daily_metrics(bars) — pure function, no network.
fetch_daily_bars(tickers, cfg)  — HTTP I/O via Yahoo Finance v8 chart API.
"""

const _METRIC_COLS = [:ticker, :date, :close, :prev_close,
                      :pct_change, :volume, :notional_volume]

function compute_daily_metrics(bars::DataFrame)::DataFrame
    empty_out = DataFrame(
        ticker=String[], date=Date[], close=Float64[], prev_close=Float64[],
        pct_change=Float64[], volume=Int[], notional_volume=Float64[],
    )
    isempty(bars) && return empty_out

    sorted = sort(bars, [:ticker, :date]; alg=MergeSort)
    result_rows = []

    for gdf in groupby(sorted, :ticker)
        nrow(gdf) < 2 && continue
        prev_row = gdf[end-1, :]
        last_row = gdf[end,   :]

        prev_close = prev_row.close
        close      = last_row.close

        (isnan(close) || isnan(prev_close) || prev_close == 0) && continue

        pct_change     = (close - prev_close) / prev_close * 100
        notional_volume = close * last_row.volume

        push!(result_rows, (
            ticker          = last_row.ticker,
            date            = last_row.date,
            close           = close,
            prev_close      = prev_close,
            pct_change      = pct_change,
            volume          = last_row.volume,
            notional_volume = notional_volume,
        ))
    end

    isempty(result_rows) && return empty_out
    return DataFrame(result_rows)
end


function fetch_daily_bars(
        tickers::AbstractVector{<:AbstractString},
        cfg::Dict;
        period_days::Int = 7,
    )::DataFrame

    max_workers = get(cfg, "max_workers", 4)
    retry_count = get(cfg, "retry_count", 2)
    timeout_sec = get(cfg, "request_timeout", 30)

    t_end   = now(UTC)
    t_start = t_end - Day(period_days)
    p1 = string(round(Int, datetime2unix(t_start)))
    p2 = string(round(Int, datetime2unix(t_end)))

    cookies = _get_yf_cookies()

    function fetch_one(ticker)
        url = "https://query1.finance.yahoo.com/v8/finance/chart/$ticker" *
              "?period1=$p1&period2=$p2&interval=1d&events=history"
        headers = [
            "User-Agent" => "Mozilla/5.0",
            "Cookie"     => cookies,
        ]
        last_exc = nothing
        for attempt in 0:retry_count
            try
                resp = HTTP.get(url, headers; readtimeout=timeout_sec)
                return _parse_yf_chart_json(ticker, resp.body)
            catch e
                last_exc = e
                sleep(1.5 * (attempt + 1))
            end
        end
        @warn "fetch failed for $ticker after $(retry_count+1) attempts: $last_exc"
        return DataFrame()
    end

    frames = asyncmap(fetch_one, tickers; ntasks=max_workers)
    non_empty = filter(!isempty, frames)
    isempty(non_empty) && return DataFrame()

    result = vcat(non_empty...)
    return filter(row -> !isnan(row.close), result)
end


function _get_yf_cookies()::String
    resp = HTTP.get("https://finance.yahoo.com/";
                    headers=["User-Agent" => "Mozilla/5.0"],
                    readtimeout=30)
    cookie_hdrs = [v for (k, v) in resp.headers if lowercase(k) == "set-cookie"]
    cookies = join([split(c, ";")[1] for c in cookie_hdrs], "; ")
    return cookies
end


function _parse_yf_chart_json(ticker::String, body)::DataFrame
    j = JSON3.read(body)
    res = get(get(j, :chart, nothing), :result, nothing)
    (isnothing(res) || isempty(res)) && return DataFrame()

    chart = res[1]
    timestamps = get(chart, :timestamp, nothing)
    isnothing(timestamps) && return DataFrame()

    quote_data = get(get(chart, :indicators, Dict()), :quote, nothing)
    (isnothing(quote_data) || isempty(quote_data)) && return DataFrame()
    q = quote_data[1]

    closes  = [ismissing(v) || isnothing(v) ? NaN : Float64(v) for v in get(q, :close,  [])]
    opens   = [ismissing(v) || isnothing(v) ? NaN : Float64(v) for v in get(q, :open,   [])]
    highs   = [ismissing(v) || isnothing(v) ? NaN : Float64(v) for v in get(q, :high,   [])]
    lows    = [ismissing(v) || isnothing(v) ? NaN : Float64(v) for v in get(q, :low,    [])]
    volumes = [ismissing(v) || isnothing(v) ? 0   : Int(v)     for v in get(q, :volume, [])]

    n = length(timestamps)
    dates = [Date(unix2datetime(Float64(ts))) for ts in timestamps]

    DataFrame(
        ticker = fill(ticker, n),
        date   = dates,
        open   = opens[1:n],
        high   = highs[1:n],
        low    = lows[1:n],
        close  = closes[1:n],
        volume = volumes[1:n],
    )
end
