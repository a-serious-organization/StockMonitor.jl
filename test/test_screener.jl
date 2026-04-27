using Test, DataFrames, Dates
using StockMonitor: screen, compute_prev_rank_map

const DEFAULT_CRITERIA = Dict(
    "min_pct_change"       => 5.0,
    "min_price"            => 2.00,
    "min_volume"           => 100_000,
    "min_notional_volume"  => 1_000_000,
    "direction"            => "gainers",
)

function make_row(; ticker, close, prev_close, volume, pct_change=nothing,
                    pct_change_2d=NaN, volume_ratio_5d=NaN)
    pc = isnothing(pct_change) ? (close - prev_close) / prev_close * 100 : pct_change
    DataFrame(
        ticker          = [ticker],
        date            = [Dates.today()],
        close           = [close],
        prev_close      = [prev_close],
        pct_change      = [pc],
        volume          = [volume],
        notional_volume = [close * volume],
        pct_change_2d   = [Float64(pct_change_2d)],
        volume_ratio_5d = [Float64(volume_ratio_5d)],
    )
end

@testset "Screener" begin

    @testset "row passing all filters is kept" begin
        df = make_row(ticker="AAA", close=10.0, prev_close=9.0, volume=200_000)
        out = screen(df, DEFAULT_CRITERIA)
        @test nrow(out) == 1
        @test out.ticker[1] == "AAA"
        @test out.rank[1] == 1
    end

    @testset "row failing price filter is dropped" begin
        df = vcat(
            make_row(ticker="LOW", close=1.50, prev_close=1.00, volume=500_000),
            make_row(ticker="OK",  close=10.0, prev_close=9.0,  volume=200_000),
        )
        out = screen(df, DEFAULT_CRITERIA)
        @test out.ticker == ["OK"]
    end

    @testset "row failing volume filter is dropped" begin
        df = vcat(
            make_row(ticker="THIN", close=10.0, prev_close=9.0, volume=50_000),
            make_row(ticker="OK",   close=10.0, prev_close=9.0, volume=200_000),
        )
        out = screen(df, DEFAULT_CRITERIA)
        @test out.ticker == ["OK"]
    end

    @testset "row failing notional volume filter is dropped" begin
        df = vcat(
            make_row(ticker="SMALL", close=3.00, prev_close=2.80, volume=200_000),
            make_row(ticker="OK",    close=10.0, prev_close=9.0,  volume=200_000),
        )
        out = screen(df, DEFAULT_CRITERIA)
        @test out.ticker == ["OK"]
    end

    @testset "row failing pct_change threshold is dropped" begin
        df = vcat(
            make_row(ticker="FLAT", close=10.0, prev_close=9.8, volume=200_000),
            make_row(ticker="JUMP", close=10.0, prev_close=9.0, volume=200_000),
        )
        out = screen(df, DEFAULT_CRITERIA)
        @test out.ticker == ["JUMP"]
    end

    @testset "sort order is abs(pct_change) descending" begin
        df = vcat(
            make_row(ticker="SMALL",  close=10.0, prev_close=9.5, volume=200_000),
            make_row(ticker="BIG",    close=10.0, prev_close=7.0, volume=200_000),
            make_row(ticker="MIDDLE", close=10.0, prev_close=8.5, volume=200_000),
        )
        out = screen(df, DEFAULT_CRITERIA)
        @test out.ticker == ["BIG", "MIDDLE", "SMALL"]
        @test out.rank   == [1, 2, 3]
    end

    @testset "empty input returns empty DataFrame with rank column" begin
        empty_df = DataFrame(
            ticker=String[], date=Date[], close=Float64[],
            prev_close=Float64[], pct_change=Float64[],
            volume=Int[], notional_volume=Float64[],
        )
        out = screen(empty_df, DEFAULT_CRITERIA)
        @test nrow(out) == 0
        @test "rank" in names(out)
    end

    @testset "gainers direction excludes negative pct_change" begin
        df = vcat(
            make_row(ticker="UP",   close=10.0, prev_close=9.0,  volume=200_000),
            make_row(ticker="DOWN", close=10.0, prev_close=12.0, volume=200_000),
        )
        out = screen(df, DEFAULT_CRITERIA)
        @test out.ticker == ["UP"]
    end

    # ── min_pct_change_2d ────────────────────────────────────────────────────

    @testset "min_pct_change_2d gainers keeps rows >= threshold" begin
        df = vcat(
            make_row(ticker="BIG2",  close=10.0, prev_close=9.0, volume=200_000, pct_change_2d=8.0),
            make_row(ticker="SMALL2",close=10.0, prev_close=9.0, volume=200_000, pct_change_2d=3.0),
        )
        crit = merge(DEFAULT_CRITERIA, Dict("min_pct_change_2d" => 5.0))
        out = screen(df, crit)
        @test out.ticker == ["BIG2"]
    end

    @testset "min_pct_change_2d losers keeps rows <= -threshold" begin
        df = vcat(
            make_row(ticker="DROP",  close=10.0, prev_close=12.0, volume=200_000,
                     pct_change=-20.0, pct_change_2d=-8.0),
            make_row(ticker="NUDGE", close=10.0, prev_close=12.0, volume=200_000,
                     pct_change=-20.0, pct_change_2d=-2.0),
        )
        crit = merge(DEFAULT_CRITERIA, Dict("direction" => "losers", "min_pct_change_2d" => 5.0))
        out = screen(df, crit)
        @test out.ticker == ["DROP"]
    end

    @testset "min_pct_change_2d both keeps rows with |2d| >= threshold" begin
        df = vcat(
            make_row(ticker="UPON",  close=10.0, prev_close=9.0,  volume=200_000, pct_change_2d= 7.0),
            make_row(ticker="DDOWN", close=10.0, prev_close=12.0, volume=200_000,
                     pct_change=-20.0, pct_change_2d=-7.0),
            make_row(ticker="FLAT2", close=10.0, prev_close=9.0,  volume=200_000, pct_change_2d= 1.0),
        )
        crit = merge(DEFAULT_CRITERIA, Dict("direction" => "both", "min_pct_change_2d" => 5.0))
        out = screen(df, crit)
        @test sort(out.ticker) == ["DDOWN", "UPON"]
    end

    @testset "min_pct_change_2d NaN rows excluded when threshold set" begin
        df = vcat(
            make_row(ticker="GOOD", close=10.0, prev_close=9.0, volume=200_000, pct_change_2d=8.0),
            make_row(ticker="NAN",  close=10.0, prev_close=9.0, volume=200_000),   # pct_change_2d=NaN
        )
        crit = merge(DEFAULT_CRITERIA, Dict("min_pct_change_2d" => 5.0))
        out = screen(df, crit)
        @test out.ticker == ["GOOD"]
    end

    @testset "min_pct_change_2d NaN rows kept when threshold absent" begin
        df = make_row(ticker="NAN", close=10.0, prev_close=9.0, volume=200_000)  # pct_change_2d=NaN
        out = screen(df, DEFAULT_CRITERIA)
        @test nrow(out) == 1
    end

    # ── min_volume_ratio_5d ──────────────────────────────────────────────────

    @testset "min_volume_ratio_5d keeps rows >= threshold" begin
        df = vcat(
            make_row(ticker="SPIKE", close=10.0, prev_close=9.0, volume=200_000, volume_ratio_5d=3.0),
            make_row(ticker="NORM",  close=10.0, prev_close=9.0, volume=200_000, volume_ratio_5d=0.9),
        )
        crit = merge(DEFAULT_CRITERIA, Dict("min_volume_ratio_5d" => 2.0))
        out = screen(df, crit)
        @test out.ticker == ["SPIKE"]
    end

    @testset "min_volume_ratio_5d NaN rows excluded when threshold set" begin
        df = vcat(
            make_row(ticker="GOOD", close=10.0, prev_close=9.0, volume=200_000, volume_ratio_5d=3.0),
            make_row(ticker="NAN",  close=10.0, prev_close=9.0, volume=200_000),   # volume_ratio_5d=NaN
        )
        crit = merge(DEFAULT_CRITERIA, Dict("min_volume_ratio_5d" => 2.0))
        out = screen(df, crit)
        @test out.ticker == ["GOOD"]
    end

    @testset "min_volume_ratio_5d NaN rows kept when threshold absent" begin
        df = make_row(ticker="NAN", close=10.0, prev_close=9.0, volume=200_000)  # volume_ratio_5d=NaN
        out = screen(df, DEFAULT_CRITERIA)
        @test nrow(out) == 1
    end

    # ── missing criteria keys → no filtering (regression) ───────────────────

    @testset "missing both new keys does not filter any row" begin
        df = vcat(
            make_row(ticker="A", close=10.0, prev_close=9.0, volume=200_000, pct_change_2d=1.0, volume_ratio_5d=0.5),
            make_row(ticker="B", close=10.0, prev_close=9.0, volume=200_000),  # both NaN
        )
        out = screen(df, DEFAULT_CRITERIA)   # no new keys in criteria
        @test nrow(out) == 2
    end

end


# ── compute_prev_rank_map: re-screen bars filtered to ≤ yesterday ───────────

function _bars_with_jumps()
    # 4 days of bars for 2 tickers with different daily moves so the screen
    # ranking changes day-over-day.
    rows = NamedTuple[]
    # Day 1 (2026-04-20): baseline
    # Day 2 (2026-04-21): AAA +20%, BBB +5%
    # Day 3 (2026-04-22): AAA +5%,  BBB +20%
    # Day 4 (2026-04-23): AAA +30%, BBB +5%
    pairs = [
        (Date(2026, 4, 20), 100.0,  50.0),
        (Date(2026, 4, 21), 120.0,  52.5),
        (Date(2026, 4, 22), 126.0,  63.0),
        (Date(2026, 4, 23), 163.8,  66.15),
    ]
    for (d, a_close, b_close) in pairs
        push!(rows, (ticker="AAA", date=d, open=a_close, high=a_close, low=a_close, close=a_close, volume=300_000))
        push!(rows, (ticker="BBB", date=d, open=b_close, high=b_close, low=b_close, close=b_close, volume=400_000))
    end
    return DataFrame(rows)
end

const _PREV_CRITERIA = Dict(
    "min_pct_change"      => 0.0,
    "min_price"           => 1.0,
    "min_volume"          => 0,
    "min_notional_volume" => 0.0,
    "direction"           => "gainers",
)

@testset "compute_prev_rank_map" begin

    @testset "yesterday at last bar date returns ranks from that day's screen" begin
        bars = _bars_with_jumps()
        m = compute_prev_rank_map(bars, _PREV_CRITERIA, Date(2026, 4, 23))
        # On day 4: AAA +30% > BBB +5% → AAA rank 1, BBB rank 2
        @test m == Dict("AAA" => 1, "BBB" => 2)
    end

    @testset "yesterday earlier in the window picks that day's ranking" begin
        bars = _bars_with_jumps()
        # Filter to ≤ day 3: latest move is AAA +5%, BBB +20% → BBB rank 1, AAA rank 2
        m = compute_prev_rank_map(bars, _PREV_CRITERIA, Date(2026, 4, 22))
        @test m == Dict("BBB" => 1, "AAA" => 2)
    end

    @testset "yesterday before any bars returns empty dict" begin
        bars = _bars_with_jumps()
        m = compute_prev_rank_map(bars, _PREV_CRITERIA, Date(2026, 4, 1))
        @test m == Dict{String,Int}()
    end

    @testset "empty bars input returns empty dict" begin
        bars = DataFrame(ticker=String[], date=Date[], open=Float64[], high=Float64[],
                         low=Float64[], close=Float64[], volume=Int[])
        m = compute_prev_rank_map(bars, _PREV_CRITERIA, Date(2026, 4, 23))
        @test m == Dict{String,Int}()
    end

    @testset "ticker added in window appears only in dates ≥ its first bar" begin
        bars = _bars_with_jumps()
        # Inject a "CCC" ticker with bars only from day 3 onward, +50% on day 4
        new_rows = [
            (ticker="CCC", date=Date(2026, 4, 22), open=10.0, high=10.0, low=10.0, close=10.0, volume=500_000),
            (ticker="CCC", date=Date(2026, 4, 23), open=15.0, high=15.0, low=15.0, close=15.0, volume=500_000),
        ]
        bars = vcat(bars, DataFrame(new_rows))

        # yesterday = day 3: CCC has only 1 session → no metric, excluded.
        m = compute_prev_rank_map(bars, _PREV_CRITERIA, Date(2026, 4, 22))
        @test !haskey(m, "CCC")
        @test haskey(m, "AAA") && haskey(m, "BBB")
    end

    @testset "criteria filter applied (high min_pct_change drops everything)" begin
        bars = _bars_with_jumps()
        crit = merge(_PREV_CRITERIA, Dict("min_pct_change" => 99.0))
        m = compute_prev_rank_map(bars, crit, Date(2026, 4, 23))
        @test m == Dict{String,Int}()
    end

end
