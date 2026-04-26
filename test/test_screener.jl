using Test, DataFrames, Dates
using StockMonitor: screen

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
