using Test, DataFrames, Dates
using StockMonitor: screen

const DEFAULT_CRITERIA = Dict(
    "min_pct_change"       => 5.0,
    "min_price"            => 2.00,
    "min_volume"           => 100_000,
    "min_notional_volume"  => 1_000_000,
    "direction"            => "gainers",
)

function make_row(; ticker, close, prev_close, volume, pct_change=nothing)
    pc = isnothing(pct_change) ? (close - prev_close) / prev_close * 100 : pct_change
    DataFrame(
        ticker         = [ticker],
        date           = [Dates.today()],
        close          = [close],
        prev_close     = [prev_close],
        pct_change     = [pc],
        volume         = [volume],
        notional_volume= [close * volume],
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

end
