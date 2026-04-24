using Test, DataFrames, Dates
using StockMonitor: compute_daily_metrics

function make_bars(rows)
    tickers = [r[1] for r in rows]
    dates   = [r[2] for r in rows]
    closes  = Float64[r[3] for r in rows]
    volumes = Int[r[4] for r in rows]
    DataFrame(
        ticker = tickers,
        date   = dates,
        open   = closes,
        high   = closes,
        low    = closes,
        close  = closes,
        volume = volumes,
    )
end

@testset "compute_daily_metrics" begin
    d1 = Date(2026, 4, 21)
    d2 = Date(2026, 4, 22)

    @testset "two sessions → one row with correct metrics" begin
        bars = make_bars([("AAA", d1, 100.0, 1_000_000),
                          ("AAA", d2, 110.0, 2_000_000)])
        out = compute_daily_metrics(bars)
        @test nrow(out) == 1
        row = out[1, :]
        @test row.ticker     == "AAA"
        @test row.date       == d2
        @test row.close      ≈ 110.0
        @test row.prev_close ≈ 100.0
        @test row.pct_change ≈ 10.0
        @test row.volume     == 2_000_000
        @test row.notional_volume ≈ 110.0 * 2_000_000
    end

    @testset "uses last two sessions when more than two exist" begin
        d3 = Date(2026, 4, 22)
        bars = make_bars([("AAA", Date(2026,4,18), 50.0,  500_000),
                          ("AAA", Date(2026,4,19), 100.0, 1_000_000),
                          ("AAA", d3,              120.0, 3_000_000)])
        out = compute_daily_metrics(bars)
        @test nrow(out) == 1
        @test out[1, :close]      ≈ 120.0
        @test out[1, :prev_close] ≈ 100.0
        @test out[1, :pct_change] ≈ 20.0
    end

    @testset "multiple tickers → one row each" begin
        bars = make_bars([("AAA", d1, 10.0, 100_000),
                          ("AAA", d2, 11.0, 200_000),
                          ("BBB", d1, 50.0, 100_000),
                          ("BBB", d2, 55.0, 300_000)])
        out = compute_daily_metrics(bars)
        @test sort(out.ticker) == ["AAA", "BBB"]
        @test nrow(out) == 2
    end

    @testset "tickers with only one session are dropped" begin
        bars = make_bars([("AAA", d2,  10.0, 100_000),
                          ("BBB", d1,  50.0, 100_000),
                          ("BBB", d2,  55.0, 300_000)])
        out = compute_daily_metrics(bars)
        @test out.ticker == ["BBB"]
    end

    @testset "tickers with NaN close are dropped" begin
        bars = make_bars([("AAA", d1, 10.0, 100_000),
                          ("AAA", d2, 10.0, 200_000),
                          ("BBB", d1, 50.0, 100_000),
                          ("BBB", d2, 55.0, 300_000)])
        bars[2, :close] = NaN   # corrupt latest close for AAA
        out = compute_daily_metrics(bars)
        @test out.ticker == ["BBB"]
    end

    @testset "tickers with zero prev_close are dropped" begin
        bars = make_bars([("AAA", d1, 0.0,  100_000),
                          ("AAA", d2, 10.0, 200_000),
                          ("BBB", d1, 50.0, 100_000),
                          ("BBB", d2, 55.0, 300_000)])
        out = compute_daily_metrics(bars)
        @test out.ticker == ["BBB"]
    end

    @testset "empty input returns empty DataFrame with expected columns" begin
        bars = DataFrame(ticker=String[], date=Date[], open=Float64[],
                         high=Float64[], low=Float64[], close=Float64[], volume=Int[])
        out = compute_daily_metrics(bars)
        @test nrow(out) == 0
        for col in [:ticker, :date, :close, :prev_close,
                    :pct_change, :volume, :notional_volume,
                    :pct_change_2d, :pct_change_5d, :pct_change_1m]
            @test col in Symbol.(names(out))
        end
    end

    @testset "pct_change_2d computed from 3 sessions" begin
        d3 = Date(2026, 4, 23)
        bars = make_bars([("AAA", d1, 90.0, 1_000_000),
                          ("AAA", d2, 100.0, 1_500_000),
                          ("AAA", d3, 110.0, 2_000_000)])
        out = compute_daily_metrics(bars)
        @test nrow(out) == 1
        @test out[1, :pct_change_2d] ≈ (110.0 - 90.0) / 90.0 * 100
    end

    @testset "pct_change_5d computed from 6 sessions" begin
        dates_6 = [Date(2026, 4, 1) + Day(i) for i in 0:5]
        rows_6  = [("AAA", dates_6[i], Float64(70 + i*10), 1_000_000) for i in 1:6]
        bars    = make_bars(rows_6)
        out     = compute_daily_metrics(bars)
        @test nrow(out) == 1
        # close_5ago = 80.0, close_today = 130.0
        @test out[1, :pct_change_5d] ≈ (130.0 - 80.0) / 80.0 * 100
    end

    @testset "pct_change_1m computed from 22 sessions" begin
        dates_22 = [Date(2026, 3, 1) + Day(i) for i in 0:21]
        closes_22 = vcat(fill(100.0, 21), [120.0])
        rows_22   = [("AAA", dates_22[i], closes_22[i], 1_000_000) for i in 1:22]
        bars      = make_bars(rows_22)
        out       = compute_daily_metrics(bars)
        @test nrow(out) == 1
        @test out[1, :pct_change_1m] ≈ (120.0 - 100.0) / 100.0 * 100
    end

    @testset "multi-period gains are NaN when insufficient history" begin
        bars = make_bars([("AAA", d1, 100.0, 1_000_000),
                          ("AAA", d2, 110.0, 2_000_000)])
        out = compute_daily_metrics(bars)
        @test nrow(out) == 1
        @test isnan(out[1, :pct_change_2d])
        @test isnan(out[1, :pct_change_5d])
        @test isnan(out[1, :pct_change_1m])
    end
end
