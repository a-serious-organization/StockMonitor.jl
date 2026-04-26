using Test, DataFrames, Dates
using StockMonitor: _build_chart_json

@testset "Site chart JSON" begin

    @testset "trailing window has exactly `window` labels ending on scan_date" begin
        scan_date = Date(2026, 4, 26)
        json = _build_chart_json(DataFrame(), scan_date, 14)
        # 14 calendar days: 4-13 through 4-26
        @test occursin("\"2026-04-13\"", json)
        @test occursin("\"2026-04-26\"", json)
        @test !occursin("\"2026-04-12\"", json)
        @test !occursin("\"2026-04-27\"", json)
    end

    @testset "all-empty history → all zeros over the window" begin
        json = _build_chart_json(DataFrame(), Date(2026, 4, 26), 14)
        @test occursin("\"counts\":[0,0,0,0,0,0,0,0,0,0,0,0,0,0]", json)
    end

    @testset "days with zero gainers and weekends both render as 0" begin
        # scan_date = Sun 2026-04-26; weekend within window: 4-25, 4-26, 4-19, 4-18
        scan_date = Date(2026, 4, 26)
        history = DataFrame(
            ticker    = ["AAA", "BBB", "CCC"],
            scan_date = ["2026-04-23", "2026-04-23", "2026-04-24"],
        )
        json = _build_chart_json(history, scan_date, 14)
        # 4-23 has 2, 4-24 has 1, every other day has 0
        # window: 4-13 ... 4-26 (14 days)
        # expected counts in order: 0,0,0,0,0,0,0,0,0,0,2,1,0,0
        @test occursin("\"counts\":[0,0,0,0,0,0,0,0,0,0,2,1,0,0]", json)
    end

    @testset "history rows outside the trailing window are dropped" begin
        scan_date = Date(2026, 4, 26)
        history = DataFrame(
            ticker    = ["OLD"],
            scan_date = ["2026-03-01"],   # well outside the 14-day window
        )
        json = _build_chart_json(history, scan_date, 14)
        @test !occursin("\"2026-03-01\"", json)
        @test occursin("\"counts\":[0,0,0,0,0,0,0,0,0,0,0,0,0,0]", json)
    end

    @testset "counts payload reflects gainer counts on matching days" begin
        scan_date = Date(2026, 4, 26)
        history = DataFrame(
            ticker    = ["A","B","C","D","E","X","Y","Z"],
            scan_date = ["2026-04-25","2026-04-25","2026-04-25","2026-04-25","2026-04-25",
                         "2026-04-26","2026-04-26","2026-04-26"],
        )
        json = _build_chart_json(history, scan_date, 14)
        @test occursin("5,3]", json)
    end

    @testset "smaller window respected" begin
        scan_date = Date(2026, 4, 26)
        json = _build_chart_json(DataFrame(), scan_date, 3)
        @test occursin("\"2026-04-24\"", json)
        @test occursin("\"2026-04-25\"", json)
        @test occursin("\"2026-04-26\"", json)
        @test !occursin("\"2026-04-23\"", json)
        @test occursin("\"counts\":[0,0,0]", json)
    end

end
