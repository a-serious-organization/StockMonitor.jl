using Test, Dates
using StockMonitor: _parse_date_flag, _parse_date_flags

@testset "CLI date-flag parsing" begin

    @testset "single date YYYYMMDD" begin
        result = _parse_date_flag("20260424")
        @test result == [Date(2026, 4, 24)]
    end

    @testset "date range YYYYMMDD:YYYYMMDD produces inclusive vector" begin
        result = _parse_date_flag("20260420:20260424")
        @test length(result) == 5
        @test result[1] == Date(2026, 4, 20)
        @test result[end] == Date(2026, 4, 24)
        @test result == collect(Date(2026, 4, 20):Day(1):Date(2026, 4, 24))
    end

    @testset "malformed input throws with message" begin
        @test_throws ErrorException _parse_date_flag("notadate")
        @test_throws ErrorException _parse_date_flag("2026042")      # too short
        @test_throws ErrorException _parse_date_flag("202604240")    # too long
        @test_throws ErrorException _parse_date_flag("20260424:20260420")  # end < start
        @test_throws ErrorException _parse_date_flag("2026-04-24")   # wrong separator
    end

    @testset "_parse_date_flags unions repeated values and dedupes" begin
        # empty input → empty output
        @test _parse_date_flags(String[]) == Date[]

        # single value
        @test _parse_date_flags(["20260424"]) == [Date(2026, 4, 24)]

        # union of one date + a range
        got = _parse_date_flags(["20260415", "20260420:20260422"])
        @test got == [Date(2026, 4, 15), Date(2026, 4, 20), Date(2026, 4, 21), Date(2026, 4, 22)]

        # overlapping ranges are deduped
        got = _parse_date_flags(["20260420:20260422", "20260421:20260423"])
        @test got == collect(Date(2026, 4, 20):Day(1):Date(2026, 4, 23))

        # malformed entry in the list propagates the error
        @test_throws ErrorException _parse_date_flags(["20260424", "notadate"])
    end

end
