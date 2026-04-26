using Test, DataFrames, Dates
using StockMonitor: _build_chart_json

function _mk_partitions(tmpdir, dates)
    for d in dates
        mkpath(joinpath(tmpdir, "date=$(Dates.format(d, "yyyy-mm-dd"))"))
    end
end

@testset "Site chart JSON" begin

    @testset "days with zero gainers still get a bar (with count 0)" begin
        mktempdir() do tmpdir
            d1, d2, d3 = Date(2026, 4, 21), Date(2026, 4, 22), Date(2026, 4, 23)
            _mk_partitions(tmpdir, [d1, d2, d3])
            # history only has rows for d2 (d1 and d3 had zero gainers)
            history = DataFrame(
                ticker    = ["AAA", "BBB"],
                scan_date = [string(d2), string(d2)],
            )
            json = _build_chart_json(history, tmpdir, 14)
            @test occursin("\"2026-04-21\"", json)
            @test occursin("\"2026-04-22\"", json)
            @test occursin("\"2026-04-23\"", json)
            @test occursin("[0,2,0]", json)
        end
    end

    @testset "all-empty history but on-disk partitions → all zeros" begin
        mktempdir() do tmpdir
            _mk_partitions(tmpdir, [Date(2026, 4, 21), Date(2026, 4, 22)])
            json = _build_chart_json(DataFrame(), tmpdir, 14)
            @test occursin("\"2026-04-21\"", json)
            @test occursin("\"2026-04-22\"", json)
            @test occursin("[0,0]", json)
        end
    end

    @testset "tail trims to last `window` scan dates" begin
        mktempdir() do tmpdir
            dates = [Date(2026, 4, d) for d in 10:25]   # 16 dates
            _mk_partitions(tmpdir, dates)
            json = _build_chart_json(DataFrame(), tmpdir, 14)
            @test !occursin("\"2026-04-10\"", json)
            @test !occursin("\"2026-04-11\"", json)
            @test occursin("\"2026-04-12\"", json)
            @test occursin("\"2026-04-25\"", json)
        end
    end

    @testset "missing history_dir returns empty payload" begin
        mktempdir() do tmpdir
            json = _build_chart_json(DataFrame(), joinpath(tmpdir, "nope"), 14)
            @test json == """{"labels":[],"counts":[]}"""
        end
    end

    @testset "non-partition entries in history_dir are ignored" begin
        mktempdir() do tmpdir
            _mk_partitions(tmpdir, [Date(2026, 4, 22)])
            mkdir(joinpath(tmpdir, "not-a-partition"))
            touch(joinpath(tmpdir, "stray-file"))
            json = _build_chart_json(DataFrame(), tmpdir, 14)
            @test occursin("\"2026-04-22\"", json)
            @test !occursin("not-a-partition", json)
            @test !occursin("stray-file", json)
        end
    end

end
