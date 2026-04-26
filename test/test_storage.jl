using Test, DataFrames, Dates, Parquet2
using StockMonitor: write_results, load_history, load_prev_rank_map

function sample_results(scan_date=Date(2026, 4, 22))
    DataFrame(
        rank            = [1, 2],
        ticker          = ["AAA", "BBB"],
        date            = [scan_date, scan_date],
        close           = [10.5, 22.0],
        prev_close      = [9.0, 20.0],
        pct_change      = [16.67, 10.0],
        volume          = [200_000, 150_000],
        notional_volume = [2_100_000.0, 3_300_000.0],
    )
end

@testset "Storage" begin
    @testset "write_results creates all three outputs" begin
        mktempdir() do tmpdir
            rdir = joinpath(tmpdir, "results")
            hdir = joinpath(tmpdir, "history")
            lat  = joinpath(rdir, "latest.parquet")
            d = Date(2026, 4, 22)
            write_results(sample_results(d), d, rdir, hdir, lat)

            @test isfile(joinpath(rdir, "gainers_2026-04-22.parquet"))
            @test isfile(lat)
            @test isfile(joinpath(hdir, "date=2026-04-22", "results.parquet"))
        end
    end

    @testset "dated parquet and latest parquet match" begin
        mktempdir() do tmpdir
            rdir = joinpath(tmpdir, "results")
            hdir = joinpath(tmpdir, "history")
            lat  = joinpath(rdir, "latest.parquet")
            d = Date(2026, 4, 22)
            df = sample_results(d)
            write_results(df, d, rdir, hdir, lat)

            dated  = DataFrame(Parquet2.Dataset(joinpath(rdir, "gainers_2026-04-22.parquet")); copycols=false)
            latest = DataFrame(Parquet2.Dataset(lat); copycols=false)
            @test sort(dated.ticker) == sort(latest.ticker)
            @test dated.close == latest.close
        end
    end

    @testset "history parquet contains scan_date column" begin
        mktempdir() do tmpdir
            rdir = joinpath(tmpdir, "results")
            hdir = joinpath(tmpdir, "history")
            lat  = joinpath(rdir, "latest.parquet")
            d = Date(2026, 4, 22)
            write_results(sample_results(d), d, rdir, hdir, lat)

            pq = joinpath(hdir, "date=2026-04-22", "results.parquet")
            rt = DataFrame(Parquet2.Dataset(pq); copycols=false)
            @test sort(rt.ticker) == ["AAA", "BBB"]
            @test "scan_date" in names(rt)
            @test all(==(string(d)), rt.scan_date)
        end
    end

    @testset "load_history round-trips two days" begin
        mktempdir() do tmpdir
            rdir = joinpath(tmpdir, "results")
            hdir = joinpath(tmpdir, "history")
            lat  = joinpath(rdir, "latest.parquet")
            d1 = Date(2026, 4, 21)
            d2 = Date(2026, 4, 22)
            write_results(sample_results(d1), d1, rdir, hdir, lat)
            write_results(sample_results(d2), d2, rdir, hdir, lat)

            hist = load_history(hdir)
            @test nrow(hist) == 4
            @test Set(hist.scan_date) == Set([string(d1), string(d2)])
            @test "ticker" in names(hist)
        end
    end

    @testset "load_history on empty dir returns empty DataFrame" begin
        mktempdir() do tmpdir
            out = load_history(joinpath(tmpdir, "nothing"))
            @test nrow(out) == 0
        end
    end

    @testset "load_prev_rank_map" begin
        @testset "nonexistent history_dir returns empty dict" begin
            mktempdir() do tmpdir
                @test load_prev_rank_map(joinpath(tmpdir, "nothing"), Date(2026, 4, 23)) == Dict{String,Int}()
            end
        end

        @testset "empty history_dir returns empty dict" begin
            mktempdir() do tmpdir
                hdir = joinpath(tmpdir, "history")
                mkpath(hdir)
                @test load_prev_rank_map(hdir, Date(2026, 4, 23)) == Dict{String,Int}()
            end
        end

        @testset "strict less-than: scan_date equals last partition returns prior" begin
            mktempdir() do tmpdir
                rdir = joinpath(tmpdir, "results")
                hdir = joinpath(tmpdir, "history")
                lat  = joinpath(rdir, "latest.parquet")
                d21 = Date(2026, 4, 21)
                d22 = Date(2026, 4, 22)
                d23 = Date(2026, 4, 23)
                write_results(sample_results(d21), d21, rdir, hdir, lat)
                write_results(sample_results(d22), d22, rdir, hdir, lat)
                write_results(sample_results(d23), d23, rdir, hdir, lat)

                m = load_prev_rank_map(hdir, Date(2026, 4, 23))
                @test m == Dict("AAA" => 1, "BBB" => 2)
            end
        end

        @testset "scan_date=2026-04-22 returns map from 2026-04-21" begin
            mktempdir() do tmpdir
                rdir = joinpath(tmpdir, "results")
                hdir = joinpath(tmpdir, "history")
                lat  = joinpath(rdir, "latest.parquet")
                d21 = Date(2026, 4, 21)
                d22 = Date(2026, 4, 22)
                d23 = Date(2026, 4, 23)
                write_results(sample_results(d21), d21, rdir, hdir, lat)
                write_results(sample_results(d22), d22, rdir, hdir, lat)
                write_results(sample_results(d23), d23, rdir, hdir, lat)

                m = load_prev_rank_map(hdir, Date(2026, 4, 22))
                @test m == Dict("AAA" => 1, "BBB" => 2)
            end
        end

        @testset "scan_date=2026-04-21 with no prior partition returns empty dict" begin
            mktempdir() do tmpdir
                rdir = joinpath(tmpdir, "results")
                hdir = joinpath(tmpdir, "history")
                lat  = joinpath(rdir, "latest.parquet")
                d21 = Date(2026, 4, 21)
                d22 = Date(2026, 4, 22)
                d23 = Date(2026, 4, 23)
                write_results(sample_results(d21), d21, rdir, hdir, lat)
                write_results(sample_results(d22), d22, rdir, hdir, lat)
                write_results(sample_results(d23), d23, rdir, hdir, lat)

                @test load_prev_rank_map(hdir, Date(2026, 4, 21)) == Dict{String,Int}()
            end
        end

        @testset "prior partition with zero rows returns empty dict" begin
            mktempdir() do tmpdir
                hdir = joinpath(tmpdir, "history")
                part = joinpath(hdir, "date=2026-04-21")
                mkpath(part)
                empty_df = DataFrame(rank=Int[], ticker=String[])
                Parquet2.writefile(joinpath(part, "results.parquet"), empty_df)

                @test load_prev_rank_map(hdir, Date(2026, 4, 23)) == Dict{String,Int}()
            end
        end

        @testset "rank values match the parquet data" begin
            mktempdir() do tmpdir
                rdir = joinpath(tmpdir, "results")
                hdir = joinpath(tmpdir, "history")
                lat  = joinpath(rdir, "latest.parquet")
                d21 = Date(2026, 4, 21)
                d22 = Date(2026, 4, 22)
                write_results(sample_results(d21), d21, rdir, hdir, lat)
                write_results(sample_results(d22), d22, rdir, hdir, lat)

                m = load_prev_rank_map(hdir, Date(2026, 4, 22))
                @test m["AAA"] == 1
                @test m["BBB"] == 2
            end
        end
    end
end
