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

        @testset "bars_dir filter: weekend prev partition is skipped" begin
            mktempdir() do tmpdir
                hdir = joinpath(tmpdir, "history")
                bdir = joinpath(tmpdir, "bars")
                # Thu 4-23, Fri 4-24, Sat 4-25, Sun 4-26 — each tagged with a distinct ticker
                for (d, top) in [(Date(2026, 4, 23), "THU"), (Date(2026, 4, 24), "FRI"),
                                 (Date(2026, 4, 25), "SAT"), (Date(2026, 4, 26), "SUN")]
                    part = joinpath(hdir, "date=$(Dates.format(d, "yyyy-mm-dd"))")
                    mkpath(part)
                    Parquet2.writefile(joinpath(part, "results.parquet"),
                                       DataFrame(rank=[1], ticker=[top]))
                end
                # Bars only on trading days (Thu, Fri)
                mkpath(joinpath(bdir, "date=2026-04-23"))
                mkpath(joinpath(bdir, "date=2026-04-24"))

                # No bars_dir → calendar logic → prev = Sat
                @test load_prev_rank_map(hdir, Date(2026, 4, 26)) == Dict("SAT" => 1)

                # With bars_dir → effective_today = Fri, prev trading day = Thu
                @test load_prev_rank_map(hdir, Date(2026, 4, 26); bars_dir=bdir) == Dict("THU" => 1)
            end
        end

        @testset "bars_dir filter: today is trading day, prev jumps over weekend" begin
            mktempdir() do tmpdir
                hdir = joinpath(tmpdir, "history")
                bdir = joinpath(tmpdir, "bars")
                for (d, top) in [(Date(2026, 4, 23), "THU"), (Date(2026, 4, 24), "FRI"),
                                 (Date(2026, 4, 27), "MON")]
                    part = joinpath(hdir, "date=$(Dates.format(d, "yyyy-mm-dd"))")
                    mkpath(part)
                    Parquet2.writefile(joinpath(part, "results.parquet"),
                                       DataFrame(rank=[1], ticker=[top]))
                end
                for d in [Date(2026, 4, 23), Date(2026, 4, 24), Date(2026, 4, 27)]
                    mkpath(joinpath(bdir, "date=$(Dates.format(d, "yyyy-mm-dd"))"))
                end

                @test load_prev_rank_map(hdir, Date(2026, 4, 27); bars_dir=bdir) == Dict("FRI" => 1)
            end
        end

        @testset "bars_dir filter: holiday gap is skipped" begin
            mktempdir() do tmpdir
                hdir = joinpath(tmpdir, "history")
                bdir = joinpath(tmpdir, "bars")
                # Imagine Mon was a holiday: history partition was still written (scan ran), but
                # Yahoo returned no bars, so no bars partition for Mon.
                for (d, top) in [(Date(2026, 4, 23), "THU"), (Date(2026, 4, 24), "FRI"),
                                 (Date(2026, 4, 27), "MON_HOL"), (Date(2026, 4, 28), "TUE")]
                    part = joinpath(hdir, "date=$(Dates.format(d, "yyyy-mm-dd"))")
                    mkpath(part)
                    Parquet2.writefile(joinpath(part, "results.parquet"),
                                       DataFrame(rank=[1], ticker=[top]))
                end
                for d in [Date(2026, 4, 23), Date(2026, 4, 24), Date(2026, 4, 28)]
                    mkpath(joinpath(bdir, "date=$(Dates.format(d, "yyyy-mm-dd"))"))
                end

                @test load_prev_rank_map(hdir, Date(2026, 4, 28); bars_dir=bdir) == Dict("FRI" => 1)
            end
        end

        @testset "bars_dir empty: falls back to calendar logic" begin
            mktempdir() do tmpdir
                hdir = joinpath(tmpdir, "history")
                bdir = joinpath(tmpdir, "bars")
                mkpath(bdir)
                for (d, top) in [(Date(2026, 4, 24), "FRI"), (Date(2026, 4, 25), "SAT")]
                    part = joinpath(hdir, "date=$(Dates.format(d, "yyyy-mm-dd"))")
                    mkpath(part)
                    Parquet2.writefile(joinpath(part, "results.parquet"),
                                       DataFrame(rank=[1], ticker=[top]))
                end

                @test load_prev_rank_map(hdir, Date(2026, 4, 26); bars_dir=bdir) == Dict("SAT" => 1)
            end
        end
    end
end
