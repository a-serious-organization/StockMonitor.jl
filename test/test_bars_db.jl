using Test, DataFrames, Dates, Parquet2
using StockMonitor: bar_partition_path, existing_bar_dates,
                    write_bars_partitions, load_bars

function sample_bars(dates=nothing)
    if isnothing(dates)
        dates = [Date(2026, 4, 21), Date(2026, 4, 22), Date(2026, 4, 23)]
    end
    n = length(dates)
    DataFrame(
        ticker = repeat(["AAA", "BBB"], n),
        date   = repeat(dates; inner=2),
        open   = fill(10.0, 2n),
        high   = fill(11.0, 2n),
        low    = fill(9.0,  2n),
        close  = fill(10.5, 2n),
        volume = fill(100_000, 2n),
    )
end

@testset "BarsDB" begin

    @testset "write_bars_partitions creates date=YYYY-MM-DD/bars.parquet" begin
        mktempdir() do tmpdir
            df = sample_bars()
            write_bars_partitions(df, tmpdir)
            @test isfile(joinpath(tmpdir, "date=2026-04-21", "bars.parquet"))
            @test isfile(joinpath(tmpdir, "date=2026-04-22", "bars.parquet"))
            @test isfile(joinpath(tmpdir, "date=2026-04-23", "bars.parquet"))
        end
    end

    @testset "round-trip: write then load returns same rows" begin
        mktempdir() do tmpdir
            df = sample_bars()
            write_bars_partitions(df, tmpdir)
            loaded = load_bars(tmpdir, Date(2026, 4, 21), Date(2026, 4, 23))
            @test nrow(loaded) == nrow(df)
            @test Set(loaded.ticker) == Set(df.ticker)
        end
    end

    @testset "overwrite=false on existing partition is a no-op" begin
        mktempdir() do tmpdir
            d = Date(2026, 4, 22)
            df1 = sample_bars([d])
            write_bars_partitions(df1, tmpdir; overwrite=false)

            pq_path = bar_partition_path(tmpdir, d)
            mtime1 = stat(pq_path).mtime
            sleep(0.1)

            df2 = copy(df1)
            df2[!, :close] .= 99.0
            write_bars_partitions(df2, tmpdir; overwrite=false)

            @test stat(pq_path).mtime == mtime1   # file not touched
            loaded = load_bars(tmpdir, d, d)
            @test all(==(10.5), loaded.close)      # original data intact
        end
    end

    @testset "overwrite=true on existing partition replaces the file" begin
        mktempdir() do tmpdir
            d = Date(2026, 4, 22)
            df1 = sample_bars([d])
            write_bars_partitions(df1, tmpdir; overwrite=false)

            df2 = copy(df1)
            df2[!, :close] .= 99.0
            write_bars_partitions(df2, tmpdir; overwrite=true)

            loaded = load_bars(tmpdir, d, d)
            @test all(==(99.0), loaded.close)
        end
    end

    @testset "existing_bar_dates returns sorted Date values for present partitions" begin
        mktempdir() do tmpdir
            dates = [Date(2026, 4, 21), Date(2026, 4, 23)]
            write_bars_partitions(sample_bars(dates), tmpdir)
            got = existing_bar_dates(tmpdir)
            @test got == sort(dates)
            # non-partition dirs are ignored
            mkdir(joinpath(tmpdir, "not-a-partition"))
            @test existing_bar_dates(tmpdir) == sort(dates)
        end
    end

    @testset "load_bars on empty dir returns empty DataFrame with canonical schema" begin
        mktempdir() do tmpdir
            out = load_bars(joinpath(tmpdir, "nonexistent"), Date(2026, 4, 1), Date(2026, 4, 30))
            @test nrow(out) == 0
            for col in [:ticker, :date, :open, :high, :low, :close, :volume]
                @test col in Symbol.(names(out))
            end
        end
    end

    @testset "load_bars filters partitions by date window" begin
        mktempdir() do tmpdir
            dates = [Date(2026, 4, 20), Date(2026, 4, 21),
                     Date(2026, 4, 22), Date(2026, 4, 23)]
            write_bars_partitions(sample_bars(dates), tmpdir)
            loaded = load_bars(tmpdir, Date(2026, 4, 21), Date(2026, 4, 22))
            @test Set(loaded.date) == Set([Date(2026, 4, 21), Date(2026, 4, 22)])
            @test Date(2026, 4, 20) ∉ loaded.date
            @test Date(2026, 4, 23) ∉ loaded.date
        end
    end

end
