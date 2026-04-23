using Test, Dates
using StockMonitor

const SAMPLE_IWV_CSV = """iShares Russell 3000 ETF
Fund Holdings as of,"Apr 22, 2026"
Inception Date,"May 22, 2000"
Shares Outstanding,
Stock,-
\x20
Ticker,Name,Sector,Asset Class,Market Value,Weight (%),Notional Value,Quantity,Price,Location,Exchange,Currency,FX Rate,Market Currency,Accrual Date
AAPL,APPLE INC,Information Technology,Equity,"1000000","2.50","1000000","1000","100.00",United States,NASDAQ,USD,1.00,USD,-
BRK.B,BERKSHIRE HATHAWAY B,Financials,Equity,"500000","1.20","500000","500","500.00",United States,NYSE,USD,1.00,USD,-
MSFT,MICROSOFT CORP,Information Technology,Equity,"800000","2.00","800000","800","300.00",United States,NASDAQ,USD,1.00,USD,-
XCASH,USD CASH,Cash,Cash,"100000","0.10","100000","100","1.00",-,-,USD,1.00,USD,-
lowercase,BOGUS LOWER,-,Equity,"1","0","1","1","1",US,NASDAQ,USD,1.00,USD,-
"""

@testset "Universe" begin

    @testset "parse: BRK.B normalized to BRK-B, no dots in output" begin
        tickers = StockMonitor._parse_iwv_holdings(SAMPLE_IWV_CSV)
        @test "BRK-B" in tickers
        @test !any(contains(t, ".") for t in tickers)
    end

    @testset "parse: equity rows kept, cash rows dropped" begin
        tickers = StockMonitor._parse_iwv_holdings(SAMPLE_IWV_CSV)
        @test "AAPL"  in tickers
        @test "BRK-B" in tickers
        @test "MSFT"  in tickers
        @test !("XCASH" in tickers)
    end

    @testset "parse: lowercase ticker fails regex and is dropped" begin
        tickers = StockMonitor._parse_iwv_holdings(SAMPLE_IWV_CSV)
        @test !("lowercase" in tickers)
        @test !any(any(islowercase, t) for t in tickers)
    end

    @testset "load_universe: fresh cache used without network" begin
        mktempdir() do tmpdir
            cache = joinpath(tmpdir, "u.csv")
            write(cache, "ticker\nAAPL\nMSFT\n")
            cfg = Dict("cache_path" => cache, "cache_max_age_days" => 30)
            tickers = load_universe(cfg)
            @test tickers == ["AAPL", "MSFT"]
        end
    end

    @testset "load_universe: force_refresh bypasses fresh cache (uses injected CSV)" begin
        mktempdir() do tmpdir
            cache = joinpath(tmpdir, "u.csv")
            write(cache, "ticker\nCACHED\n")
            # Inject test CSV so no real network call is made
            cfg = Dict(
                "cache_path"       => cache,
                "cache_max_age_days" => 30,
                "_test_csv"        => SAMPLE_IWV_CSV,
            )
            tickers = load_universe(cfg; force_refresh=true)
            @test "AAPL"     in tickers
            @test !("CACHED" in tickers)
        end
    end

    @testset "load_universe: stale cache refreshes (zero max_age forces refresh)" begin
        mktempdir() do tmpdir
            cache = joinpath(tmpdir, "u.csv")
            write(cache, "ticker\nOLD\n")
            cfg = Dict(
                "cache_path"       => cache,
                "cache_max_age_days" => 0,   # always stale
                "_test_csv"        => SAMPLE_IWV_CSV,
            )
            tickers = load_universe(cfg)
            @test "AAPL"   in tickers
            @test !("OLD" in tickers)
        end
    end

end
