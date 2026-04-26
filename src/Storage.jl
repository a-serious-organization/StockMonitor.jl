"""
Parquet output writers and history loader.

Layout:
    results_dir/gainers_YYYY-MM-DD.parquet
    results_dir/latest.parquet
    history_dir/date=YYYY-MM-DD/results.parquet
"""

using Parquet2

function write_results(
        df::DataFrame,
        scan_date::Date,
        results_dir::AbstractString,
        history_dir::AbstractString,
        latest_parquet::AbstractString,
    )::Nothing

    mkpath(results_dir)
    mkpath(dirname(latest_parquet))

    dated = joinpath(results_dir, "gainers_$(Dates.format(scan_date, "yyyy-mm-dd")).parquet")
    Parquet2.writefile(dated, df)
    @info "wrote $dated ($(nrow(df)) rows)"

    Parquet2.writefile(latest_parquet, df)
    @info "wrote $latest_parquet"

    part_dir = joinpath(history_dir, "date=$(Dates.format(scan_date, "yyyy-mm-dd"))")
    mkpath(part_dir)
    pq_path = joinpath(part_dir, "results.parquet")
    with_date = copy(df)
    with_date[!, :scan_date] .= string(scan_date)
    Parquet2.writefile(pq_path, with_date)
    @info "wrote $pq_path"

    return nothing
end


function load_prev_rank_map(history_dir::AbstractString, scan_date::Date)::Dict{String,Int}
    isdir(history_dir) || return Dict{String,Int}()

    candidates = [
        Date(d[6:end])
        for d in readdir(history_dir)
        if startswith(d, "date=") && tryparse(Date, d[6:end]) !== nothing
    ]
    filter!(d -> d < scan_date, candidates)
    isempty(candidates) && return Dict{String,Int}()

    prev = maximum(candidates)
    p = joinpath(history_dir, "date=$(Dates.format(prev, "yyyy-mm-dd"))", "results.parquet")
    isfile(p) || return Dict{String,Int}()

    df = DataFrame(Parquet2.Dataset(p); copycols=false)
    ("rank" in names(df) && "ticker" in names(df)) || return Dict{String,Int}()
    nrow(df) == 0 && return Dict{String,Int}()

    return Dict(string(row.ticker) => Int(row.rank) for row in eachrow(df))
end


function load_history(history_dir::AbstractString)::DataFrame
    isdir(history_dir) || return DataFrame()

    parts = filter(isfile, [
        joinpath(history_dir, d, "results.parquet")
        for d in readdir(history_dir)
        if startswith(d, "date=")
    ]) |> sort

    isempty(parts) && return DataFrame()

    frames = [DataFrame(Parquet2.Dataset(p); copycols=false) for p in parts]
    return vcat(frames...; cols=:union)
end
