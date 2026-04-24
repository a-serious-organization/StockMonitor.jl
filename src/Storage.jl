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


function load_history(history_dir::AbstractString)::DataFrame
    isdir(history_dir) || return DataFrame()

    parts = filter(isfile, [
        joinpath(history_dir, d, "results.parquet")
        for d in readdir(history_dir)
        if startswith(d, "date=")
    ]) |> sort

    isempty(parts) && return DataFrame()

    frames = [DataFrame(Parquet2.Dataset(p); copycols=false) for p in parts]
    return vcat(frames...)
end
