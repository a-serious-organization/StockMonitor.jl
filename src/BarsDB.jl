"""
Date-partitioned bars cache at data/bars/date=YYYY-MM-DD/bars.parquet.

bar_partition_path  — path for a single date's partition file.
existing_bar_dates  — list of dates that have a partition on disk.
write_bars_partitions — write (or skip/overwrite) per-date partition files.
load_bars           — read all partitions in a date window into one DataFrame.
"""

using Parquet2

function bar_partition_path(bars_dir::AbstractString, d::Date)::String
    joinpath(bars_dir, "date=$(Dates.format(d, "yyyy-mm-dd"))", "bars.parquet")
end


function existing_bar_dates(bars_dir::AbstractString)::Vector{Date}
    isdir(bars_dir) || return Date[]
    dates = Date[]
    for entry in readdir(bars_dir)
        if startswith(entry, "date=") && isdir(joinpath(bars_dir, entry))
            date_str = entry[6:end]   # strip "date="
            try
                push!(dates, Date(date_str, "yyyy-mm-dd"))
            catch
            end
        end
    end
    return sort(dates)
end


function write_bars_partitions(
        df::DataFrame,
        bars_dir::AbstractString;
        overwrite::Bool = false,
    )::Nothing

    isempty(df) && return nothing

    for gdf in groupby(df, :date)
        d = gdf[1, :date]
        pq_path = bar_partition_path(bars_dir, d)
        if !overwrite && isfile(pq_path)
            continue
        end
        mkpath(dirname(pq_path))
        Parquet2.writefile(pq_path, DataFrame(gdf))
        @info "wrote $pq_path ($(nrow(gdf)) rows)"
    end

    return nothing
end


function load_bars(bars_dir::AbstractString, start::Date, end_::Date)::DataFrame
    empty_schema = DataFrame(
        ticker = String[],
        date   = Date[],
        open   = Float64[],
        high   = Float64[],
        low    = Float64[],
        close  = Float64[],
        volume = Int[],
    )

    isdir(bars_dir) || return empty_schema

    parts = String[]
    for d in existing_bar_dates(bars_dir)
        start <= d <= end_ || continue
        p = bar_partition_path(bars_dir, d)
        isfile(p) && push!(parts, p)
    end

    isempty(parts) && return empty_schema

    frames = [DataFrame(Parquet2.Dataset(p); copycols=false) for p in parts]
    return vcat(frames...; cols=:union)
end
