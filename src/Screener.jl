"""
Pure-function screener: filter a metrics DataFrame by criteria and rank results.

Expected input columns: ticker, date, close, prev_close, pct_change,
volume, notional_volume. Returns the same columns plus `rank` (1 = largest
|pct_change|). No side effects; returns a new DataFrame.
"""
function screen(df::DataFrame, criteria::Dict)::DataFrame
    direction   = get(criteria, "direction", "gainers")
    min_pct     = Float64(criteria["min_pct_change"])
    min_price   = Float64(criteria["min_price"])
    min_vol     = Float64(criteria["min_volume"])
    min_notional= Float64(criteria["min_notional_volume"])

    if direction == "gainers"
        dir_mask = df.pct_change .>= min_pct
    elseif direction == "losers"
        dir_mask = df.pct_change .<= -min_pct
    elseif direction == "both"
        dir_mask = abs.(df.pct_change) .>= min_pct
    else
        error("Unknown direction: $direction")
    end

    mask = dir_mask .&
           (df.close          .>= min_price) .&
           (df.volume         .>= min_vol) .&
           (df.notional_volume .>= min_notional)

    if haskey(criteria, "min_pct_change_2d")
        th   = Float64(criteria["min_pct_change_2d"])
        safe = .!isnan.(df.pct_change_2d)
        if direction == "gainers"
            mask = mask .& safe .& (df.pct_change_2d .>= th)
        elseif direction == "losers"
            mask = mask .& safe .& (df.pct_change_2d .<= -th)
        elseif direction == "both"
            mask = mask .& safe .& (abs.(df.pct_change_2d) .>= th)
        end
    end

    if haskey(criteria, "min_volume_ratio_5d")
        th   = Float64(criteria["min_volume_ratio_5d"])
        safe = .!isnan.(df.volume_ratio_5d)
        mask = mask .& safe .& (df.volume_ratio_5d .>= th)
    end

    before = nrow(df)
    filtered = df[mask, :]

    sorted = sort(filtered, :pct_change;
                  by=abs, rev=true, alg=MergeSort)
    result = copy(sorted)
    insertcols!(result, 1, :rank => 1:nrow(result))

    @info "screener: $before → $(nrow(result)) rows after filters"
    return result
end
