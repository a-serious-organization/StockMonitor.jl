"""
Static HTML dashboard generator. Writes data/site/index.html after each scan.
Chart.js bar chart of daily gainer counts loaded via CDN.
"""

using Dates

const _HISTORY_WINDOW = 14

const _PAGE_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Stock Monitor — {{scan_date}}</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  :root { color-scheme: light dark; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    margin: 0; padding: 2rem; max-width: 1100px; margin-inline: auto; line-height: 1.4;
  }
  header h1 { margin: 0 0 0.25rem; font-size: 1.6rem; }
  header .sub { color: #666; font-size: 0.9rem; }
  section { margin-top: 2rem; }
  h2 { font-size: 1.2rem; border-bottom: 1px solid #ddd; padding-bottom: 0.3rem; }
  table.gainers { border-collapse: collapse; width: 100%; font-size: 0.95rem; }
  table.gainers th, table.gainers td {
    padding: 0.5rem 0.75rem; text-align: right; border-bottom: 1px solid #eee;
  }
  table.gainers th {
    position: sticky; top: 0; background: #fafafa; font-weight: 600;
  }
  table.gainers th:nth-child(2), table.gainers td:nth-child(2) { text-align: left; }
  .empty { padding: 2rem; text-align: center; color: #777; border: 1px dashed #ccc; border-radius: 0.5rem; }
  .criteria { font-size: 0.85rem; color: #555; }
  #chart-wrap { position: relative; height: 280px; }
  footer { margin-top: 3rem; font-size: 0.8rem; color: #888; }
  footer a { color: #555; }
</style>
</head>
<body>
<header>
  <h1>Daily Stock Monitor</h1>
  <div class="sub">Scan for <strong>{{scan_date}}</strong> &middot; {{result_count}} gainer(s)</div>
  <div class="criteria">Criteria: {{criteria_summary}}</div>
</header>
<section>
  <h2>Today's gainers</h2>
  {{results_block}}
</section>
<section>
  <h2>Daily gainer count (last {{history_window}} days)</h2>
  <div id="chart-wrap"><canvas id="counts"></canvas></div>
</section>
<footer>
  Generated {{scan_date}}. &middot;
  <a href="../results/latest.csv">latest.csv</a> &middot;
  <a href="https://github.com/a-serious-organization/StockMonitor.jl">repo</a>
</footer>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<script>
  const chartData = {{chart_data_json}};
  const ctx = document.getElementById('counts').getContext('2d');
  new Chart(ctx, {
    type: 'bar',
    data: {
      labels: chartData.labels,
      datasets: [{
        label: 'Gainers',
        data: chartData.counts,
        backgroundColor: 'rgba(46,160,67,0.75)',
        borderColor: 'rgba(46,160,67,1)',
        borderWidth: 1,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: { y: { beginAtZero: true, ticks: { precision: 0 } } },
    },
  });
</script>
</body>
</html>
"""


function render_site(
        results::DataFrame,
        history::DataFrame,
        config::Dict,
        scan_date::Date,
        site_dir::AbstractString,
    )::Nothing

    mkpath(site_dir)

    results_block   = _format_results_block(results)
    chart_data_json = _build_chart_json(history, _HISTORY_WINDOW)
    criteria_str    = _format_criteria(get(config, "criteria", Dict()))

    page = _PAGE_TEMPLATE
    page = replace(page, "{{scan_date}}"        => string(scan_date))
    page = replace(page, "{{result_count}}"     => string(nrow(results)))
    page = replace(page, "{{criteria_summary}}" => criteria_str)
    page = replace(page, "{{results_block}}"    => results_block)
    page = replace(page, "{{history_window}}"   => string(_HISTORY_WINDOW))
    page = replace(page, "{{chart_data_json}}"  => chart_data_json)

    out = joinpath(site_dir, "index.html")
    write(out, page)
    @info "wrote $out"
    return nothing
end


function _format_results_block(df::DataFrame)::String
    nrow(df) == 0 && return """<div class="empty">No gainers today.</div>"""

    io = IOBuffer()
    println(io, """<table class="gainers"><thead><tr>""")
    for col in ["rank","ticker","close","prev_close","pct_change","volume","notional_volume"]
        print(io, "<th>$col</th>")
    end
    println(io, "</tr></thead><tbody>")
    for row in eachrow(df)
        println(io, "<tr>",
            "<td>$(row.rank)</td>",
            "<td>$(row.ticker)</td>",
            "<td>$(round(row.close, digits=2))</td>",
            "<td>$(round(row.prev_close, digits=2))</td>",
            "<td>$(round(row.pct_change, digits=2))%</td>",
            "<td>$(format_int(row.volume))</td>",
            "<td>\$$(format_int(round(Int, row.notional_volume)))</td>",
            "</tr>")
    end
    println(io, "</tbody></table>")
    return String(take!(io))
end

format_int(n) = replace(string(Int(n)), r"(?<=\d)(?=(\d{3})+$)" => ",")


function _build_chart_json(history::DataFrame, window::Int)::String
    if isempty(history) || !("scan_date" in names(history))
        return """{"labels":[],"counts":[]}"""
    end
    counts = combine(groupby(history, :scan_date), nrow => :n)
    sort!(counts, :scan_date)
    tail = last(counts, window)
    labels = ["\"$(row.scan_date)\"" for row in eachrow(tail)]
    values = [string(row.n) for row in eachrow(tail)]
    return """{"labels":[$(join(labels,","))],"counts":[$(join(values,","))]}"""
end


function _format_criteria(crit::Dict)::String
    parts = String[]
    haskey(crit, "min_pct_change")      && push!(parts, "pct≥$(crit["min_pct_change"])%")
    haskey(crit, "min_price")           && push!(parts, "price≥\$$(crit["min_price"])")
    haskey(crit, "min_volume")          && push!(parts, "vol≥$(crit["min_volume"])")
    haskey(crit, "min_notional_volume") && push!(parts, "notional≥\$$(crit["min_notional_volume"])")
    haskey(crit, "direction")           && push!(parts, "direction=$(crit["direction"])")
    return join(parts, ", ")
end
