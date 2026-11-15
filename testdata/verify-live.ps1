$ErrorActionPreference = 'Stop'
$base = 'http://localhost:8080'
$prom = 'http://localhost:9090'
$cases = @(
    @{ zip = '12601'; days = 5 },
    @{ zip = '02108'; days = 1 },
    @{ zip = '10001'; days = 3 },
    @{ zip = '60601'; days = 2 }
)
$cases | ConvertTo-Json | Set-Content -Encoding utf8 (Join-Path $PSScriptRoot 'forecast-inputs.json')
$tasks = foreach ($item in $cases) {
    Start-ThreadJob -ArgumentList $base, $item -ScriptBlock {
        param($base, $item)
        $ErrorActionPreference = 'Stop'
        $start = [DateTimeOffset]::UtcNow
        $body = $item | ConvertTo-Json -Compress
        $accepted = Invoke-RestMethod -Method Post -ContentType 'application/json' -TimeoutSec 5 -Body $body "$base/forecasts"
        $deadline = $start.AddSeconds(40)
        do {
            $result = Invoke-RestMethod -TimeoutSec 5 "$base/forecasts/$($accepted.id)"
            if ($result.status -in @('completed','failed')) { break }
            Start-Sleep -Milliseconds 250
        } while ([DateTimeOffset]::UtcNow -lt $deadline)
        if ($result.status -ne 'completed' -or !$result.kafka -or $result.forecast.zip -ne $item.zip -or $result.forecast.days -ne $item.days) { throw "Unexpected result for $body" }
        [pscustomobject]@{
            input = $item
            sent_at_utc = $start.ToString('o')
            observed_elapsed_ms = [int]([DateTimeOffset]::UtcNow - $start).TotalMilliseconds
            server_elapsed_ms = [int](([DateTimeOffset]$result.updated_at) - ([DateTimeOffset]$result.created_at)).TotalMilliseconds
            result = $result
        }
    }
}
try { $results = @(Receive-Job -Job $tasks -Wait -ErrorAction Stop) }
finally { Remove-Job -Job $tasks -Force }
if ($results.Count -ne $cases.Count) { throw 'Missing completed forecasts' }
function Query-Prometheus($query) {
    $encoded = [uri]::EscapeDataString($query)
    (Invoke-RestMethod -TimeoutSec 5 "$prom/api/v1/query?query=$encoded").data.result
}
$checks = foreach ($entry in $results) {
    $r = $entry.result
    $id = $r.id
    $page = (Invoke-WebRequest -TimeoutSec 5 "$base/requests/$id").Content
    $rows = [regex]::Matches($page,'<th scope="row">').Count
    $embeds = [regex]::Matches($page,"var-forecast_request=$id").Count
    if ($rows -ne $r.forecast.points.Count -or $embeds -ne 2) { throw "Website mismatch for $id" }
    $metrics = @{}
    foreach ($name in @('valid_time_seconds','temperature_celsius','wind_meters_per_second')) {
        $query = 'weather_request_forecast_' + $name + '{request_id="' + $id + '"}'
        $deadline = (Get-Date).AddSeconds(14)
        do {
            $samples = @(Query-Prometheus $query)
            if ($samples.Count -eq $rows) { break }
            Start-Sleep -Milliseconds 600
        } while ((Get-Date) -lt $deadline)
        if ($samples.Count -ne $rows) { throw "Missing $name samples for $id" }
        foreach ($sample in $samples) {
            $point = $r.forecast.points[([int]$sample.metric.step - 1)]
            $expected = switch ($name) {
                'valid_time_seconds' { ([DateTimeOffset]$point.time).ToUnixTimeSeconds() }
                default { $point.$name }
            }
            if ([Math]::Abs([double]$sample.value[1] - [double]$expected) -gt 0.000001) { throw "Mismatched $name for $id" }
        }
        $metrics[$name] = $samples.Count
    }
    [ordered]@{
        zip = $r.forecast.zip
        days = $r.forecast.days
        request_id = $id
        website_rows = $rows
        grafana_embeds = $embeds
        matching_prometheus_samples = $metrics
    }
}
$invalid = Invoke-WebRequest -Method Post -ContentType 'application/json' -TimeoutSec 5 -Body '{"zip":"12","days":0}' -SkipHttpErrorCheck "$base/forecasts"
if ($invalid.StatusCode -ne 400) { throw 'Invalid request was not rejected' }
$latestSubmission = ($results | ForEach-Object { [DateTimeOffset]$_.result.created_at } | Sort-Object | Select-Object -Last 1)
$earliestCompletion = ($results | ForEach-Object { [DateTimeOffset]$_.result.updated_at } | Sort-Object | Select-Object -First 1)
$overlap = $latestSubmission -lt $earliestCompletion
if (!$overlap) { throw 'The four live requests did not overlap' }
[ordered]@{
    tested_at_utc = [DateTimeOffset]::UtcNow.ToString('o')
    git_commit = (git rev-parse HEAD)
    all_four_requests_overlapped = $overlap
    invalid_input = @{ body = @{ zip = '12'; days = 0 }; status = [int]$invalid.StatusCode; response = ($invalid.Content | ConvertFrom-Json) }
    requests = @($results | Sort-Object { $_.input.days } | Select-Object input,sent_at_utc,observed_elapsed_ms,server_elapsed_ms,result)
    checks = @($checks | Sort-Object { $_.days })
} | ConvertTo-Json -Depth 18 | Set-Content -Encoding utf8 (Join-Path $PSScriptRoot 'forecast-results.json')
$results | Sort-Object { $_.input.days } | ForEach-Object {
    '{0} | {1} day(s) | {2} points | {3} ms server | {4} ms observed' -f $_.input.zip,$_.input.days,$_.result.forecast.points.Count,$_.server_elapsed_ms,$_.observed_elapsed_ms
}
'All four forecast requests overlapped. Invalid input returned HTTP 400.'
