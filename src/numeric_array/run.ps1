#Requires -Version 5.1
<#
  Runs the correctness suite, then the 10-second-per-scenario benchmark.
  Any extra arguments go to the correctness run (e.g. --only=computed).

  Usage:
    .\Run All.ps1
    .\Run All.ps1 --only=computed
    .\Run All.ps1 -BenchMs 30000
#>
[CmdletBinding()]
param(
    [int] $BenchMs = 0,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]] $Rest
)

$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot

function Invoke-Tsx {
    param([string[]] $Args)
    & npx --yes tsx @Args
    return $LASTEXITCODE
}

Write-Host '== PART 1: correctness ==' -ForegroundColor Cyan
$tsxArgs = @('Correctness Test.ts')
if ($Rest) { $tsxArgs += $Rest }
$correctness = Invoke-Tsx $tsxArgs

Write-Host ''
Write-Host '== PART 2: throughput ==' -ForegroundColor Cyan
$benchArgs = @('Benchmark.ts')
if ($BenchMs -gt 0) { $benchArgs += "--ms=$BenchMs" }
$bench = Invoke-Tsx $benchArgs

Write-Host ''
Write-Host ("correctness exit: {0}   benchmark exit: {1}" -f $correctness, $bench)
if ($correctness -ne 0) { exit $correctness }
exit $bench
