# Prefer an installed Python, falling back to the bundled desktop runtime.
$marinePython = Get-Command python -ErrorAction SilentlyContinue
if (Test-Path -LiteralPath (Join-Path $PSScriptRoot '.venv\Scripts\python.exe')) {
    $marinePythonPath = Join-Path $PSScriptRoot '.venv\Scripts\python.exe'
} elseif ($marinePython -and $marinePython.Source -notlike '*WindowsApps*') {
    $marinePythonPath = $marinePython.Source
} else {
    $marinePythonPath = Join-Path $env:USERPROFILE '.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe'
}
if (-not (Test-Path -LiteralPath $marinePythonPath)) {
    throw 'Install Python 3.11 or newer, then run python -m marine_tools.'
}
Push-Location $PSScriptRoot
try {
    & $marinePythonPath -m marine_tools @args
    $marineExitCode = $LASTEXITCODE
} finally {
    Pop-Location
}
exit $marineExitCode
