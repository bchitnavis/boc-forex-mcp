# Common per user install location for Python console scripts
$roamingScripts = "$env:APPDATA\Python\Python314\Scripts"
Test-Path $roamingScripts
Get-ChildItem $roamingScripts -Filter "uv*" -ErrorAction SilentlyContinue

# Also check a general Roaming Python Scripts path (sometimes versioned differently)
Get-ChildItem "$env:APPDATA\Python" -Recurse -Filter "uv*.exe" -ErrorAction SilentlyContinue | Select-Object -First 20 FullName

# Check user local bin style folders
Get-ChildItem "$env:LOCALAPPDATA" -Recurse -Filter "uv*.exe" -ErrorAction SilentlyContinue | Select-Object -First 20 FullName