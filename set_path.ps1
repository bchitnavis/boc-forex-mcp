$uvDir = "C:\Users\pchitnbh\AppData\Roaming\Python\Python314\Scripts"

$currentUserPath = [Environment]::GetEnvironmentVariable("PATH","User")
if ([string]::IsNullOrWhiteSpace($currentUserPath)) { $currentUserPath = "" }

$parts = $currentUserPath -split ';' | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
    ForEach-Object { $_.Trim().TrimEnd('\') }

$normalizedUvDir = $uvDir.Trim().TrimEnd('\')

if ($parts -notcontains $normalizedUvDir) {
    $newUserPath = if ($currentUserPath) { "$currentUserPath;$uvDir" } else { $uvDir }
    [Environment]::SetEnvironmentVariable("PATH", $newUserPath, "User")
    Write-Host "Added to User PATH: $uvDir" -ForegroundColor Green
} else {
    Write-Host "Already present in User PATH: $uvDir" -ForegroundColor Yellow
}

Write-Host "Close and reopen PowerShell to load the updated PATH." -ForegroundColor Cyan