# PowerShell script to create empty .md files corresponding to .sql files
# Usage: Run this script in the folder containing the .sql files

# Get all .sql files in the current directory
$sqlFiles = Get-ChildItem -Path "." -Filter "*.sql" -File

# Check if any .sql files were found
if ($sqlFiles.Count -eq 0) {
    Write-Host "No .sql files found in the current directory." -ForegroundColor Yellow
    exit
}

Write-Host "Found $($sqlFiles.Count) .sql file(s). Creating corresponding .md files..." -ForegroundColor Green

# Loop through each .sql file and create a corresponding empty .md file
foreach ($sqlFile in $sqlFiles) {
    # Get the base name without extension (e.g., "1" from "1.sql")
    $baseName = [System.IO.Path]::GetFileNameWithoutExtension($sqlFile.Name)
    
    # Create the .md filename with full path
    $mdFileName = Join-Path -Path $folderPath -ChildPath "$baseName.md"
    
    # Create empty .md file (or overwrite if it exists)
    New-Item -Path $mdFileName -ItemType File -Force | Out-Null
    
    Write-Host "Created: $mdFileName" -ForegroundColor Cyan
}

Write-Host "`nCompleted! Created $($sqlFiles.Count) .md file(s)." -ForegroundColor Green
