.\dependency-check.bat --format ALL --out "C:\GIT\DOTNET\SFTP.Lib\dependency-check-report" --enableExperimental --enableRetired --scan "C:\GIT\DOTNET\SFTP.Lib\bin" --scan "C:\GIT\DOTNET\SFTP.Lib\packages" --failOnCVSS 7 --suppression suppression.xml --prettyPrint --log dependency-check.log



# OWASP Dependency Check Analysis Script for .NET 4.8 SSH.NET Project
param(
    [string]$ProjectPath = ".",
    [string]$DependencyCheckPath = "C:\Tools\dependency-check\dependency-check\bin\dependency-check.bat",
    [string]$OutputDir = ".\dependency-check-report",
    [string]$ProjectName = "SshNetDemo",
    [ValidateSet("LOW", "MEDIUM", "HIGH", "CRITICAL")]
    [string]$FailOnCVSS = "HIGH",
    [switch]$UpdateDatabase
)

Write-Host "OWASP Dependency Check Analysis for SSH.NET Project" -ForegroundColor Green
Write-Host "====================================================" -ForegroundColor Green

# Verify Dependency Check installation
if (!(Test-Path $DependencyCheckPath)) {
    Write-Host "OWASP Dependency Check not found at: $DependencyCheckPath" -ForegroundColor Red
    Write-Host "Please run Setup-OWASP-DependencyCheck.ps1 first" -ForegroundColor Yellow
    exit 1
}

# Create output directory
if (!(Test-Path $OutputDir)) {
    New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null
    Write-Host "Created output directory: $OutputDir" -ForegroundColor Yellow
}

# Build the project first
Write-Host "`nBuilding the project..." -ForegroundColor Cyan
try {
    # Restore NuGet packages
    if (Test-Path "packages.config") {
        Write-Host "Restoring NuGet packages..." -ForegroundColor Yellow
        nuget restore -PackagesDirectory packages
    }
    
    # Build the project
    msbuild /p:Configuration=Release /p:Platform="Any CPU" /verbosity:minimal
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Build successful!" -ForegroundColor Green
    } else {
        Write-Host "Build failed with exit code: $LASTEXITCODE" -ForegroundColor Red
        Write-Host "Continuing with analysis of existing binaries..." -ForegroundColor Yellow
    }
} catch {
    Write-Host "Build error: $_" -ForegroundColor Red
    Write-Host "Continuing with analysis..." -ForegroundColor Yellow
}

# Prepare scan directories
$scanPaths = @()
$binPath = Join-Path $ProjectPath "bin"
$packagesPath = Join-Path $ProjectPath "packages"

if (Test-Path $binPath) {
    $scanPaths += $binPath
    Write-Host "Found bin directory: $binPath" -ForegroundColor Cyan
}

if (Test-Path $packagesPath) {
    $scanPaths += $packagesPath
    Write-Host "Found packages directory: $packagesPath" -ForegroundColor Cyan
}

if ($scanPaths.Count -eq 0) {
    Write-Host "No scan directories found. Creating a minimal scan..." -ForegroundColor Yellow
    $scanPaths += $ProjectPath
}

# Build Dependency Check command
$dcCommand = @(
    "`"$DependencyCheckPath`""
    "--project `"$ProjectName`""
    "--format ALL"
    "--out `"$OutputDir`""
    "--enableExperimental"
    "--enableRetired"
)

# Add each scan path
foreach ($path in $scanPaths) {
    $dcCommand += "--scan `"$path`""
}

# Add database update if requested
if ($UpdateDatabase) {
    $dcCommand += "--updateonly"
    Write-Host "`nUpdating vulnerability database..." -ForegroundColor Cyan
    
    $updateCmd = @(
        "`"$DependencyCheckPath`""
        "--updateonly"
    )
    
    $updateCmdString = $updateCmd -join " "
    Write-Host "Running: $updateCmdString" -ForegroundColor Gray
    Invoke-Expression $updateCmdString
    
    Write-Host "Database update completed.`n" -ForegroundColor Green
}

# Set fail threshold
$cvssMap = @{
    "LOW" = "0"
    "MEDIUM" = "4"
    "HIGH" = "7"  
    "CRITICAL" = "9"
}

if ($cvssMap.ContainsKey($FailOnCVSS)) {
    $dcCommand += "--failOnCVSS $($cvssMap[$FailOnCVSS])"
}

# Add additional useful options
$dcCommand += @(
    "--suppression suppression.xml"
    "--prettyPrint"
    "--log dependency-check.log"
)

# Join command and execute
$fullCommand = $dcCommand -join " "
Write-Host "`nRunning OWASP Dependency Check..." -ForegroundColor Cyan
Write-Host "Command: $fullCommand" -ForegroundColor Gray
Write-Host "This may take several minutes on first run..." -ForegroundColor Yellow

try {
    # Execute the command
    $startTime = Get-Date
    Invoke-Expression $fullCommand
    $endTime = Get-Date
    $duration = $endTime - $startTime
    
    Write-Host "`nScan completed in $($duration.TotalMinutes.ToString('F1')) minutes" -ForegroundColor Green
    
    # Check if reports were generated
    $htmlReport = Join-Path $OutputDir "dependency-check-report.html"
    $jsonReport = Join-Path $OutputDir "dependency-check-report.json"
    $xmlReport = Join-Path $OutputDir "dependency-check-report.xml"
    
    Write-Host "`nGenerated Reports:" -ForegroundColor Green
    if (Test-Path $htmlReport) {
        Write-Host "  HTML Report: $htmlReport" -ForegroundColor Cyan
    }
    if (Test-Path $jsonReport) {
        Write-Host "  JSON Report: $jsonReport" -ForegroundColor Cyan
    }
    if (Test-Path $xmlReport) {
        Write-Host "  XML Report:  $xmlReport" -ForegroundColor Cyan
    }
    
    # Parse and display summary from JSON report
    if (Test-Path $jsonReport) {
        Write-Host "`nAnalyzing results..." -ForegroundColor Yellow
        try {
            $reportData = Get-Content $jsonReport | ConvertFrom-Json
            
            $totalDependencies = $reportData.dependencies.Count
            $vulnerableDependencies = ($reportData.dependencies | Where-Object { $_.vulnerabilities }).Count
            $totalVulnerabilities = ($reportData.dependencies | ForEach-Object { $_.vulnerabilities } | Measure-Object).Count
            
            Write-Host "`nSUMMARY:" -ForegroundColor Green
            Write-Host "========" -ForegroundColor Green
            Write-Host "Total Dependencies Scanned: $totalDependencies" -ForegroundColor White
            Write-Host "Dependencies with Vulnerabilities: $vulnerableDependencies" -ForegroundColor Yellow
            Write-Host "Total Vulnerabilities Found: $totalVulnerabilities" -ForegroundColor Red
            
            # Count vulnerabilities by severity
            $severityCounts = @{}
            $cvssScores = @()
            
            foreach ($dep in $reportData.dependencies) {
                if ($dep.vulnerabilities) {
                    foreach ($vuln in $dep.vulnerabilities) {
                        if ($vuln.severity) {
                            $severity = $vuln.severity.ToUpper()
                            if (!$severityCounts[$severity]) { $severityCounts[$severity] = 0 }
                            $severityCounts[$severity]++
                        }
                        
                        if ($vuln.cvssv3 -and $vuln.cvssv3.baseScore) {
                            $cvssScores += [double]$vuln.cvssv3.baseScore
                        } elseif ($vuln.cvssv2 -and $vuln.cvssv2.score) {
                            $cvssScores += [double]$vuln.cvssv2.score
                        }
                    }
                }
            }
            
            Write-Host "`nVulnerabilities by Severity:" -ForegroundColor Green
            foreach ($severity in @("CRITICAL", "HIGH", "MEDIUM", "LOW")) {
                if ($severityCounts[$severity]) {
                    $color = switch ($severity) {
                        "CRITICAL" { "Red" }
                        "HIGH" { "Red" }
                        "MEDIUM" { "Yellow" }
                        "LOW" { "White" }
                    }
                    Write-Host "  $severity`: $($severityCounts[$severity])" -ForegroundColor $color
                }
            }
            
            if ($cvssScores.Count -gt 0) {
                $avgCvss = ($cvssScores | Measure-Object -Average).Average
                $maxCvss = ($cvssScores | Measure-Object -Maximum).Maximum
                Write-Host "`nCVSS Scores:" -ForegroundColor Green
                Write-Host "  Highest CVSS Score: $($maxCvss.ToString('F1'))" -ForegroundColor Red
                Write-Host "  Average CVSS Score: $($avgCvss.ToString('F1'))" -ForegroundColor Yellow
            }
            
            # Show top vulnerabilities
            $highVulns = @()
            foreach ($dep in $reportData.dependencies) {
                if ($dep.vulnerabilities) {
                    foreach ($vuln in $dep.vulnerabilities) {
                        $score = 0
                        if ($vuln.cvssv3 -and $vuln.cvssv3.baseScore) {
                            $score = [double]$vuln.cvssv3.baseScore
                        } elseif ($vuln.cvssv2 -and $vuln.cvssv2.score) {
                            $score = [double]$vuln.cvssv2.score
                        }
                        
                        if ($score -ge 7.0) {
                            $highVulns += @{
                                Dependency = $dep.fileName
                                CVE = $vuln.name
                                Score = $score
                                Severity = $vuln.severity
                                Description = $vuln.description
                            }
                        }
                    }
                }
            }
            
            if ($highVulns.Count -gt 0) {
                Write-Host "`nHigh/Critical Vulnerabilities (CVSS >= 7.0):" -ForegroundColor Red
                $topVulns = $highVulns | Sort-Object Score -Descending | Select-Object -First 5
                foreach ($vuln in $topVulns) {
                    Write-Host "`n  CVE: $($vuln.CVE)" -ForegroundColor White
                    Write-Host "  File: $($vuln.Dependency)" -ForegroundColor Gray
                    Write-Host "  CVSS Score: $($vuln.Score)" -ForegroundColor Red
                    Write-Host "  Severity: $($vuln.Severity)" -ForegroundColor Yellow
                    if ($vuln.Description -and $vuln.Description.Length -gt 0) {
                        $desc = $vuln.Description.Substring(0, [Math]::Min(100, $vuln.Description.Length))
                        Write-Host "  Description: $desc..." -ForegroundColor Gray
                    }
                }
            }
            
        } catch {
            Write-Host "Could not parse JSON report: $_" -ForegroundColor Yellow
        }
    }
    
    # Open HTML report
    if (Test-Path $htmlReport) {
        Write-Host "`nOpening HTML report in default browser..." -ForegroundColor Cyan
        Start-Process $htmlReport
    }
    
} catch {
    Write-Host "Error running dependency check: $_" -ForegroundColor Red
    exit 1
}

Write-Host "`nDependency check analysis completed!" -ForegroundColor Green
