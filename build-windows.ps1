<#
.SYNOPSIS
Builds Spotifast locally on Windows.

.DESCRIPTION
The default release build includes MilkDrop and validates its native build
prerequisites before running Cargo. Use -WithoutMilkDrop for a single-binary
build that only requires Rust and the Visual Studio C++ toolchain.

.EXAMPLE
.\build-windows.ps1

.EXAMPLE
.\build-windows.ps1 -VcpkgRoot C:\src\vcpkg

.EXAMPLE
.\build-windows.ps1 -WithoutMilkDrop

.EXAMPLE
.\build-windows.ps1 -Configuration Debug -WithoutMilkDrop
#>
[CmdletBinding()]
param(
    [ValidateSet('Debug', 'Release')]
    [string]$Configuration = 'Release',

    [Alias('NoMilkDrop')]
    [switch]$WithoutMilkDrop,

    [string]$VcpkgRoot = $env:VCPKG_INSTALLATION_ROOT
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

if ($env:OS -ne 'Windows_NT') {
    throw 'This script builds the Windows application and must run on Windows.'
}

$repoRoot = $PSScriptRoot

function Find-RequiredCommand {
    param(
        [Parameter(Mandatory)]
        [string]$Name,

        [Parameter(Mandatory)]
        [string]$InstallHint
    )

    $command = Get-Command $Name -ErrorAction SilentlyContinue
    if ($null -eq $command) {
        throw "Required command '$Name' was not found. $InstallHint"
    }
    return $command.Source
}

$cargo = Find-RequiredCommand -Name 'cargo' -InstallHint 'Install Rust from https://rustup.rs/.'

if (-not $WithoutMilkDrop) {
    $architecture = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture
    if ($architecture -eq [System.Runtime.InteropServices.Architecture]::Arm64) {
        throw 'MilkDrop is not supported by the Windows ARM64 build. Run again with -WithoutMilkDrop.'
    }

    $null = Find-RequiredCommand -Name 'cmake' -InstallHint 'Install CMake and add it to PATH.'
    $clang = Get-Command 'clang' -ErrorAction SilentlyContinue
    $clangCl = Get-Command 'clang-cl' -ErrorAction SilentlyContinue
    if ($null -eq $clang -and $null -eq $clangCl) {
        throw "LLVM was not found. Install LLVM, including libclang, and add its bin directory to PATH."
    }

    if ([string]::IsNullOrWhiteSpace($VcpkgRoot)) {
        throw @'
MilkDrop requires vcpkg. Pass -VcpkgRoot C:\path\to\vcpkg or set
VCPKG_INSTALLATION_ROOT, then install glew:x64-windows-static.
'@
    }

    $resolvedVcpkgRoot = (Resolve-Path -LiteralPath $VcpkgRoot).Path
    $vcpkgToolchain = Join-Path $resolvedVcpkgRoot 'scripts\buildsystems\vcpkg.cmake'
    if (-not (Test-Path -LiteralPath $vcpkgToolchain -PathType Leaf)) {
        throw "The vcpkg toolchain was not found at '$vcpkgToolchain'."
    }

    $glewPackage = Join-Path $resolvedVcpkgRoot 'installed\x64-windows-static\share\glew'
    if (-not (Test-Path -LiteralPath $glewPackage -PathType Container)) {
        throw "GLEW is not installed for the static Windows triplet. Run '$resolvedVcpkgRoot\vcpkg.exe install glew:x64-windows-static'."
    }

    # projectm-sys reads this exact variable when configuring CMake.
    $env:VCPKG_INSTALLATION_ROOT = $resolvedVcpkgRoot
}

$cargoArguments = @('build', '--locked')
if ($Configuration -eq 'Release') {
    $cargoArguments += '--release'
}
if ($WithoutMilkDrop) {
    $cargoArguments += '--no-default-features'
}

Write-Host "Running: cargo $($cargoArguments -join ' ')" -ForegroundColor Cyan
Push-Location $repoRoot
try {
    & $cargo @cargoArguments
    if ($LASTEXITCODE -ne 0) {
        throw "Cargo exited with code $LASTEXITCODE."
    }
}
finally {
    Pop-Location
}

$profileDirectory = $Configuration.ToLowerInvariant()
$executable = Join-Path $repoRoot "target\$profileDirectory\spotifast.exe"
if (-not (Test-Path -LiteralPath $executable -PathType Leaf)) {
    throw "Cargo succeeded, but '$executable' was not found."
}

Write-Host ''
Write-Host 'Spotifast was built successfully:' -ForegroundColor Green
Write-Host $executable
