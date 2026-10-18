# Instala o binário do Pinax no Windows x64.
#
# Uso:
#   irm https://raw.githubusercontent.com/nicolasmmb/pinax/main/scripts/install.ps1 | iex
#   .\install.ps1 -Version v0.1.0 -InstallDir C:\Tools\pinax
#
# Variáveis de ambiente:
#   PINAX_VERSION       versão a instalar (padrão: latest)
#   PINAX_INSTALL_DIR   diretório de instalação (padrão: %LOCALAPPDATA%\Programs\pinax)

[CmdletBinding()]
param(
    [string]$Version = $env:PINAX_VERSION,
    [string]$InstallDir = $env:PINAX_INSTALL_DIR
)

$ErrorActionPreference = "Stop"
$repo = "nicolasmmb/pinax"

if (-not $InstallDir) {
    $InstallDir = Join-Path $env:LOCALAPPDATA "Programs\pinax"
}

if (-not [Environment]::Is64BitOperatingSystem) {
    throw "O Pinax requer Windows x64."
}

$arch = if ($env:PROCESSOR_ARCHITEW6432) { $env:PROCESSOR_ARCHITEW6432 } else { $env:PROCESSOR_ARCHITECTURE }
if ($arch -ne "AMD64") {
    throw "Arquitetura $arch não suportada (apenas Windows x64)."
}

if (-not $Version) {
    $Version = "latest"
}

$asset = "pinax-windows-x64.exe"
$Version = $Version.TrimStart("v")

if ($Version -eq "latest") {
    $url = "https://github.com/$repo/releases/latest/download/$asset"
}
else {
    $url = "https://github.com/$repo/releases/download/v$Version/$asset"
}

Write-Host "Baixando $url"

New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
$target = Join-Path $InstallDir "pinax.exe"
$temp = [System.IO.Path]::GetTempFileName()

try {
    $request = @{ Uri = $url; OutFile = $temp }
    if ($PSVersionTable.PSVersion.Major -lt 6) {
        $request.UseBasicParsing = $true
    }
    Invoke-WebRequest @request

    if ((Get-Item $temp).Length -eq 0) {
        throw "o download está vazio: $url"
    }

    Copy-Item -Force -Path $temp -Destination $target
}
finally {
    Remove-Item -Force -Path $temp -ErrorAction SilentlyContinue
}

# Adiciona o diretório ao PATH do usuário, sem duplicar entradas.
$userPath = [Environment]::GetEnvironmentVariable("Path", "User")
$entries = @()
if ($userPath) {
    $entries = $userPath -split ";" | Where-Object { $_ }
}

if ($entries -notcontains $InstallDir) {
    $newPath = (@($entries) + $InstallDir) -join ";"
    [Environment]::SetEnvironmentVariable("Path", $newPath, "User")
    Write-Host "Adicionado ao PATH do usuário (abra um novo terminal para usar)."
}

if (($env:Path -split ";") -notcontains $InstallDir) {
    $env:Path = "$InstallDir;$env:Path"
}

Write-Host "Pinax instalado em $target"
Write-Host ""
Write-Host "Próximos passos:"
Write-Host "  1. Configure PINAX_DOCUMENT_ROOT e PINAX_DATABASE_PATH (veja o README)."
Write-Host "  2. Inicie o worker: pinax worker"
Write-Host "  3. Configure seu cliente MCP para executar: pinax serve"
