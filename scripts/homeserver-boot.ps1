# Home server boot script, run at startup by the "Hookline WSL" scheduled task. Starts the
# WSL distro (Docker starts via systemd and brings the k3d cluster back), points the 80/443
# port proxy at WSL's current IP, then keeps WSL alive. If WSL stops (wsl --shutdown, a WSL
# update), it loops: restart, refresh the proxy, hold.
#
# Register it (elevated PowerShell). S4U runs as your user without a stored password and
# without anyone logged in; WSL distros are per user, so a SYSTEM task cannot see them:
#   Copy-Item scripts\homeserver-boot.ps1 C:\ProgramData\Hookline\
#   $a = New-ScheduledTaskAction -Execute powershell.exe -Argument "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File C:\ProgramData\Hookline\homeserver-boot.ps1 -Distro Ubuntu"
#   $p = New-ScheduledTaskPrincipal -UserId (whoami) -LogonType S4U -RunLevel Highest
#   $s = New-ScheduledTaskSettingsSet -ExecutionTimeLimit ([TimeSpan]::Zero) -RestartCount 5 -RestartInterval (New-TimeSpan -Minutes 1) -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable
#   Register-ScheduledTask -TaskName "Hookline WSL" -Action $a -Trigger (New-ScheduledTaskTrigger -AtStartup) -Principal $p -Settings $s
param(
  [string]$Distro = 'Ubuntu',
  [string]$Log = 'C:\ProgramData\Hookline\boot.log'
)
$ErrorActionPreference = 'Continue'
New-Item -ItemType Directory -Force (Split-Path $Log) | Out-Null
function Log($m) { "$(Get-Date -Format s) $m" | Out-File -Append -Encoding utf8 $Log }

while ($true) {
  Log 'starting WSL'
  & wsl.exe -d $Distro -u root -- systemctl start docker
  Log "docker start exit=$LASTEXITCODE"

  $wslIp = $null
  for ($i = 0; $i -lt 30 -and -not $wslIp; $i++) {
    $out = (& wsl.exe -d $Distro -- hostname -I) -join ' '
    $wslIp = ($out.Trim() -split '\s+' | Where-Object { $_ -match '^\d+\.\d+\.\d+\.\d+$' } | Select-Object -First 1)
    if (-not $wslIp) { Start-Sleep 2 }
  }
  Log "wsl ip=$wslIp"

  if ($wslIp) {
    foreach ($p in 80, 443) {
      & netsh interface portproxy delete v4tov4 listenport=$p listenaddress=0.0.0.0 | Out-Null
      & netsh interface portproxy add v4tov4 listenport=$p listenaddress=0.0.0.0 connectport=$p connectaddress=$wslIp | Out-Null
    }
    Log 'portproxy updated'
  }

  # WSL stops an idle distro once no wsl.exe client is attached; hold one open.
  Log 'holding WSL open'
  & wsl.exe -d $Distro -u root -- sleep infinity
  Log "hold exited code=$LASTEXITCODE; restarting in 15 s"
  Start-Sleep 15
}
