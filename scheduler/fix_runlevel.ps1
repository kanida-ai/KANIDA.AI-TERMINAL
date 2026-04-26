$files = @(
  'C:\Users\SPS\Desktop\KANIDA.AI_TERMINAL\scheduler\KANIDA_NSE_Nightly.xml',
  'C:\Users\SPS\Desktop\KANIDA.AI_TERMINAL\scheduler\KANIDA_US_Nightly.xml'
)
foreach ($f in $files) {
  $c = Get-Content $f -Raw
  $c = $c -replace 'HighestAvailable','LeastPrivilege'
  [System.IO.File]::WriteAllText($f, $c, [System.Text.Encoding]::Unicode)
  Write-Host "Patched $f"
}
