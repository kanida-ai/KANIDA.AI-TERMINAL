$files = @(
  'C:\Users\SPS\Desktop\KANIDA.AI_TERMINAL\scheduler\KANIDA_NSE_Nightly.xml',
  'C:\Users\SPS\Desktop\KANIDA.AI_TERMINAL\scheduler\KANIDA_US_Nightly.xml'
)
foreach ($f in $files) {
  $content = Get-Content $f -Raw
  [System.IO.File]::WriteAllText($f, $content, [System.Text.Encoding]::Unicode)
  Write-Host "Converted $f"
}
