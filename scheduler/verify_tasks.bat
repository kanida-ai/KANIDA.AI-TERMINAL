@echo off
schtasks /Query /TN "KANIDA_NSE_Nightly" /FO LIST
echo ===
schtasks /Query /TN "KANIDA_US_Nightly" /FO LIST
