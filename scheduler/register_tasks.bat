@echo off
schtasks /Create /TN "KANIDA_NSE_Nightly" /XML "C:\Users\SPS\Desktop\KANIDA.AI_TERMINAL\scheduler\KANIDA_NSE_Nightly.xml" /F
echo ---
schtasks /Create /TN "KANIDA_US_Nightly" /XML "C:\Users\SPS\Desktop\KANIDA.AI_TERMINAL\scheduler\KANIDA_US_Nightly.xml" /F
