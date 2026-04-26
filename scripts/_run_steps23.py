import subprocess, sys
root = r'C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine'
python = sys.executable

steps = [
    (r'C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\engine\backtest\run_backtest.py',           r'C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\outputs\pipeline_2_backtest.log'),
    (r'C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\engine\backtest\run_execution_analysis.py', r'C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\outputs\pipeline_3_execution.log'),
]
for script, log in steps:
    print(f'Starting {script}', flush=True)
    with open(log, 'w', encoding='utf-8', errors='replace') as f:
        r = subprocess.run([python, script], cwd=root, stdout=f, stderr=subprocess.STDOUT, text=True)
    print(f'Done rc={r.returncode}', flush=True)
    if r.returncode != 0:
        print('FAILED - stopping', flush=True)
        break
print('ALL DONE', flush=True)
