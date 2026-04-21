"""
Roda todos os dashboards ativos em sequência.
Usado no Railway (cron diário) e localmente via: python generate_all.py
"""
import subprocess, sys, os

SCRIPTS = [
    "generate_tratorval.py",
    "generate_qualy_usa.py",
]

base = os.path.dirname(os.path.abspath(__file__))
erros = []

for script in SCRIPTS:
    print(f"\n{'='*50}")
    print(f"▶ {script}")
    print('='*50)
    r = subprocess.run([sys.executable, script], cwd=base)
    if r.returncode != 0:
        print(f"❌ ERRO em {script} (rc={r.returncode})")
        erros.append(script)
    else:
        print(f"✅ {script} concluído")

print(f"\n{'='*50}")
if erros:
    print(f"⚠️  {len(erros)} erro(s): {', '.join(erros)}")
    sys.exit(1)
else:
    print(f"✅ Todos os {len(SCRIPTS)} dashboards gerados e deployados!")
