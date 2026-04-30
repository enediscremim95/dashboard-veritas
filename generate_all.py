"""
Roda todos os dashboards ativos em sequencia.
Uso: python generate_all.py
     python generate_all.py tratorval kooldent   (so esses)
"""
import subprocess, sys, os

ALL_SCRIPTS = [
    "generate_tratorval.py",
    "generate_qualy_usa.py",
    "generate_kooldent.py",
    "generate_dr_amanda.py",
    "generate_dfort.py",
    "generate_dentuga.py",
    "generate_kensington.py",
    "generate_porto_smile.py",
    "generate_all_clinique.py",
    "generate_bivar.py",
    "generate_lar_e_cia.py",
    "generate_mediconvert.py",
    "generate_felt.py",
    # gnauto: sem conta configurada ainda
    # cioalges: sem dados
    # perfectclinic: shell sem dados
]

base = os.path.dirname(os.path.abspath(__file__))

# Filtra por argumento (ex: python generate_all.py tratorval)
slugs = sys.argv[1:]
if slugs:
    SCRIPTS = [s for s in ALL_SCRIPTS if any(sl.replace('-','_') in s for sl in slugs)]
    if not SCRIPTS:
        print(f"Nenhum script encontrado para: {slugs}")
        sys.exit(1)
else:
    SCRIPTS = ALL_SCRIPTS

erros = []
for script in SCRIPTS:
    print(f"\n{'='*50}")
    print(f"[{script}]")
    print('='*50)
    r = subprocess.run([sys.executable, script], cwd=base)
    if r.returncode != 0:
        print(f"ERRO em {script} (rc={r.returncode})")
        erros.append(script)
    else:
        print(f"OK {script}")

print(f"\n{'='*50}")
if erros:
    print(f"ERROS ({len(erros)}): {', '.join(erros)}")
    sys.exit(1)
else:
    print(f"OK - {len(SCRIPTS)} dashboards gerados e deployados!")
