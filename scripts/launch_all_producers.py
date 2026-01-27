#!/usr/bin/env python3
"""
=============================================================
 launch_all_producers.py – Lance les 3 producers en parallèle
=============================================================
 Démarre ecommerce + taxi + météo dans des processus séparés.
 Affiche les logs de chaque producer avec un préfixe coloré.

 Utilisation :
   python launch_all_producers.py
   python launch_all_producers.py --only ecommerce weather
=============================================================
"""

import argparse
import subprocess
import sys
import threading
import time
from pathlib import Path

PRODUCERS_DIR = Path(__file__).parent.parent / "src" / "producers"

PRODUCERS = {
    "ecommerce": {
        "script": "ecommerce_producer.py",
        "color":  "\033[92m",   # vert
        "label":  "🛒 ECOM ",
    },
    "taxi": {
        "script": "nyc_taxi_producer.py",
        "color":  "\033[93m",   # jaune
        "label":  "🚕 TAXI ",
        "args":   ["--mode", "simulate"],
    },
    "weather": {
        "script": "weather_producer.py",
        "color":  "\033[94m",   # bleu
        "label":  "🌤️ METEO",
    },
}

RESET = "\033[0m"

def stream_output(proc, color: str, label: str):
    """Lit stdout d'un subprocess et l'affiche avec un préfixe coloré."""
    for line in iter(proc.stdout.readline, b""):
        text = line.decode("utf-8", errors="replace").rstrip()
        if text:
            print(f"  {color}[{label}]{RESET} {text}")

def main():
    parser = argparse.ArgumentParser(description="Lance tous les producers Kafka")
    parser.add_argument(
        "--only",
        nargs="+",
        choices=list(PRODUCERS.keys()),
        help="Lancer uniquement certains producers"
    )
    args = parser.parse_args()

    to_launch = args.only if args.only else list(PRODUCERS.keys())

    print(f"\n{'═'*65}")
    print(f"  🚀  Lancement des Producers Kafka")
    print(f"  Producers : {', '.join(to_launch)}")
    print(f"  Ctrl+C pour tout arrêter")
    print(f"{'═'*65}\n")

    processes = []
    threads   = []

    for name in to_launch:
        p_cfg   = PRODUCERS[name]
        script  = str(PRODUCERS_DIR / p_cfg["script"])
        cmd     = [sys.executable, script] + p_cfg.get("args", [])

        print(f"  {p_cfg['color']}▶ {name}{RESET}  →  {script}")
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=str(PRODUCERS_DIR),
            bufsize=1,
        )
        processes.append(proc)

        t = threading.Thread(
            target=stream_output,
            args=(proc, p_cfg["color"], p_cfg["label"]),
            daemon=True,
        )
        t.start()
        threads.append(t)
        time.sleep(1)  # décaler les démarrages

    print(f"\n  ✅  {len(processes)} producers actifs — logs en cours…\n")

    try:
        while True:
            # Vérifier si des processus se sont terminés
            for i, proc in enumerate(processes):
                if proc.poll() is not None:
                    name = to_launch[i]
                    print(f"\n  ⚠️  Producer '{name}' s'est arrêté (code: {proc.returncode})")
            time.sleep(5)

    except KeyboardInterrupt:
        print(f"\n\n  🛑  Arrêt en cours…")
        for proc in processes:
            proc.terminate()
        for proc in processes:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        print(f"  ✅  Tous les producers arrêtés.")

if __name__ == "__main__":
    main()
