"""Resume lisible d'un flux de messages produit par le collecteur.

Le collecteur ecrit une ligne JSON par message sur la sortie standard. Ce script relit
ce flux et en tire un bilan : volumes par topic, qualite des mesures, pannes de capteurs
rencontrees, methodes d'imputation appliquees. Il verifie aussi les invariants du contrat.

Usage:
    uv run enervision-etl backfill --site SITE002 --hours 6 > messages.jsonl
    uv run python scripts/inspect_message_stream.py messages.jsonl
"""

import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any


def section_title(texte: str) -> None:
    print(f"\n{'=' * 74}\n{texte}\n{'=' * 74}")


def load_messages(path: str) -> list[dict[str, Any]]:
    """Relit un flux de messages, en ignorant les lignes qui n'en sont pas.

    Args:
        path: Fichier a relire, ou "-" pour l'entree standard.

    Returns:
        Les messages, chacun sous la forme topic, cle, valeur.
    """
    if path == "-":
        raw_text = sys.stdin.read()
    else:
        # PowerShell 5.1 redirige en UTF-16 la ou tout le reste ecrit en UTF-8.
        raw_bytes = Path(path).read_bytes()
        for encoding in ("utf-8", "utf-8-sig", "utf-16"):
            try:
                raw_text = raw_bytes.decode(encoding)
                break
            except UnicodeDecodeError:
                continue
        else:
            raise SystemExit(f"Encodage de {path} non reconnu")
    messages: list[dict[str, Any]] = []
    for line in raw_text.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            decoded = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(decoded, dict) and "topic" in decoded and "value" in decoded:
            messages.append(decoded)
    return messages


path = sys.argv[1] if len(sys.argv) > 1 else "-"
messages = load_messages(path)

if not messages:
    print("Aucun message dans ce flux. Deux causes possibles :")
    print("  1. Le collecteur a refuse de publier la fenetre demandee. Cherchez la ligne")
    print("     degenerate_window_refused dans les journaux restes a l'ecran : le site")
    print("     etait en panne et la periode entiere est vide.")
    print("  2. La redirection n'a pas fonctionne et le flux n'est jamais arrive ici.")
    sys.exit(1)

print(f"{len(messages)} messages relus depuis {path}")

section_title("1. Volumes par topic")
for topic, count in sorted(Counter(m["topic"] for m in messages).items()):
    print(f"  {topic:<34} {count:>6}")

raw_readings = [m for m in messages if m["value"]["event_type"] == "measure_raw"]
imputed_readings = [m for m in messages if m["value"]["event_type"] == "measure_imputed"]
sites = [m for m in messages if m["value"]["event_type"] == "site"]

if sites:
    section_title("2. Referentiel publie")
    for message in sites:
        fiche = message["value"]["payload"]
        print(f"  {fiche['site_id']}  {fiche['site_type']:<12} "
              f"{fiche['capacity_kw']:>7.0f} kW  {fiche['site_name']}")

if raw_readings:
    section_title("3. Qualite des mesures brutes")
    qualities = Counter(m["value"]["payload"]["data_quality"] for m in raw_readings)
    for niveau, count in sorted(qualities.items(), key=lambda pair: -pair[1]):
        part = 100 * count / len(raw_readings)
        print(f"  {niveau:<12} {count:>6}  ({part:5.1f} %)")

    causes = Counter(
        cause for m in raw_readings for cause in m["value"]["payload"]["null_reasons"]
    )
    print(f"\n  Pannes de capteurs rencontrees : {dict(causes) if causes else 'aucune'}")

    manquantes = sum(1 for m in raw_readings if m["value"]["payload"]["consumption_kw"] is None)
    print(f"  Mesures sans consommation      : {manquantes}/{len(raw_readings)}")

if imputed_readings:
    section_title("4. Reconstruction des trous")
    methods = Counter(m["value"]["payload"]["imputation_method"] for m in imputed_readings)
    for methode, count in sorted(methods.items(), key=lambda pair: -pair[1]):
        print(f"  {methode:<24} {count:>6}")

    filled_readings = [
        m for m in imputed_readings if m["value"]["payload"]["imputation_method"] != "none"
    ]
    if filled_readings and raw_readings:
        section_title("5. Exemple de trou comble")
        by_timestamp = {
            (m["value"]["payload"]["site_id"], m["value"]["payload"]["timestamp"]): m
            for m in raw_readings
        }
        shown = 0
        for message in filled_readings:
            fiche = message["value"]["payload"]
            twin = by_timestamp.get((fiche["site_id"], fiche["timestamp"]))
            if twin is None:
                continue
            print(f"  {fiche['site_id']}  {fiche['timestamp']}")
            print(f"    brut    consumption_kw = {twin['value']['payload']['consumption_kw']}")
            print(f"    impute  consumption_kw = {fiche['consumption_kw']}"
                  f"   ({fiche['imputation_method']})")
            shown += 1
            if shown == 3:
                break

section_title("6. Controle des invariants du contrat")
anomalies: list[str] = []

for message in messages:
    fiche = message["value"]["payload"]
    if message["key"] != fiche["site_id"]:
        anomalies.append(f"cle {message['key']} differente du site {fiche['site_id']}")
    if message["value"]["schema_version"] != "1.0.0":
        anomalies.append(f"version de schema inattendue : {message['value']['schema_version']}")
    timestamp = fiche.get("timestamp")
    if timestamp is not None and not (timestamp.endswith("Z") or "+00:00" in timestamp):
        anomalies.append(f"horodatage sans fuseau UTC : {timestamp}")

checks = [
    ("cle de partition egale au site", "cle" ),
    ("version de schema constante", "version"),
    ("horodatages en UTC", "horodatage"),
]
for label, prefix in checks:
    offenders = [a for a in anomalies if a.startswith(prefix)]
    print(f"  {'ok ' if not offenders else 'NON'} {label}"
          + (f"  ({len(offenders)} ecarts)" if offenders else ""))

if raw_readings and imputed_readings:
    matching_timestamps = {
        (m["value"]["payload"]["site_id"], m["value"]["payload"]["timestamp"]) for m in raw_readings
    } == {
        (m["value"]["payload"]["site_id"], m["value"]["payload"]["timestamp"])
        for m in imputed_readings
    }
    print(f"  {'ok ' if matching_timestamps else 'NON'} "
          "une mesure imputee par mesure brute, memes horodatages")

if anomalies:
    print(f"\n  {len(anomalies)} anomalie(s) :")
    for anomaly in anomalies[:5]:
        print(f"    - {anomaly}")
    sys.exit(1)
