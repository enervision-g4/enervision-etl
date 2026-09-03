"""Resume lisible d'un flux de messages produit par le collecteur.

Le collecteur ecrit une ligne JSON par message sur la sortie standard. Ce script relit
ce flux et en tire un bilan : volumes par topic, qualite des mesures, pannes de capteurs
rencontrees, methodes d'imputation appliquees. Il verifie aussi les invariants du contrat.

Usage:
    uv run enervision-etl backfill --site SITE002 --hours 6 > messages.jsonl
    uv run python scripts/inspecter_flux.py messages.jsonl
"""

import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any


def titre(texte: str) -> None:
    print(f"\n{'=' * 74}\n{texte}\n{'=' * 74}")


def charger(chemin: str) -> list[dict[str, Any]]:
    """Relit un flux de messages, en ignorant les lignes qui n'en sont pas.

    Args:
        chemin: Fichier a relire, ou "-" pour l'entree standard.

    Returns:
        Les messages, chacun sous la forme topic, cle, valeur.
    """
    if chemin == "-":
        brut = sys.stdin.read()
    else:
        # PowerShell 5.1 redirige en UTF-16 la ou tout le reste ecrit en UTF-8.
        octets = Path(chemin).read_bytes()
        for encodage in ("utf-8", "utf-8-sig", "utf-16"):
            try:
                brut = octets.decode(encodage)
                break
            except UnicodeDecodeError:
                continue
        else:
            raise SystemExit(f"Encodage de {chemin} non reconnu")
    messages: list[dict[str, Any]] = []
    for ligne in brut.splitlines():
        ligne = ligne.strip()
        if not ligne.startswith("{"):
            continue
        try:
            decode = json.loads(ligne)
        except json.JSONDecodeError:
            continue
        if isinstance(decode, dict) and "topic" in decode and "value" in decode:
            messages.append(decode)
    return messages


chemin = sys.argv[1] if len(sys.argv) > 1 else "-"
messages = charger(chemin)

if not messages:
    print("Aucun message trouve. Le flux a-t-il bien ete redirige dans ce fichier ?")
    sys.exit(1)

print(f"{len(messages)} messages relus depuis {chemin}")

titre("1. Volumes par topic")
for topic, nombre in sorted(Counter(m["topic"] for m in messages).items()):
    print(f"  {topic:<34} {nombre:>6}")

brutes = [m for m in messages if m["value"]["event_type"] == "measure_raw"]
imputees = [m for m in messages if m["value"]["event_type"] == "measure_imputed"]
sites = [m for m in messages if m["value"]["event_type"] == "site"]

if sites:
    titre("2. Referentiel publie")
    for message in sites:
        fiche = message["value"]["payload"]
        print(f"  {fiche['site_id']}  {fiche['site_type']:<12} "
              f"{fiche['capacity_kw']:>7.0f} kW  {fiche['site_name']}")

if brutes:
    titre("3. Qualite des mesures brutes")
    qualites = Counter(m["value"]["payload"]["data_quality"] for m in brutes)
    for niveau, nombre in sorted(qualites.items(), key=lambda paire: -paire[1]):
        part = 100 * nombre / len(brutes)
        print(f"  {niveau:<12} {nombre:>6}  ({part:5.1f} %)")

    causes = Counter(
        cause for m in brutes for cause in m["value"]["payload"]["null_reasons"]
    )
    print(f"\n  Pannes de capteurs rencontrees : {dict(causes) if causes else 'aucune'}")

    manquantes = sum(1 for m in brutes if m["value"]["payload"]["consumption_kw"] is None)
    print(f"  Mesures sans consommation      : {manquantes}/{len(brutes)}")

if imputees:
    titre("4. Reconstruction des trous")
    methodes = Counter(m["value"]["payload"]["imputation_method"] for m in imputees)
    for methode, nombre in sorted(methodes.items(), key=lambda paire: -paire[1]):
        print(f"  {methode:<24} {nombre:>6}")

    comblees = [
        m for m in imputees if m["value"]["payload"]["imputation_method"] != "none"
    ]
    if comblees and brutes:
        titre("5. Exemple de trou comble")
        par_horodatage = {
            (m["value"]["payload"]["site_id"], m["value"]["payload"]["timestamp"]): m
            for m in brutes
        }
        montres = 0
        for message in comblees:
            fiche = message["value"]["payload"]
            jumelle = par_horodatage.get((fiche["site_id"], fiche["timestamp"]))
            if jumelle is None:
                continue
            print(f"  {fiche['site_id']}  {fiche['timestamp']}")
            print(f"    brut    consumption_kw = {jumelle['value']['payload']['consumption_kw']}")
            print(f"    impute  consumption_kw = {fiche['consumption_kw']}"
                  f"   ({fiche['imputation_method']})")
            montres += 1
            if montres == 3:
                break

titre("6. Controle des invariants du contrat")
anomalies: list[str] = []

for message in messages:
    fiche = message["value"]["payload"]
    if message["key"] != fiche["site_id"]:
        anomalies.append(f"cle {message['key']} differente du site {fiche['site_id']}")
    if message["value"]["schema_version"] != "1.0.0":
        anomalies.append(f"version de schema inattendue : {message['value']['schema_version']}")
    horodatage = fiche.get("timestamp")
    if horodatage is not None and not (horodatage.endswith("Z") or "+00:00" in horodatage):
        anomalies.append(f"horodatage sans fuseau UTC : {horodatage}")

controles = [
    ("cle de partition egale au site", "cle" ),
    ("version de schema constante", "version"),
    ("horodatages en UTC", "horodatage"),
]
for libelle, motif in controles:
    fautes = [a for a in anomalies if a.startswith(motif)]
    print(f"  {'ok ' if not fautes else 'NON'} {libelle}"
          + (f"  ({len(fautes)} ecarts)" if fautes else ""))

if brutes and imputees:
    memes_horodatages = {
        (m["value"]["payload"]["site_id"], m["value"]["payload"]["timestamp"]) for m in brutes
    } == {
        (m["value"]["payload"]["site_id"], m["value"]["payload"]["timestamp"]) for m in imputees
    }
    print(f"  {'ok ' if memes_horodatages else 'NON'} "
          "une mesure imputee par mesure brute, memes horodatages")

if anomalies:
    print(f"\n  {len(anomalies)} anomalie(s) :")
    for anomalie in anomalies[:5]:
        print(f"    - {anomalie}")
    sys.exit(1)
