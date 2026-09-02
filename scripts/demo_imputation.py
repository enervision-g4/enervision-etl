"""Eprouve l'imputation sur les donnees reelles de l'API mock.

Deux volets. Le premier montre ce que l'imputation fait des trous reellement
presents dans une serie. Le second est un controle de justesse : on masque des
valeurs dont on connait la vraie mesure, on impute, et on compare. C'est la seule
facon de chiffrer l'erreur commise par une strategie de reconstruction.

Usage:
    uv run python scripts/demo_imputation.py [SITE002] [heures] [resolution_s]
"""

import sys
from datetime import UTC, datetime, timedelta
from typing import Optional

from enervision_etl.config import load_settings
from enervision_etl.extract.http_client import ResilientHttpClient
from enervision_etl.extract.mock_api_client import MockApiClient
from enervision_etl.transform.imputation import forward_fill_series
from enervision_etl.transform.normalization import normalize_reading

MAX_GAP_MEASURES = 3
POSITIONS_A_MASQUER = (4, 9, 10, 15, 16, 17)


def titre(texte: str) -> None:
    print(f"\n{'=' * 82}\n{texte}\n{'=' * 82}")


def affiche(valeur: Optional[float], largeur: int = 9) -> str:
    return f"{valeur:>{largeur}.2f}" if valeur is not None else f"{'NULL':>{largeur}}"


site_id = sys.argv[1] if len(sys.argv) > 1 else "SITE002"
heures = float(sys.argv[2]) if len(sys.argv) > 2 else 2.0
resolution = float(sys.argv[3]) if len(sys.argv) > 3 else 300.0

settings = load_settings()
fin = datetime.now(UTC).replace(tzinfo=None)
debut = fin - timedelta(hours=heures)

with ResilientHttpClient(settings.api_mock_base_url, settings.api_mock_timeout_seconds) as http:
    brutes = MockApiClient(http).fetch_readings_window(site_id, debut, fin, resolution)

serie = [normalize_reading(mesure, settings.api_mock_source_timezone) for mesure in brutes]
print(f"Site {site_id} : {len(serie)} mesures sur {heures} h, une toutes les {resolution:.0f} s")

if not serie:
    print("Aucune mesure sur la periode. Rien a demontrer.")
    sys.exit(0)

titre("VOLET 1 : ce que l'imputation fait des trous reellement presents")
imputee = forward_fill_series(serie, MAX_GAP_MEASURES)
trous_reels = [i for i, m in enumerate(serie) if m.consumption_kw is None]

if not trous_reels:
    print("  La serie ne contient aucun trou : tous les capteurs ont repondu.")
    print("  C'est le cas nominal. Le volet 2 va donc en creer de facon controlee.")
else:
    print(f"  {len(trous_reels)} mesures a consumption_kw nul sur {len(serie)}")
    print(f"\n  {'heure':<7}{'BRUT':>10}{'IMPUTE':>11}  {'methode':<22}champs reconstruits")
    print("  " + "-" * 78)
    for mesure, remplie in zip(serie, imputee, strict=True):
        if mesure.missing_measurement_fields() or remplie.imputed_fields:
            print(f"  {mesure.timestamp:%H:%M}  {affiche(mesure.consumption_kw)}"
                  f"{affiche(remplie.consumption_kw, 11)}  {remplie.imputation_method:<22}"
                  f"{', '.join(remplie.imputed_fields) or '-'}")

titre("VOLET 2 : controle de justesse sur des valeurs dont on connait la verite")
saines = [m for m in serie if m.consumption_kw is not None]
if len(saines) < 20:
    print(f"  Seulement {len(saines)} mesures saines, trop peu pour le controle.")
    sys.exit(0)

positions = [p for p in POSITIONS_A_MASQUER if p < len(saines)]
verite = {p: saines[p].consumption_kw for p in positions}
trouee = [
    mesure.model_copy(update={"consumption_kw": None, "consumption_kwh": None})
    if position in verite
    else mesure
    for position, mesure in enumerate(saines)
]
print(f"  {len(verite)} valeurs masquees sur {len(saines)} : positions {positions}")
print("  Elles forment un trou de 1, un trou de 2 et un trou de 3 mesures consecutives.")

reconstruite = forward_fill_series(trouee, MAX_GAP_MEASURES)

print(f"\n  {'heure':<7}{'VRAIE':>10}{'RECONSTRUITE':>14}{'ecart':>10}{'ecart %':>10}  methode")
print("  " + "-" * 78)
erreurs = []
for position in positions:
    attendue = verite[position]
    obtenue = reconstruite[position].consumption_kw
    if obtenue is None:
        print(f"  {saines[position].timestamp:%H:%M}  {affiche(attendue)}"
              f"{'non comblee':>14}{'-':>10}{'-':>10}  "
              f"{reconstruite[position].imputation_method}")
        continue
    ecart = abs(obtenue - attendue)
    erreurs.append(100 * ecart / attendue)
    print(f"  {saines[position].timestamp:%H:%M}  {affiche(attendue)}{affiche(obtenue, 14)}"
          f"{affiche(ecart, 10)}{erreurs[-1]:>9.2f}%  "
          f"{reconstruite[position].imputation_method}")

if erreurs:
    print(f"\n  Erreur moyenne : {sum(erreurs) / len(erreurs):.2f} %")
    print(f"  Erreur maximale : {max(erreurs):.2f} %")
print("\n  Ce chiffre servira de point de comparaison face a l'interpolation lineaire.")
