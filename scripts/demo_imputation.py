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
from itertools import pairwise
from typing import Optional

from enervision_etl.config import load_settings
from enervision_etl.contracts.energy_reading import EnergyReading
from enervision_etl.extract.http_client import ResilientHttpClient
from enervision_etl.extract.mock_api_client import MockApiClient
from enervision_etl.transform.imputation import (
    forward_fill_series,
    linear_interpolation_series,
)
from enervision_etl.transform.normalization import normalize_reading

MAX_GAP_MEASURES = 3
MIN_PLAGE_CONTINUE = 12


def titre(texte: str) -> None:
    print(f"\n{'=' * 82}\n{texte}\n{'=' * 82}")


def affiche(valeur: Optional[float], largeur: int = 9) -> str:
    return f"{valeur:>{largeur}.2f}" if valeur is not None else f"{'NULL':>{largeur}}"


def positions_a_masquer(longueur: int) -> list[int]:
    """Place un trou de 1, un trou de 2 et un trou de 3 mesures, bien separes."""
    isolee = longueur // 5
    paire = 2 * longueur // 5
    triplet = 3 * longueur // 5
    candidates = [isolee, paire, paire + 1, triplet, triplet + 1, triplet + 2]
    return sorted({position for position in candidates if 0 < position < longueur})


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

# Le controle n'a de sens que sur des mesures reellement consecutives dans le temps.
# Compacter la serie en retirant ses trous rendrait voisines des mesures separees de
# plusieurs heures, et gonflerait artificiellement l'erreur attribuee a la strategie.
plages: list[list[EnergyReading]] = []
plage_courante: list[EnergyReading] = []
for mesure in serie:
    if mesure.consumption_kw is None:
        if plage_courante:
            plages.append(plage_courante)
        plage_courante = []
    else:
        plage_courante.append(mesure)
if plage_courante:
    plages.append(plage_courante)

continue_ = max(plages, key=len) if plages else []
if len(continue_) < MIN_PLAGE_CONTINUE:
    print(f"  Plus longue plage continue : {len(continue_)} mesures, il en faut "
          f"au moins {MIN_PLAGE_CONTINUE}. Elargissez la fenetre ou affinez la resolution.")
    sys.exit(0)

valeurs = [m.consumption_kw for m in continue_ if m.consumption_kw is not None]
ecarts_voisins = [
    100 * abs(suivante - courante) / courante
    for courante, suivante in pairwise(valeurs)
    if courante
]
variation_moyenne = sum(ecarts_voisins) / len(ecarts_voisins)
print(f"  Plage continue retenue : {len(continue_)} mesures, "
      f"de {continue_[0].timestamp:%H:%M} a {continue_[-1].timestamp:%H:%M}")
print(f"  Consommation : min {min(valeurs):.1f} kW, max {max(valeurs):.1f} kW, "
      f"moyenne {sum(valeurs) / len(valeurs):.1f} kW")
print(f"  Variation moyenne entre deux mesures voisines : {variation_moyenne:.1f} %")
print(f"  Variation maximale entre deux mesures voisines : {max(ecarts_voisins):.1f} %")
print("\n  Ce dernier chiffre est le plancher incompressible : aucune strategie ne peut")
print("  faire mieux que la variation naturelle du signal qu'elle tente de reconstituer.")

positions = positions_a_masquer(len(continue_))
verite = {p: continue_[p].consumption_kw for p in positions}
trouee = [
    mesure.model_copy(update={"consumption_kw": None, "consumption_kwh": None})
    if position in verite
    else mesure
    for position, mesure in enumerate(continue_)
]
print(f"\n  {len(verite)} valeurs masquees aux positions {positions} :")
print("  un trou de 1 mesure, un trou de 2, un trou de 3 consecutives.")

par_recopie = forward_fill_series(trouee, MAX_GAP_MEASURES)
par_interpolation = linear_interpolation_series(trouee, MAX_GAP_MEASURES)

print(f"\n  {'heure':<7}{'ANCRE':>9}{'VRAIE':>9}"
      f"{'ffill':>10}{'err %':>8}{'interp':>11}{'err %':>8}")
print("  " + "-" * 74)

erreurs_recopie: list[float] = []
erreurs_interpolation: list[float] = []

for position in positions:
    attendue = verite[position]
    if attendue is None:
        continue
    ancre = next(
        (continue_[p].consumption_kw for p in range(position - 1, -1, -1) if p not in verite),
        None,
    )
    obtenue_recopie = par_recopie[position].consumption_kw
    obtenue_interpolation = par_interpolation[position].consumption_kw

    if obtenue_recopie is not None:
        erreur_recopie = 100 * abs(obtenue_recopie - attendue) / attendue
        erreurs_recopie.append(erreur_recopie)
        colonne_recopie = f"{obtenue_recopie:>10.2f}{erreur_recopie:>7.2f}%"
    else:
        colonne_recopie = f"{'-':>10}{'-':>8}"

    if obtenue_interpolation is not None:
        erreur_interpolation = 100 * abs(obtenue_interpolation - attendue) / attendue
        erreurs_interpolation.append(erreur_interpolation)
        colonne_interpolation = f"{obtenue_interpolation:>11.2f}{erreur_interpolation:>7.2f}%"
    else:
        colonne_interpolation = f"{'non comblee':>11}{'-':>8}"

    print(f"  {continue_[position].timestamp:%H:%M}{affiche(ancre)}{affiche(attendue)}"
          f"{colonne_recopie}{colonne_interpolation}")

titre("VERDICT : quelle strategie pour ce site ?")
for nom, erreurs in (
    ("forward_fill        ", erreurs_recopie),
    ("linear_interpolation", erreurs_interpolation),
):
    if not erreurs:
        print(f"  {nom} aucun trou comble")
        continue
    print(f"  {nom} erreur moyenne {sum(erreurs) / len(erreurs):>6.2f} %"
          f"   maximale {max(erreurs):>6.2f} %"
          f"   ({len(erreurs)}/{len(positions)} combles)")
print(f"  variation du signal  {variation_moyenne:>20.2f} %   (plancher incompressible)")

if erreurs_recopie and erreurs_interpolation:
    moyenne_recopie = sum(erreurs_recopie) / len(erreurs_recopie)
    moyenne_interpolation = sum(erreurs_interpolation) / len(erreurs_interpolation)
    meilleure = (
        "linear_interpolation"
        if moyenne_interpolation < moyenne_recopie
        else "forward_fill"
    )
    print(f"\n  Meilleure sur ce site : {meilleure}")
    print("  Noter que l'interpolation comble moins de trous : elle exige un point")
    print("  d'ancrage des deux cotes, donc elle renonce en fin de serie la ou le")
    print("  forward fill se contente du passe. Ce compromis est le sujet du commit 8.")
