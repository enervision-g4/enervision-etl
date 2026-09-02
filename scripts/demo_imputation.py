"""Eprouve les strategies d'imputation sur les donnees reelles de l'API mock.

Deux volets. Le premier montre ce que l'imputation fait des trous reellement presents
dans une serie. Le second est un controle de justesse : on masque des valeurs dont on
connait la vraie mesure, on impute, et on compare. C'est la seule facon de chiffrer
l'erreur commise par une strategie de reconstruction.

Usage:
    uv run python scripts/demo_imputation.py [SITE002] [heures] [resolution_secondes]
"""

import sys
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from typing import Optional

from enervision_contracts.energy_reading import EnergyReading
from enervision_etl.config import load_settings
from enervision_etl.extract.http_client import ResilientHttpClient
from enervision_etl.extract.mock_api_client import MockApiClient
from enervision_etl.extract.site_selection import resolve_site_identifiers
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


def plus_longue_plage_continue(serie: list[EnergyReading]) -> list[EnergyReading]:
    """Isole la plus longue suite de mesures consecutives sans trou.

    Le controle de justesse n'a de sens que sur des mesures reellement voisines dans
    le temps. Compacter la serie en retirant ses trous rendrait adjacentes des mesures
    separees de plusieurs heures, et gonflerait artificiellement l'erreur mesuree.
    """
    plages: list[list[EnergyReading]] = []
    courante: list[EnergyReading] = []
    for mesure in serie:
        if mesure.consumption_kw is None:
            if courante:
                plages.append(courante)
            courante = []
        else:
            courante.append(mesure)
    if courante:
        plages.append(courante)
    return max(plages, key=len) if plages else []


def positions_a_masquer(longueur: int) -> list[int]:
    """Place un trou de 1, un trou de 2 et un trou de 3 mesures, sans jamais les coller.

    Les groupes doivent rester separes par au moins une mesure saine, sinon ils
    fusionnent en un seul long trou que la limite fait rejeter. Un point d'ancrage
    est preserve de chaque cote, faute de quoi l'interpolation ne peut pas operer.
    """
    pas = max(1, (longueur - 2) // 6)
    isolee = 1 + pas
    paire = isolee + 1 + pas
    triplet = paire + 2 + pas
    candidates = [isolee, paire, paire + 1, triplet, triplet + 1, triplet + 2]
    return [position for position in candidates if 0 < position < longueur - 1]


site_demande = sys.argv[1] if len(sys.argv) > 1 else "SITE002"
heures = float(sys.argv[2]) if len(sys.argv) > 2 else 2.0
resolution = float(sys.argv[3]) if len(sys.argv) > 3 else 300.0

settings = load_settings()
fin = datetime.now(UTC).replace(tzinfo=None)
debut = fin - timedelta(hours=heures)

with ResilientHttpClient(settings.api_mock_base_url, settings.api_mock_timeout_seconds) as http:
    api = MockApiClient(http)

    def charger(site_id: str) -> list[EnergyReading]:
        brutes = api.fetch_readings_window(site_id, debut, fin, resolution)
        return [normalize_reading(m, settings.api_mock_source_timezone) for m in brutes]

    serie = charger(site_demande)
    print(f"Site {site_demande} : {len(serie)} mesures sur {heures} h, "
          f"une toutes les {resolution:.0f} s")

    titre("VOLET 1 : ce que l'imputation fait des trous reellement presents")
    if not serie:
        print("  Aucune mesure sur la periode.")
    else:
        imputee = forward_fill_series(serie, MAX_GAP_MEASURES)
        trous = [i for i, m in enumerate(serie) if m.consumption_kw is None]
        combles = [r for r in imputee if r.imputed_fields]
        print(f"  {len(trous)} mesures a consumption_kw nul sur {len(serie)}")
        print(f"  {len(combles)} mesures reconstruites")
        longueurs: list[int] = []
        compteur = 0
        for mesure in serie:
            if mesure.consumption_kw is None:
                compteur += 1
            elif compteur:
                longueurs.append(compteur)
                compteur = 0
        if compteur:
            longueurs.append(compteur)
        if longueurs:
            print(f"  Longueur des trous, en nombre de mesures : {longueurs}")
            print(f"  Limite d'imputation configuree : {MAX_GAP_MEASURES} mesures")
            refuses = [taille for taille in longueurs if taille > MAX_GAP_MEASURES]
            if refuses:
                print(f"  {len(refuses)} trou(s) trop long(s) pour etre comble(s) : {refuses}")
                print("  L'imputation les laisse tels quels, c'est le comportement attendu.")

    titre("VOLET 2 : controle de justesse sur des valeurs dont on connait la verite")
    continue_ = plus_longue_plage_continue(serie)

    if len(continue_) < MIN_PLAGE_CONTINUE:
        print(f"  {site_demande} n'offre qu'une plage continue de {len(continue_)} mesures.")
        print(f"  Il en faut {MIN_PLAGE_CONTINUE}. Recherche d'un site exploitable.\n")
        print(f"  {'site':<10}{'mesures':>9}{'plage continue':>17}")
        print("  " + "-" * 36)
        meilleure_plage: list[EnergyReading] = continue_
        meilleur_site = site_demande
        for site_id in resolve_site_identifiers(settings.sites, api.fetch_site_registry()):
            candidate = charger(site_id) if site_id != site_demande else serie
            plage = plus_longue_plage_continue(candidate)
            print(f"  {site_id:<10}{len(candidate):>9}{len(plage):>17}")
            if len(plage) > len(meilleure_plage):
                meilleure_plage, meilleur_site = plage, site_id
        continue_ = meilleure_plage
        if len(continue_) < MIN_PLAGE_CONTINUE:
            print("\n  Aucun site n'offre de plage exploitable en ce moment. Les pannes du")
            print("  simulateur sont trop etendues. Reessayez dans quelques minutes, ou")
            print("  affinez la resolution pour obtenir davantage de points par heure.")
            sys.exit(0)
        print(f"\n  Site retenu pour le controle : {meilleur_site}")

valeurs = [m.consumption_kw for m in continue_ if m.consumption_kw is not None]
ecarts_voisins = [
    100 * abs(suivante - courante) / courante
    for courante, suivante in pairwise(valeurs)
    if courante
]
variation_moyenne = sum(ecarts_voisins) / len(ecarts_voisins)
print(f"\n  Plage continue : {len(continue_)} mesures, "
      f"de {continue_[0].timestamp:%H:%M} a {continue_[-1].timestamp:%H:%M}")
print(f"  Consommation : min {min(valeurs):.1f} kW, max {max(valeurs):.1f} kW, "
      f"moyenne {sum(valeurs) / len(valeurs):.1f} kW")
print(f"  Variation entre deux mesures voisines : {variation_moyenne:.1f} % en moyenne, "
      f"{max(ecarts_voisins):.1f} % au pire")
print("\n  Cette variation est le plancher incompressible : aucune strategie ne peut")
print("  reconstruire mieux que ce que le signal bouge de lui meme.")

positions = positions_a_masquer(len(continue_))
verite = {p: continue_[p].consumption_kw for p in positions}
trouee = [
    mesure.model_copy(update={"consumption_kw": None, "consumption_kwh": None})
    if position in verite
    else mesure
    for position, mesure in enumerate(continue_)
]
print(f"\n  {len(verite)} valeurs masquees aux positions {positions} :")
print("  un trou de 1 mesure, un trou de 2, un trou de 3, separes par des mesures saines.")

par_recopie = forward_fill_series(trouee, MAX_GAP_MEASURES)
par_interpolation = linear_interpolation_series(trouee, MAX_GAP_MEASURES)

print(f"\n  {'heure':<7}{'ANCRE':>9}{'VRAIE':>9}{'ffill':>10}{'err %':>8}"
      f"{'interp':>10}{'err %':>8}")
print("  " + "-" * 72)

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
    colonnes = ""
    for obtenue, erreurs in (
        (par_recopie[position].consumption_kw, erreurs_recopie),
        (par_interpolation[position].consumption_kw, erreurs_interpolation),
    ):
        if obtenue is None:
            colonnes += f"{'-':>10}{'-':>8}"
            continue
        erreur = 100 * abs(obtenue - attendue) / attendue
        erreurs.append(erreur)
        colonnes += f"{obtenue:>10.2f}{erreur:>7.2f}%"
    print(f"  {continue_[position].timestamp:%H:%M}{affiche(ancre)}{affiche(attendue)}{colonnes}")

titre("VERDICT : quelle strategie pour ce signal ?")
for nom, erreurs in (
    ("forward_fill        ", erreurs_recopie),
    ("linear_interpolation", erreurs_interpolation),
):
    if not erreurs:
        print(f"  {nom} aucun trou comble")
        continue
    print(f"  {nom} erreur moyenne {sum(erreurs) / len(erreurs):>6.2f} %"
          f"   maximale {max(erreurs):>6.2f} %   ({len(erreurs)}/{len(positions)} combles)")
print(f"  variation du signal  {variation_moyenne:>20.2f} %   (plancher incompressible)")

if erreurs_recopie and erreurs_interpolation:
    moyenne_recopie = sum(erreurs_recopie) / len(erreurs_recopie)
    moyenne_interpolation = sum(erreurs_interpolation) / len(erreurs_interpolation)
    meilleure = (
        "linear_interpolation"
        if moyenne_interpolation < moyenne_recopie
        else "forward_fill"
    )
    print(f"\n  Meilleure sur ce signal : {meilleure}")
