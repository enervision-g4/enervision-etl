"""Compare les strategies d'imputation sur l'ensemble du parc, pour trancher par la mesure.

La documentation recommande le forward fill pour les sites a consommation stable et
l'interpolation lineaire ailleurs. Ce script verifie si les donnees de l'API mock
soutiennent cette regle, en accumulant les erreurs sur tous les sites et sur plusieurs
fenetres temporelles, plutot que sur un echantillon unique.

Usage:
    uv run python scripts/bilan_strategies.py [resolution_secondes] [nombre_de_fenetres]
"""

import sys
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from statistics import mean
from typing import Optional

from enervision_etl.config import load_settings
from enervision_etl.contracts.energy_reading import EnergyReading
from enervision_etl.extract.errors import MockApiError
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
DUREE_FENETRE_HEURES = 3


def plages_continues(serie: list[EnergyReading]) -> list[list[EnergyReading]]:
    """Decoupe une serie en suites de mesures consecutives sans trou."""
    plages: list[list[EnergyReading]] = []
    courante: list[EnergyReading] = []
    for mesure in serie:
        if mesure.consumption_kw is None:
            if len(courante) >= MIN_PLAGE_CONTINUE:
                plages.append(courante)
            courante = []
        else:
            courante.append(mesure)
    if len(courante) >= MIN_PLAGE_CONTINUE:
        plages.append(courante)
    return plages


def positions_a_masquer(longueur: int) -> list[int]:
    """Place un trou de 1, un trou de 2 et un trou de 3 mesures, sans les coller."""
    pas = max(1, (longueur - 2) // 6)
    isolee = 1 + pas
    paire = isolee + 1 + pas
    triplet = paire + 2 + pas
    candidates = [isolee, paire, paire + 1, triplet, triplet + 1, triplet + 2]
    return [position for position in candidates if 0 < position < longueur - 1]


def erreurs_sur_plage(plage: list[EnergyReading]) -> tuple[list[float], list[float], float]:
    """Masque des valeurs connues et renvoie les erreurs des deux strategies."""
    positions = positions_a_masquer(len(plage))
    verite = {position: plage[position].consumption_kw for position in positions}
    trouee = [
        mesure.model_copy(update={"consumption_kw": None, "consumption_kwh": None})
        if position in verite
        else mesure
        for position, mesure in enumerate(plage)
    ]

    par_recopie = forward_fill_series(trouee, MAX_GAP_MEASURES)
    par_interpolation = linear_interpolation_series(trouee, MAX_GAP_MEASURES)

    erreurs_recopie: list[float] = []
    erreurs_interpolation: list[float] = []
    for position in positions:
        attendue = verite[position]
        if not attendue:
            continue
        for reconstruite, accumulateur in (
            (par_recopie[position].consumption_kw, erreurs_recopie),
            (par_interpolation[position].consumption_kw, erreurs_interpolation),
        ):
            if reconstruite is not None:
                accumulateur.append(100 * abs(reconstruite - attendue) / attendue)

    valeurs = [m.consumption_kw for m in plage if m.consumption_kw is not None]
    variations = [
        100 * abs(suivante - courante) / courante
        for courante, suivante in pairwise(valeurs)
        if courante
    ]
    return erreurs_recopie, erreurs_interpolation, mean(variations) if variations else 0.0


resolution = float(sys.argv[1]) if len(sys.argv) > 1 else 60.0
nombre_fenetres = int(sys.argv[2]) if len(sys.argv) > 2 else 4

settings = load_settings()
maintenant = datetime.now(UTC).replace(tzinfo=None)

recopie_par_type: dict[str, list[float]] = defaultdict(list)
interpolation_par_type: dict[str, list[float]] = defaultdict(list)
variation_par_type: dict[str, list[float]] = defaultdict(list)
plages_par_type: dict[str, int] = defaultdict(int)
type_par_site: dict[str, str] = {}

print(f"Resolution {resolution:.0f} s, {nombre_fenetres} fenetres de "
      f"{DUREE_FENETRE_HEURES} h par site")
print("Collecte en cours...\n")

with ResilientHttpClient(settings.api_mock_base_url, 20.0) as http:
    api = MockApiClient(http)
    referentiel = api.fetch_site_registry()
    for site in referentiel:
        type_par_site[site.site_id] = site.site_type
    sites_collectes = resolve_site_identifiers(settings.sites, referentiel)

    for site_id in sites_collectes:
        site_type = type_par_site.get(site_id, "inconnu")
        plages_trouvees = 0
        for index_fenetre in range(nombre_fenetres):
            fin = maintenant - timedelta(hours=DUREE_FENETRE_HEURES * index_fenetre)
            debut = fin - timedelta(hours=DUREE_FENETRE_HEURES)
            try:
                brutes = api.fetch_readings_window(site_id, debut, fin, resolution)
            except MockApiError as echec:
                print(f"  {site_id} fenetre {index_fenetre} : {echec}")
                continue
            serie = [
                normalize_reading(m, settings.api_mock_source_timezone) for m in brutes
            ]
            for plage in plages_continues(serie):
                recopie, interpolation, variation = erreurs_sur_plage(plage)
                recopie_par_type[site_type].extend(recopie)
                interpolation_par_type[site_type].extend(interpolation)
                variation_par_type[site_type].append(variation)
                plages_par_type[site_type] += 1
                plages_trouvees += 1
        print(f"  {site_id:<10} {site_type:<12} {plages_trouvees} plage(s) exploitable(s)")


def moyenne(valeurs: list[float]) -> Optional[float]:
    return mean(valeurs) if valeurs else None


def colonne(valeur: Optional[float]) -> str:
    return f"{valeur:>10.2f}" if valeur is not None else f"{'-':>10}"


print(f"\n{'=' * 82}\nERREUR MOYENNE PAR TYPE DE SITE\n{'=' * 82}")
print(f"  {'type':<12}{'plages':>8}{'trous':>8}{'ffill %':>10}{'interp %':>10}"
      f"{'signal %':>10}  meilleure")
print("  " + "-" * 76)

for site_type in sorted(plages_par_type):
    recopie_moyenne = moyenne(recopie_par_type[site_type])
    interpolation_moyenne = moyenne(interpolation_par_type[site_type])
    variation_moyenne = moyenne(variation_par_type[site_type])
    if recopie_moyenne is None or interpolation_moyenne is None:
        meilleure = "indeterminee"
    else:
        meilleure = (
            "interpolation" if interpolation_moyenne < recopie_moyenne else "forward_fill"
        )
    print(f"  {site_type:<12}{plages_par_type[site_type]:>8}"
          f"{len(recopie_par_type[site_type]):>8}"
          f"{colonne(recopie_moyenne)}{colonne(interpolation_moyenne)}"
          f"{colonne(variation_moyenne)}  {meilleure}")

toutes_recopies = [e for valeurs in recopie_par_type.values() for e in valeurs]
toutes_interpolations = [e for valeurs in interpolation_par_type.values() for e in valeurs]

print(f"\n{'=' * 82}\nSYNTHESE\n{'=' * 82}")
if not toutes_recopies:
    print("  Aucune plage exploitable sur l'ensemble du parc.")
    sys.exit(0)

print(f"  Trous evalues : {len(toutes_recopies)}")
print(f"  forward_fill         {mean(toutes_recopies):>6.2f} %")
print(f"  linear_interpolation {mean(toutes_interpolations):>6.2f} %")
ecart = 100 * (mean(toutes_recopies) - mean(toutes_interpolations)) / mean(toutes_recopies)
print(f"\n  L'interpolation fait {ecart:+.1f} % de mieux que la recopie sur ce parc.")
print("\n  La documentation recommande le forward fill pour datacenter et hospital.")
print("  Les lignes ci dessus disent si les donnees du mock soutiennent cette regle.")
