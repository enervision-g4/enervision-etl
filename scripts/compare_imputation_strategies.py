"""Compare les strategies d'imputation sur l'ensemble du parc, pour trancher par la mesure.

L'equipe fait l'hypothese qu'un site a consommation stable gagne moins a
l'interpolation qu'a la recopie de la derniere valeur connue. Ce script verifie si les
donnees de l'API mock soutiennent cette hypothese, en accumulant les erreurs sur tous
les sites et sur plusieurs fenetres temporelles, plutot que sur un echantillon unique.

Usage:
    uv run python scripts/compare_imputation_strategies.py [resolution_s] [nb_fenetres]
"""

import sys
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from statistics import mean
from typing import Optional

from enervision_contracts.energy_reading import EnergyReading
from enervision_etl.config import load_settings
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


def continuous_runs(series: list[EnergyReading]) -> list[list[EnergyReading]]:
    """Decoupe une serie en suites de mesures consecutives sans trou."""
    runs: list[list[EnergyReading]] = []
    courante: list[EnergyReading] = []
    for measurement in series:
        if measurement.consumption_kw is None:
            if len(courante) >= MIN_PLAGE_CONTINUE:
                runs.append(courante)
            courante = []
        else:
            courante.append(measurement)
    if len(courante) >= MIN_PLAGE_CONTINUE:
        runs.append(courante)
    return runs


def positions_to_mask(length: int) -> list[int]:
    """Place un trou de 1, un trou de 2 et un trou de 3 mesures, sans les coller."""
    step = max(1, (length - 2) // 6)
    single = 1 + step
    pair = single + 1 + step
    triple = pair + 2 + step
    candidates = [single, pair, pair + 1, triple, triple + 1, triple + 2]
    return [position for position in candidates if 0 < position < length - 1]


def errors_on_run(run: list[EnergyReading]) -> tuple[list[float], list[float], float]:
    """Masque des valeurs connues et renvoie les erreurs des deux strategies."""
    positions = positions_to_mask(len(run))
    truth = {position: run[position].consumption_kw for position in positions}
    punched_series = [
        measurement.model_copy(update={"consumption_kw": None, "consumption_kwh": None})
        if position in truth
        else measurement
        for position, measurement in enumerate(run)
    ]

    by_carry = forward_fill_series(punched_series, MAX_GAP_MEASURES)
    by_interpolation = linear_interpolation_series(punched_series, MAX_GAP_MEASURES)

    carry_errors: list[float] = []
    interpolation_errors: list[float] = []
    for position in positions:
        expected = truth[position]
        if not expected:
            continue
        for reconstructed, accumulateur in (
            (by_carry[position].consumption_kw, carry_errors),
            (by_interpolation[position].consumption_kw, interpolation_errors),
        ):
            if reconstructed is not None:
                accumulateur.append(100 * abs(reconstructed - expected) / expected)

    values = [m.consumption_kw for m in run if m.consumption_kw is not None]
    variations = [
        100 * abs(suivante - courante) / courante
        for courante, suivante in pairwise(values)
        if courante
    ]
    return carry_errors, interpolation_errors, mean(variations) if variations else 0.0


resolution = float(sys.argv[1]) if len(sys.argv) > 1 else 60.0
nombre_fenetres = int(sys.argv[2]) if len(sys.argv) > 2 else 4

settings = load_settings()
maintenant = datetime.now(UTC).replace(tzinfo=None)

carry_errors_by_type: dict[str, list[float]] = defaultdict(list)
interpolation_errors_by_type: dict[str, list[float]] = defaultdict(list)
variation_by_type: dict[str, list[float]] = defaultdict(list)
runs_by_type: dict[str, int] = defaultdict(int)
type_by_site: dict[str, str] = {}

print(f"Resolution {resolution:.0f} s, {nombre_fenetres} fenetres de "
      f"{DUREE_FENETRE_HEURES} h par site")
print("Collecte en cours...\n")

with ResilientHttpClient(settings.api_mock_base_url, 20.0) as http:
    api = MockApiClient(http)
    registry = api.fetch_site_registry()
    for site in registry:
        type_by_site[site.site_id] = site.site_type
    collected_sites = resolve_site_identifiers(settings.sites, registry)

    for site_id in collected_sites:
        site_type = type_by_site.get(site_id, "inconnu")
        runs_found = 0
        for window_index in range(nombre_fenetres):
            end_time = maintenant - timedelta(hours=DUREE_FENETRE_HEURES * window_index)
            start_time = end_time - timedelta(hours=DUREE_FENETRE_HEURES)
            try:
                raw_readings = api.fetch_readings_window(site_id, start_time, end_time, resolution)
            except MockApiError as failure:
                print(f"  {site_id} fenetre {window_index} : {failure}")
                continue
            series = [
                normalize_reading(m, settings.api_mock_source_timezone) for m in raw_readings
            ]
            for run in continuous_runs(series):
                carried, interpolated, variation = errors_on_run(run)
                carry_errors_by_type[site_type].extend(carried)
                interpolation_errors_by_type[site_type].extend(interpolated)
                variation_by_type[site_type].append(variation)
                runs_by_type[site_type] += 1
                runs_found += 1
        print(f"  {site_id:<10} {site_type:<12} {runs_found} plage(s) exploitable(s)")


def mean_of(values: list[float]) -> Optional[float]:
    return mean(values) if values else None


def column(valeur: Optional[float]) -> str:
    return f"{valeur:>10.2f}" if valeur is not None else f"{'-':>10}"


print(f"\n{'=' * 82}\nERREUR MOYENNE PAR TYPE DE SITE\n{'=' * 82}")
print(f"  {'type':<12}{'plages':>8}{'trous':>8}{'ffill %':>10}{'interp %':>10}"
      f"{'signal %':>10}  meilleure")
print("  " + "-" * 76)

for site_type in sorted(runs_by_type):
    mean_carry_error = mean_of(carry_errors_by_type[site_type])
    mean_interpolation_error = mean_of(interpolation_errors_by_type[site_type])
    mean_variation = mean_of(variation_by_type[site_type])
    if mean_carry_error is None or mean_interpolation_error is None:
        best = "indeterminee"
    else:
        best = (
            "interpolation" if mean_interpolation_error < mean_carry_error else "forward_fill"
        )
    print(f"  {site_type:<12}{runs_by_type[site_type]:>8}"
          f"{len(carry_errors_by_type[site_type]):>8}"
          f"{column(mean_carry_error)}{column(mean_interpolation_error)}"
          f"{column(mean_variation)}  {best}")

all_carry_errors = [e for values in carry_errors_by_type.values() for e in values]
all_interpolation_errors = [e for values in interpolation_errors_by_type.values() for e in values]

print(f"\n{'=' * 82}\nSYNTHESE\n{'=' * 82}")
if not all_carry_errors:
    print("  Aucune plage exploitable sur l'ensemble du parc.")
    sys.exit(0)

print(f"  Trous evalues : {len(all_carry_errors)}")
print(f"  forward_fill         {mean(all_carry_errors):>6.2f} %")
print(f"  linear_interpolation {mean(all_interpolation_errors):>6.2f} %")
deviation = 100 * (mean(all_carry_errors) - mean(all_interpolation_errors)) / mean(all_carry_errors)
print(f"\n  L'interpolation fait {deviation:+.1f} % de mieux que la recopie sur ce parc.")
print("\n  Hypothese testee : forward fill preferable sur datacenter et hospital.")
print("  Les lignes ci dessus disent si les donnees du mock la soutiennent.")
