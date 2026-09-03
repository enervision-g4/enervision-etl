"""Eprouve les strategies d'imputation sur les donnees reelles de l'API mock.

Deux volets. Le premier montre ce que l'imputation fait des trous reellement presents
dans une serie. Le second est un controle de justesse : on masque des valeurs dont on
connait la vraie mesure, on impute, et on compare. C'est la seule facon de chiffrer
l'erreur commise par une strategie de reconstruction.

Usage:
    uv run python scripts/measure_imputation_accuracy.py [SITE002] [heures] [resolution_secondes]
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


def section_title(texte: str) -> None:
    print(f"\n{'=' * 82}\n{texte}\n{'=' * 82}")


def formatted(valeur: Optional[float], largeur: int = 9) -> str:
    return f"{valeur:>{largeur}.2f}" if valeur is not None else f"{'NULL':>{largeur}}"


def longest_continuous_run(series: list[EnergyReading]) -> list[EnergyReading]:
    """Isole la plus longue suite de mesures consecutives sans trou.

    Le controle de justesse n'a de sens que sur des mesures reellement voisines dans
    le temps. Compacter la serie en retirant ses trous rendrait adjacentes des mesures
    separees de plusieurs heures, et gonflerait artificiellement l'erreur mesuree.
    """
    runs: list[list[EnergyReading]] = []
    courante: list[EnergyReading] = []
    for measurement in series:
        if measurement.consumption_kw is None:
            if courante:
                runs.append(courante)
            courante = []
        else:
            courante.append(measurement)
    if courante:
        runs.append(courante)
    return max(runs, key=len) if runs else []


def positions_to_mask(length: int) -> list[int]:
    """Place un trou de 1, un trou de 2 et un trou de 3 mesures, sans jamais les coller.

    Les groupes doivent rester separes par au moins une mesure saine, sinon ils
    fusionnent en un seul long trou que la limite fait rejeter. Un point d'ancrage
    est preserve de chaque cote, faute de quoi l'interpolation ne peut pas operer.
    """
    step = max(1, (length - 2) // 6)
    single = 1 + step
    pair = single + 1 + step
    triple = pair + 2 + step
    candidates = [single, pair, pair + 1, triple, triple + 1, triple + 2]
    return [position for position in candidates if 0 < position < length - 1]


requested_site = sys.argv[1] if len(sys.argv) > 1 else "SITE002"
hours = float(sys.argv[2]) if len(sys.argv) > 2 else 2.0
resolution = float(sys.argv[3]) if len(sys.argv) > 3 else 300.0

settings = load_settings()
end_time = datetime.now(UTC).replace(tzinfo=None)
start_time = end_time - timedelta(hours=hours)

with ResilientHttpClient(settings.api_mock_base_url, settings.api_mock_timeout_seconds) as http:
    api = MockApiClient(http)

    def load_messages(site_id: str) -> list[EnergyReading]:
        raw_readings = api.fetch_readings_window(site_id, start_time, end_time, resolution)
        return [normalize_reading(m, settings.api_mock_source_timezone) for m in raw_readings]

    series = load_messages(requested_site)
    print(f"Site {requested_site} : {len(series)} mesures sur {hours} h, "
          f"une toutes les {resolution:.0f} s")

    section_title("VOLET 1 : ce que l'imputation fait des trous reellement presents")
    if not series:
        print("  Aucune mesure sur la periode.")
    else:
        imputed_series = forward_fill_series(series, MAX_GAP_MEASURES)
        gaps = [i for i, m in enumerate(series) if m.consumption_kw is None]
        filled = [r for r in imputed_series if r.imputed_fields]
        print(f"  {len(gaps)} mesures a consumption_kw nul sur {len(series)}")
        print(f"  {len(filled)} mesures reconstruites")
        run_lengths: list[int] = []
        run_length = 0
        for measurement in series:
            if measurement.consumption_kw is None:
                run_length += 1
            elif run_length:
                run_lengths.append(run_length)
                run_length = 0
        if run_length:
            run_lengths.append(run_length)
        if run_lengths:
            print(f"  Longueur des trous, en nombre de mesures : {run_lengths}")
            print(f"  Limite d'imputation configuree : {MAX_GAP_MEASURES} mesures")
            refused_runs = [taille for taille in run_lengths if taille > MAX_GAP_MEASURES]
            if refused_runs:
                print(f"  {len(refused_runs)} trou(s) trop long(s) : {refused_runs}")
                print("  L'imputation les laisse tels quels, c'est le comportement attendu.")

    section_title("VOLET 2 : controle de justesse sur des valeurs dont on connait la verite")
    continuous_run = longest_continuous_run(series)

    if len(continuous_run) < MIN_PLAGE_CONTINUE:
        print(f"  {requested_site} n'offre qu'une plage continue de {len(continuous_run)} mesures.")
        print(f"  Il en faut {MIN_PLAGE_CONTINUE}. Recherche d'un site exploitable.\n")
        print(f"  {'site':<10}{'mesures':>9}{'plage continue':>17}")
        print("  " + "-" * 36)
        best_run: list[EnergyReading] = continuous_run
        best_site = requested_site
        for site_id in resolve_site_identifiers(settings.sites, api.fetch_site_registry()):
            candidate_series = load_messages(site_id) if site_id != requested_site else series
            run = longest_continuous_run(candidate_series)
            print(f"  {site_id:<10}{len(candidate_series):>9}{len(run):>17}")
            if len(run) > len(best_run):
                best_run, best_site = run, site_id
        continuous_run = best_run
        if len(continuous_run) < MIN_PLAGE_CONTINUE:
            print("\n  Aucun site n'offre de plage exploitable en ce moment. Les pannes du")
            print("  simulateur sont trop etendues. Reessayez dans quelques minutes, ou")
            print("  affinez la resolution pour obtenir davantage de points par heure.")
            sys.exit(0)
        print(f"\n  Site retenu pour le controle : {best_site}")

values = [m.consumption_kw for m in continuous_run if m.consumption_kw is not None]
neighbour_deviations = [
    100 * abs(suivante - courante) / courante
    for courante, suivante in pairwise(values)
    if courante
]
mean_variation = sum(neighbour_deviations) / len(neighbour_deviations)
print(f"\n  Plage continue : {len(continuous_run)} mesures, "
      f"de {continuous_run[0].timestamp:%H:%M} a {continuous_run[-1].timestamp:%H:%M}")
print(f"  Consommation : min {min(values):.1f} kW, max {max(values):.1f} kW, "
      f"moyenne {sum(values) / len(values):.1f} kW")
print(f"  Variation entre deux mesures voisines : {mean_variation:.1f} % en moyenne, "
      f"{max(neighbour_deviations):.1f} % au pire")
print("\n  Cette variation est le plancher incompressible : aucune strategie ne peut")
print("  reconstruire mieux que ce que le signal bouge de lui meme.")

positions = positions_to_mask(len(continuous_run))
truth = {p: continuous_run[p].consumption_kw for p in positions}
punched_series = [
    measurement.model_copy(update={"consumption_kw": None, "consumption_kwh": None})
    if position in truth
    else measurement
    for position, measurement in enumerate(continuous_run)
]
print(f"\n  {len(truth)} valeurs masquees aux positions {positions} :")
print("  un trou de 1 mesure, un trou de 2, un trou de 3, separes par des mesures saines.")

by_carry = forward_fill_series(punched_series, MAX_GAP_MEASURES)
by_interpolation = linear_interpolation_series(punched_series, MAX_GAP_MEASURES)

print(f"\n  {'heure':<7}{'ANCRE':>9}{'VRAIE':>9}{'ffill':>10}{'err %':>8}"
      f"{'interp':>10}{'err %':>8}")
print("  " + "-" * 72)

carry_errors: list[float] = []
interpolation_errors: list[float] = []

for position in positions:
    expected = truth[position]
    if expected is None:
        continue
    anchor = next(
        (continuous_run[p].consumption_kw for p in range(position - 1, -1, -1) if p not in truth),
        None,
    )
    columns = ""
    for obtained, erreurs in (
        (by_carry[position].consumption_kw, carry_errors),
        (by_interpolation[position].consumption_kw, interpolation_errors),
    ):
        if obtained is None:
            columns += f"{'-':>10}{'-':>8}"
            continue
        error = 100 * abs(obtained - expected) / expected
        erreurs.append(error)
        columns += f"{obtained:>10.2f}{error:>7.2f}%"
    moment = continuous_run[position].timestamp
    print(f"  {moment:%H:%M}{formatted(anchor)}{formatted(expected)}{columns}")

section_title("VERDICT : quelle strategie pour ce signal ?")
for name, erreurs in (
    ("forward_fill        ", carry_errors),
    ("linear_interpolation", interpolation_errors),
):
    if not erreurs:
        print(f"  {name} aucun trou comble")
        continue
    print(f"  {name} erreur moyenne {sum(erreurs) / len(erreurs):>6.2f} %"
          f"   maximale {max(erreurs):>6.2f} %   ({len(erreurs)}/{len(positions)} combles)")
print(f"  variation du signal  {mean_variation:>20.2f} %   (plancher incompressible)")

if carry_errors and interpolation_errors:
    mean_carry = sum(carry_errors) / len(carry_errors)
    mean_interpolation = sum(interpolation_errors) / len(interpolation_errors)
    best = (
        "linear_interpolation"
        if mean_interpolation < mean_carry
        else "forward_fill"
    )
    print(f"\n  Meilleure sur ce signal : {best}")
