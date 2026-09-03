"""Confronte l'API mock reelle au contrat que le collecteur suppose.

A lancer avant de developper contre une nouvelle instance : les ecarts entre le
contrat suppose et l'instance deployee sont la premiere cause de bug silencieux.
"""
import sys
from datetime import datetime, timedelta
from itertools import pairwise

from pydantic import ValidationError

from enervision_contracts.energy_reading import (
    KNOWN_DATA_QUALITY_LEVELS,
    MEASUREMENT_FIELD_NAMES,
    EnergyReading,
)
from enervision_etl.config import load_settings
from enervision_etl.extract.http_client import ResilientHttpClient
from enervision_etl.extract.mock_api_client import MockApiClient
from enervision_etl.extract.site_selection import (
    UnknownConfiguredSiteError,
    resolve_site_identifiers,
)

DOCUMENTED_FIELDS = frozenset(
    {
        "timestamp",
        "site_id",
        "site_type",
        "null_reasons",
        "data_quality",
        *MEASUREMENT_FIELD_NAMES,
    }
)

anomalies: list[str] = []


def section_title(texte: str) -> None:
    print(f"\n{'=' * 72}\n{texte}\n{'=' * 72}")


settings = load_settings()
print(f"Cible : {settings.api_mock_base_url}")
print(f"Sites configures : {', '.join(settings.sites) if settings.sites else 'ALL'}")

with ResilientHttpClient(settings.api_mock_base_url, settings.api_mock_timeout_seconds) as http:
    api = MockApiClient(http)

    section_title("1. Disponibilite")
    if not api.is_healthy():
        print("  /health ne repond pas 'healthy'. Arret.")
        sys.exit(1)
    print("  /health : healthy")

    section_title("2. Referentiel des sites")
    sites = api.fetch_site_registry()
    exposed_site_ids = {site.site_id for site in sites}
    for site in sites:
        print(
            f"  {site.site_id}  {site.site_type:<14} "
            f"{site.capacity_kw:>7.0f} kW  {site.site_name}"
        )
    try:
        collected_sites = resolve_site_identifiers(settings.sites, sites)
    except (UnknownConfiguredSiteError, ValueError) as failure:
        anomalies.append(str(failure))
        collected_sites = []
    if not collected_sites:
        print("\n  La configuration SITES ne peut pas etre resolue, voir le verdict.")
    elif settings.collects_every_site:
        print(f"\n  SITES n'impose aucune restriction : les {len(exposed_site_ids)} sites "
              "exposes seront collectes.")
    else:
        print(f"\n  SITES restreint la collecte a {len(collected_sites)} des "
              f"{len(exposed_site_ids)} sites exposes : {collected_sites}")

    section_title("3. Conformite du contrat EnergyReading sur /current")
    qualities: dict[str, int] = {}
    causes: dict[str, int] = {}
    for site_id in (collected_sites or sorted(exposed_site_ids)):
        raw_text = http.get_json(f"/api/v1/sites/{site_id}/current", site_id=site_id)

        inconnus = set(raw_text) - DOCUMENTED_FIELDS
        if inconnus:
            anomalies.append(
                f"{site_id} : champs non documentes dans /current : {sorted(inconnus)}"
            )
        absents = DOCUMENTED_FIELDS - set(raw_text)
        if absents:
            anomalies.append(
                f"{site_id} : champs documentes absents de /current : {sorted(absents)}"
            )

        try:
            lecture = EnergyReading.model_validate(raw_text)
        except ValidationError as failure:
            anomalies.append(
                f"{site_id} : la reponse ne valide pas le contrat : {failure}"
            )
            continue

        qualities[lecture.data_quality] = qualities.get(lecture.data_quality, 0) + 1
        for cause in lecture.null_reasons:
            causes[cause] = causes.get(cause, 0) + 1
        if not lecture.has_known_data_quality():
            anomalies.append(f"{site_id} : data_quality inconnu '{lecture.data_quality}'")

        missing = lecture.missing_measurement_fields()
        print(f"  {site_id}  quality={lecture.data_quality:<9} muets={len(missing)}/7  "
              f"consumption_kw={lecture.consumption_kw}")

    print(f"\n  Repartition data_quality : {qualities}")
    print(f"  Valeurs attendues        : {sorted(KNOWN_DATA_QUALITY_LEVELS)}")
    print(f"  null_reasons rencontres  : {causes or 'aucun'}")

    section_title("4. Endpoint batch /readings")
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=6)
    ordered_site_ids = sorted(exposed_site_ids)
    target_site = ordered_site_ids[1] if len(ordered_site_ids) > 1 else ordered_site_ids[0]
    series = api.fetch_readings_window(target_site, start_time, end_time, resolution_seconds=60.0)
    if not series:
        anomalies.append(f"/readings n'a renvoye aucune mesure pour {target_site} sur 6 heures")
        print(f"  {target_site} : aucune mesure sur la periode")
    else:
        gaps = [m for m in series if m.consumption_kw is None]
        croissant = all(a.timestamp < b.timestamp for a, b in pairwise(series))
        step = {
            int((suivante.timestamp - courante.timestamp).total_seconds())
            for courante, suivante in pairwise(series)
        }
        print(
            f"  {target_site} : {len(series)} mesures "
            f"de {series[0].timestamp} a {series[-1].timestamp}"
        )
        print(f"  horodatages strictement croissants : {croissant}")
        print(f"  pas observes entre mesures (s) : {sorted(step)[:6]}")
        null_share = 100 * len(gaps) / len(series)
        print(f"  mesures a consumption_kw null : {len(gaps)} ({null_share:.1f} %)")
        if not croissant:
            anomalies.append("/readings ne renvoie pas une serie strictement croissante")
        if len(step) > 1:
            print("\n  Le pas n'est pas regulier : l'interpolation devra ponderer par le temps")
            print("  ecoule, et non supposer des intervalles egaux.")

    section_title("5. Fuseau horaire des horodatages")
    sample = series[0].timestamp if series else None
    if sample is not None:
        naif = sample.tzinfo is None
        print(f"  Exemple : {sample.isoformat()}")
        print(f"  Horodatage naif (sans fuseau) : {naif}")
        derniere_mesure = series[-1].timestamp.replace(tzinfo=None)
        deviation = (datetime.now() - derniere_mesure).total_seconds() / 3600
        print(f"  Ecart entre la derniere mesure et l'heure locale : {deviation:.2f} h")
        print("  Un ecart proche de 0 h suggere que l'API emet en heure locale.")
        print("  Un ecart proche du decalage UTC de la machine suggere de l'UTC.")

section_title("VERDICT")
if anomalies:
    print(f"  {len(anomalies)} ecart(s) entre le contrat attendu et l'instance reelle :\n")
    for anomaly in anomalies:
        print(f"    - {anomaly}")
    sys.exit(1)
print("  Aucun ecart. L'instance est conforme au contrat attendu par le collecteur.")
