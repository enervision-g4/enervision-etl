"""Confronte l'API mock reelle au contrat que le collecteur suppose.

A lancer avant de developper contre une nouvelle instance : les ecarts entre le
contrat suppose et l'instance deployee sont la premiere cause de bug silencieux.
"""
import sys
from datetime import datetime, timedelta
from itertools import pairwise

from pydantic import ValidationError

from enervision_etl.config import load_settings
from enervision_etl.contracts.energy_reading import (
    KNOWN_DATA_QUALITY_LEVELS,
    MEASUREMENT_FIELD_NAMES,
    EnergyReading,
)
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


def titre(texte: str) -> None:
    print(f"\n{'=' * 72}\n{texte}\n{'=' * 72}")


settings = load_settings()
print(f"Cible : {settings.api_mock_base_url}")
print(f"Sites configures : {', '.join(settings.sites) if settings.sites else 'ALL'}")

with ResilientHttpClient(settings.api_mock_base_url, settings.api_mock_timeout_seconds) as http:
    api = MockApiClient(http)

    titre("1. Disponibilite")
    if not api.is_healthy():
        print("  /health ne repond pas 'healthy'. Arret.")
        sys.exit(1)
    print("  /health : healthy")

    titre("2. Referentiel des sites")
    sites = api.fetch_site_registry()
    exposes = {site.site_id for site in sites}
    for site in sites:
        print(
            f"  {site.site_id}  {site.site_type:<14} "
            f"{site.capacity_kw:>7.0f} kW  {site.site_name}"
        )
    try:
        collectes = resolve_site_identifiers(settings.sites, sites)
    except (UnknownConfiguredSiteError, ValueError) as echec:
        anomalies.append(str(echec))
        collectes = []
    if not collectes:
        print("\n  La configuration SITES ne peut pas etre resolue, voir le verdict.")
    elif settings.collects_every_site:
        print(f"\n  SITES n'impose aucune restriction : les {len(exposes)} sites "
              "exposes seront collectes.")
    else:
        print(f"\n  SITES restreint la collecte a {len(collectes)} des "
              f"{len(exposes)} sites exposes : {collectes}")

    titre("3. Conformite du contrat EnergyReading sur /current")
    qualites: dict[str, int] = {}
    causes: dict[str, int] = {}
    for site_id in (collectes or sorted(exposes)):
        brut = http.get_json(f"/api/v1/sites/{site_id}/current", site_id=site_id)

        inconnus = set(brut) - DOCUMENTED_FIELDS
        if inconnus:
            anomalies.append(
                f"{site_id} : champs non documentes dans /current : {sorted(inconnus)}"
            )
        absents = DOCUMENTED_FIELDS - set(brut)
        if absents:
            anomalies.append(
                f"{site_id} : champs documentes absents de /current : {sorted(absents)}"
            )

        try:
            lecture = EnergyReading.model_validate(brut)
        except ValidationError as echec:
            anomalies.append(
                f"{site_id} : la reponse ne valide pas le contrat : {echec}"
            )
            continue

        qualites[lecture.data_quality] = qualites.get(lecture.data_quality, 0) + 1
        for cause in lecture.null_reasons:
            causes[cause] = causes.get(cause, 0) + 1
        if not lecture.has_known_data_quality():
            anomalies.append(f"{site_id} : data_quality inconnu '{lecture.data_quality}'")

        muets = lecture.missing_measurement_fields()
        print(f"  {site_id}  quality={lecture.data_quality:<9} muets={len(muets)}/7  "
              f"consumption_kw={lecture.consumption_kw}")

    print(f"\n  Repartition data_quality : {qualites}")
    print(f"  Valeurs attendues        : {sorted(KNOWN_DATA_QUALITY_LEVELS)}")
    print(f"  null_reasons rencontres  : {causes or 'aucun'}")

    titre("4. Endpoint batch /readings")
    fin = datetime.now()
    debut = fin - timedelta(hours=6)
    cible = sorted(exposes)[1] if len(exposes) > 1 else sorted(exposes)[0]
    serie = api.fetch_readings_window(cible, debut, fin, resolution_seconds=60.0)
    if not serie:
        anomalies.append(f"/readings n'a renvoye aucune mesure pour {cible} sur 6 heures")
        print(f"  {cible} : aucune mesure sur la periode")
    else:
        trous = [m for m in serie if m.consumption_kw is None]
        croissant = all(a.timestamp < b.timestamp for a, b in pairwise(serie))
        pas = {
            int((suivante.timestamp - courante.timestamp).total_seconds())
            for courante, suivante in pairwise(serie)
        }
        print(
            f"  {cible} : {len(serie)} mesures "
            f"de {serie[0].timestamp} a {serie[-1].timestamp}"
        )
        print(f"  horodatages strictement croissants : {croissant}")
        print(f"  pas observes entre mesures (s) : {sorted(pas)[:6]}")
        proportion_nulle = 100 * len(trous) / len(serie)
        print(f"  mesures a consumption_kw null : {len(trous)} ({proportion_nulle:.1f} %)")
        if not croissant:
            anomalies.append("/readings ne renvoie pas une serie strictement croissante")
        if len(pas) > 1:
            print("\n  Le pas n'est pas regulier : l'interpolation devra ponderer par le temps")
            print("  ecoule, et non supposer des intervalles egaux.")

    titre("5. Fuseau horaire des horodatages")
    echantillon = serie[0].timestamp if serie else None
    if echantillon is not None:
        naif = echantillon.tzinfo is None
        print(f"  Exemple : {echantillon.isoformat()}")
        print(f"  Horodatage naif (sans fuseau) : {naif}")
        derniere_mesure = serie[-1].timestamp.replace(tzinfo=None)
        ecart = (datetime.now() - derniere_mesure).total_seconds() / 3600
        print(f"  Ecart entre la derniere mesure et l'heure locale : {ecart:.2f} h")
        print("  Un ecart proche de 0 h suggere que l'API emet en heure locale.")
        print("  Un ecart proche du decalage UTC de la machine suggere de l'UTC.")

titre("VERDICT")
if anomalies:
    print(f"  {len(anomalies)} ecart(s) entre le contrat attendu et l'instance reelle :\n")
    for anomalie in anomalies:
        print(f"    - {anomalie}")
    sys.exit(1)
print("  Aucun ecart. L'instance est conforme au contrat attendu par le collecteur.")
