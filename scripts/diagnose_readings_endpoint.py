"""Caracterise le taux de valeurs nulles de /api/v1/readings.

La sonde a montre 100 pour cent de nuls sur un appel limit=100 couvrant 24 heures,
alors que le meme endpoint avec limit=3 sur la meme periode renvoyait trois mesures
saines, et que /current est majoritairement sain. Ce script mesure de quoi depend
reellement ce taux : la resolution demandee, l'anciennete des donnees, ou l'etat
instantane des capteurs.
"""

from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import Any

from enervision_etl.config import load_settings
from enervision_etl.extract.errors import MockApiError
from enervision_etl.extract.http_client import ResilientHttpClient

CIBLE = "SITE002"


def section_title(texte: str) -> None:
    print(f"\n{'=' * 78}\n{texte}\n{'=' * 78}")


def null_ratio(measurements: list[dict[str, Any]]) -> float:
    if not measurements:
        return float("nan")
    nuls = sum(1 for measurement in measurements if measurement["consumption_kw"] is None)
    return 100 * nuls / len(measurements)


settings = load_settings()
maintenant = datetime.now(UTC).replace(tzinfo=None)
print(f"Cible : {settings.api_mock_base_url}  site : {CIBLE}")
print(f"Instant de reference (UTC) : {maintenant.isoformat()}")

with ResilientHttpClient(settings.api_mock_base_url, 15.0) as http:

    def fetch_window(start_time: datetime, end_time: datetime, limit: int) -> list[dict[str, Any]]:
        measurements: list[dict[str, Any]] = http.get_json(
            "/api/v1/readings",
            {
                "site_id": CIBLE,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "limit": limit,
            },
        )
        return measurements

    section_title("1. Etat instantane des capteurs (source de verite du simulateur)")
    try:
        sensor_states = http.get_json("/api/v1/sensors/status")
        for site_id, sensor_state in sensor_states.items():
            marque = "  <== cible" if site_id == CIBLE else ""
            failing_sensors = [
                f"{name}(jusqu'a {detail['failing_until']})"
                for name, detail in sensor_state["sensors"].items()
                if detail["status"] != "ok"
            ]
            resume = (
                "pannes: " + ", ".join(failing_sensors)
                if failing_sensors
                else "tous capteurs ok"
            )
            print(f"  {site_id}  overall={sensor_state['overall']:<9} "
                  f"{resume}{marque}")
    except MockApiError as failure:
        print(f"  Endpoint indisponible : {failure}")

    section_title("2. Meme fenetre de 24 h, resolution variable")
    debut_24h, fin_24h = maintenant - timedelta(hours=24), maintenant
    print(f"  {'limit':>6}  {'intervalle':>12}  {'nuls':>7}  data_quality")
    for limit in (1, 3, 10, 25, 50, 100, 250, 500, 1000):
        measurements = fetch_window(debut_24h, fin_24h, limit)
        intervalle = 86400 / limit
        qualities = dict(Counter(m["data_quality"] for m in measurements))
        share = null_ratio(measurements)
        print(f"  {limit:>6}  {intervalle:>10.0f} s  {share:>6.1f}%  {qualities}")

    section_title("3. Resolution fixe (60 s), anciennete de la fenetre variable")
    print(f"  {'fenetre':>22}  {'points':>7}  {'nuls':>7}  data_quality")
    windows = [
        ("derniere heure", 0, 1),
        ("6 dernieres heures", 0, 6),
        ("24 dernieres heures", 0, 24),
        ("il y a 24 a 48 h", 24, 48),
        ("il y a 48 a 72 h", 48, 72),
        ("il y a 7 jours", 168, 169),
    ]
    for etiquette, heures_avant_debut, heures_avant_fin in windows:
        end_time = maintenant - timedelta(hours=heures_avant_debut)
        start_time = maintenant - timedelta(hours=heures_avant_fin)
        limit = min(1000, max(1, int((end_time - start_time).total_seconds() // 60)))
        measurements = fetch_window(start_time, end_time, limit)
        qualities = dict(Counter(m["data_quality"] for m in measurements))
        share = null_ratio(measurements)
        print(f"  {etiquette:>22}  {len(measurements):>7}  {share:>6.1f}%  {qualities}")

    section_title("4. Requete strictement identique, repetee 5 fois")
    print("  Si le taux varie d'un appel a l'autre, le simulateur tire au hasard.")
    for attempt in range(1, 6):
        measurements = fetch_window(maintenant - timedelta(hours=2), maintenant, 120)
        proportion = null_ratio(measurements)
        print(f"  essai {attempt} : {proportion:>6.1f}% de nuls sur {len(measurements)} mesures")

    section_title("5. Echantillon brut : 6 mesures sur les 2 dernieres heures")
    sample = fetch_window(maintenant - timedelta(hours=2), maintenant, 6)
    if not sample:
        print("  Aucune mesure renvoyee sur cette fenetre.")
    for measurement in sample:
        print(f"  {measurement['timestamp']}  kw={measurement['consumption_kw']!s:<8} "
              f"temp={measurement['temperature_celsius']!s:<6} "
              f"quality={measurement['data_quality']:<9} {measurement['null_reasons']}")

    section_title("6. Comparaison directe avec /current au meme instant")
    instantane = http.get_json(f"/api/v1/sites/{CIBLE}/current", site_id=CIBLE)
    print(f"  /current   : kw={instantane['consumption_kw']}  "
          f"quality={instantane['data_quality']}  {instantane['null_reasons']}")
    dernieres = fetch_window(maintenant - timedelta(minutes=10), maintenant, 10)
    if not dernieres:
        print("  /readings  : aucune mesure sur les 10 dernieres minutes")
    else:
        derniere = dernieres[-1]
        print(f"  /readings  : kw={derniere['consumption_kw']}  "
              f"quality={derniere['data_quality']}  {derniere['null_reasons']}")
        print("\n  Ces deux lignes decrivent le meme site a la meme minute. Si l'une est")
        print("  saine et l'autre nulle, les deux endpoints ne partagent pas le meme")
        print("  simulateur, et le mode batch ne peut pas servir de reference.")
