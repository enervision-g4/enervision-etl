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


def titre(texte: str) -> None:
    print(f"\n{'=' * 78}\n{texte}\n{'=' * 78}")


def taux_de_nuls(mesures: list[dict[str, Any]]) -> float:
    if not mesures:
        return float("nan")
    nuls = sum(1 for mesure in mesures if mesure["consumption_kw"] is None)
    return 100 * nuls / len(mesures)


settings = load_settings()
maintenant = datetime.now(UTC).replace(tzinfo=None)
print(f"Cible : {settings.api_mock_base_url}  site : {CIBLE}")
print(f"Instant de reference (UTC) : {maintenant.isoformat()}")

with ResilientHttpClient(settings.api_mock_base_url, 15.0) as http:

    def lire(debut: datetime, fin: datetime, limit: int) -> list[dict[str, Any]]:
        return http.get_json(
            "/api/v1/readings",
            {
                "site_id": CIBLE,
                "start_time": debut.isoformat(),
                "end_time": fin.isoformat(),
                "limit": limit,
            },
        )

    titre("1. Etat instantane des capteurs (source de verite du simulateur)")
    try:
        etats = http.get_json("/api/v1/sensors/status")
        for site_id, etat in etats.items():
            marque = "  <== cible" if site_id == CIBLE else ""
            pannes = [
                f"{nom}(jusqu'a {detail['failing_until']})"
                for nom, detail in etat["sensors"].items()
                if detail["status"] != "ok"
            ]
            print(f"  {site_id}  overall={etat['overall']:<9} "
                  f"{'pannes: ' + ', '.join(pannes) if pannes else 'tous capteurs ok'}{marque}")
    except MockApiError as echec:
        print(f"  Endpoint indisponible : {echec}")

    titre("2. Meme fenetre de 24 h, resolution variable")
    debut_24h, fin_24h = maintenant - timedelta(hours=24), maintenant
    print(f"  {'limit':>6}  {'intervalle':>12}  {'nuls':>7}  data_quality")
    for limit in (1, 3, 10, 25, 50, 100, 250, 500, 1000):
        mesures = lire(debut_24h, fin_24h, limit)
        intervalle = 86400 / limit
        qualites = dict(Counter(m["data_quality"] for m in mesures))
        print(f"  {limit:>6}  {intervalle:>10.0f} s  {taux_de_nuls(mesures):>6.1f}%  {qualites}")

    titre("3. Resolution fixe (60 s), anciennete de la fenetre variable")
    print(f"  {'fenetre':>22}  {'points':>7}  {'nuls':>7}  data_quality")
    fenetres = [
        ("derniere heure", 0, 1),
        ("6 dernieres heures", 0, 6),
        ("24 dernieres heures", 0, 24),
        ("il y a 24 a 48 h", 24, 48),
        ("il y a 48 a 72 h", 48, 72),
        ("il y a 7 jours", 168, 169),
    ]
    for etiquette, heures_avant_debut, heures_avant_fin in fenetres:
        fin = maintenant - timedelta(hours=heures_avant_debut)
        debut = maintenant - timedelta(hours=heures_avant_fin)
        limit = min(1000, max(1, int((fin - debut).total_seconds() // 60)))
        mesures = lire(debut, fin, limit)
        qualites = dict(Counter(m["data_quality"] for m in mesures))
        print(f"  {etiquette:>22}  {len(mesures):>7}  {taux_de_nuls(mesures):>6.1f}%  {qualites}")

    titre("4. Requete strictement identique, repetee 5 fois")
    print("  Si le taux varie d'un appel a l'autre, le simulateur tire au hasard.")
    for essai in range(1, 6):
        mesures = lire(maintenant - timedelta(hours=2), maintenant, 120)
        proportion = taux_de_nuls(mesures)
        print(f"  essai {essai} : {proportion:>6.1f}% de nuls sur {len(mesures)} mesures")

    titre("5. Echantillon brut : 6 mesures sur les 2 dernieres heures")
    echantillon = lire(maintenant - timedelta(hours=2), maintenant, 6)
    if not echantillon:
        print("  Aucune mesure renvoyee sur cette fenetre.")
    for mesure in echantillon:
        print(f"  {mesure['timestamp']}  kw={mesure['consumption_kw']!s:<8} "
              f"temp={mesure['temperature_celsius']!s:<6} "
              f"quality={mesure['data_quality']:<9} {mesure['null_reasons']}")

    titre("6. Comparaison directe avec /current au meme instant")
    instantane = http.get_json(f"/api/v1/sites/{CIBLE}/current", site_id=CIBLE)
    print(f"  /current   : kw={instantane['consumption_kw']}  "
          f"quality={instantane['data_quality']}  {instantane['null_reasons']}")
    dernieres = lire(maintenant - timedelta(minutes=10), maintenant, 10)
    if not dernieres:
        print("  /readings  : aucune mesure sur les 10 dernieres minutes")
    else:
        derniere = dernieres[-1]
        print(f"  /readings  : kw={derniere['consumption_kw']}  "
              f"quality={derniere['data_quality']}  {derniere['null_reasons']}")
        print("\n  Ces deux lignes decrivent le meme site a la meme minute. Si l'une est")
        print("  saine et l'autre nulle, les deux endpoints ne partagent pas le meme")
        print("  simulateur, et le mode batch ne peut pas servir de reference.")
