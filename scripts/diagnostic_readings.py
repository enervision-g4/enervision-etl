"""Diagnostic cible de l'endpoint /readings et du fuseau horaire.

La sonde a revele une incoherence : 99,8 pour cent de valeurs nulles sur /readings
alors que /current est majoritairement sain. Ce script interroge l'API en appels
bruts, sans pagination, pour determiner le comportement reel de l'endpoint.
"""

from collections import Counter
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from typing import Any

from enervision_etl.config import load_settings
from enervision_etl.contracts.energy_reading import MEASUREMENT_FIELD_NAMES
from enervision_etl.extract.http_client import ResilientHttpClient

CIBLE = "SITE002"


def titre(texte: str) -> None:
    print(f"\n{'=' * 74}\n{texte}\n{'=' * 74}")


def horodatages(mesures: list[dict[str, Any]]) -> list[datetime]:
    return [datetime.fromisoformat(mesure["timestamp"]) for mesure in mesures]


settings = load_settings()
print(f"Cible : {settings.api_mock_base_url}  site : {CIBLE}")

with ResilientHttpClient(settings.api_mock_base_url, 10.0) as http:

    titre("A. Un appel brut, limit=3, sans fenetre temporelle")
    trois_mesures = http.get_json("/api/v1/readings", {"site_id": CIBLE, "limit": 3})
    print(f"  Type renvoye : {type(trois_mesures).__name__}, {len(trois_mesures)} elements")
    for mesure in trois_mesures:
        print(f"    {mesure['timestamp']}  consumption_kw={mesure['consumption_kw']}  "
              f"quality={mesure['data_quality']}  null_reasons={mesure['null_reasons']}")

    titre("B. Deux appels identiques : la serie est-elle stable ou regeneree ?")
    premier = http.get_json("/api/v1/readings", {"site_id": CIBLE, "limit": 5})
    second = http.get_json("/api/v1/readings", {"site_id": CIBLE, "limit": 5})
    ts_premier = [m["timestamp"] for m in premier]
    ts_second = [m["timestamp"] for m in second]
    print(f"  Appel 1 : {ts_premier[0]} ... {ts_premier[-1]}")
    print(f"  Appel 2 : {ts_second[0]} ... {ts_second[-1]}")
    print(f"  Horodatages identiques : {ts_premier == ts_second}")
    if ts_premier != ts_second:
        print("  -> L'API REGENERE la serie a chaque appel. Toute pagination par")
        print("     decalage de start_time est impossible : elle empilerait des")
        print("     series differentes au lieu de parcourir une serie unique.")

    titre("C. La fenetre start_time / end_time est-elle respectee ?")
    fin = datetime.now()
    debut = fin - timedelta(hours=2)
    dans_fenetre = http.get_json(
        "/api/v1/readings",
        {"site_id": CIBLE, "start_time": debut.isoformat(),
         "end_time": fin.isoformat(), "limit": 10},
    )
    bornes = horodatages(dans_fenetre)
    print(f"  Demande : {debut.isoformat()}  ->  {fin.isoformat()}")
    print(f"  Recu    : {bornes[0].isoformat()}  ->  {bornes[-1].isoformat()}")
    respectee = all(debut <= instant <= fin for instant in bornes)
    print(f"  Toutes les mesures sont dans la fenetre demandee : {respectee}")
    couverture = (bornes[-1] - bornes[0]).total_seconds() / (fin - debut).total_seconds()
    print(f"  La serie couvre {couverture * 100:.1f} % de la fenetre demandee")

    titre("D. Pas reel entre mesures, en un seul appel non pagine")
    cent_mesures = http.get_json("/api/v1/readings", {"site_id": CIBLE, "limit": 100})
    instants = horodatages(cent_mesures)
    ecarts = [(b - a).total_seconds() for a, b in pairwise(instants)]
    print(f"  {len(cent_mesures)} mesures recues")
    print(f"  Ecart minimal : {min(ecarts):.3f} s")
    print(f"  Ecart maximal : {max(ecarts):.3f} s")
    print(f"  Ecart median  : {sorted(ecarts)[len(ecarts) // 2]:.3f} s")
    print(f"  Strictement croissant : {all(e > 0 for e in ecarts)}")

    titre("E. Taux de valeurs nulles par champ, sur ce meme appel unique")
    for champ in MEASUREMENT_FIELD_NAMES:
        nuls = sum(1 for mesure in cent_mesures if mesure[champ] is None)
        proportion = 100 * nuls / len(cent_mesures)
        print(f"  {champ:<22} {nuls:>3}/{len(cent_mesures)}  ({proportion:5.1f} %)")
    qualites = Counter(mesure["data_quality"] for mesure in cent_mesures)
    causes = Counter(cause for mesure in cent_mesures for cause in mesure["null_reasons"])
    print(f"\n  data_quality : {dict(qualites)}")
    print(f"  null_reasons : {dict(causes) or 'aucun'}")

    titre("F. Longueur des trous consecutifs sur consumption_kw")
    longueurs: list[int] = []
    courant = 0
    for mesure in cent_mesures:
        if mesure["consumption_kw"] is None:
            courant += 1
        elif courant:
            longueurs.append(courant)
            courant = 0
    if courant:
        longueurs.append(courant)
    print(f"  Nombre de trous : {len(longueurs)}")
    print(f"  Longueurs observees : {dict(Counter(longueurs)) or 'aucun trou'}")
    if longueurs:
        imputables = sum(1 for taille in longueurs if taille <= 3)
        print(f"  Trous de 3 mesures ou moins (donc imputables) : {imputables}/{len(longueurs)}")

    titre("G. Fuseau horaire : /current compare a l'heure de VOTRE machine")
    instantane = http.get_json(f"/api/v1/sites/{CIBLE}/current", site_id=CIBLE)
    horodatage_api = datetime.fromisoformat(instantane["timestamp"])
    maintenant_local = datetime.now()
    maintenant_utc = datetime.now(UTC).replace(tzinfo=None)
    ecart_local = (maintenant_local - horodatage_api).total_seconds()
    ecart_utc = (maintenant_utc - horodatage_api).total_seconds()
    print(f"  Horodatage renvoye par l'API : {horodatage_api.isoformat()}")
    print(f"  Heure locale de la machine   : {maintenant_local.isoformat()}")
    print(f"  Heure UTC                    : {maintenant_utc.isoformat()}")
    print(f"\n  Ecart avec l'heure locale : {ecart_local:>9.1f} s")
    print(f"  Ecart avec l'heure UTC    : {ecart_utc:>9.1f} s")
    if abs(ecart_local) < abs(ecart_utc):
        print("\n  -> L'API emet en HEURE LOCALE. Il faudra convertir vers UTC avant Kafka.")
    else:
        print("\n  -> L'API emet en UTC. API_MOCK_SOURCE_TIMEZONE=UTC est le bon reglage.")
