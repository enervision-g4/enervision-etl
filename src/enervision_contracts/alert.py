"""Alertes de consommation renvoyees par /api/v1/alerts.

L'identifiant attribue par l'API est conserve tel quel : c'est lui qui rend l'insertion
idempotente cote consumer, en absorbant les remises multiples de Kafka.

Mesure faite sur l'instance : elle fabrique une liste neuve a chaque appel plutot que de
renvoyer des alertes actives durables, deux interrogations espacees de vingt secondes
n'ayant aucune alerte en commun. Cet identifiant n'a donc de stabilite qu'au sein d'un
message, ce qui suffit a son role, mais ne permet pas de reconnaitre deux fois la meme
alerte.
"""

from datetime import datetime
from typing import Final, Optional

from pydantic import BaseModel, ConfigDict

KNOWN_ALERT_SEVERITIES: Final[frozenset[str]] = frozenset(
    {"low", "medium", "high", "critical"}
)
"""Severites documentees. La liste n'est pas fermee cote validation."""

KNOWN_ALERT_TYPES: Final[frozenset[str]] = frozenset(
    {"spike", "threshold", "anomaly", "outage", "sensor"}
)
"""Types documentes. La liste n'est pas fermee cote validation."""


class Alert(BaseModel):
    """Alerte active sur un site, telle que renvoyee par l'API.

    Alimente ALERT. Les champs value et threshold portent des puissances en kW, renommees
    value_kw et threshold_kw au moment de la publication pour epouser les colonnes cibles.

    Attributes:
        alert_id: Identifiant attribue par l'API, en clair et non en UUID.
        timestamp: Horodatage naif, tel que renvoye par l'API.
    """

    # extra="allow" : un champ inconnu ajoute par une future version de l'API est conserve
    # plutot que rejete ou silencieusement perdu, conformement a la regle de non destruction.
    # frozen=True : une alerte est un fait date, aucun code aval ne doit la reecrire.
    model_config = ConfigDict(extra="allow", frozen=True)

    alert_id: str
    timestamp: datetime
    site_id: str
    severity: str
    type: str
    message: str
    value: Optional[float] = None
    threshold: Optional[float] = None

    def has_known_severity(self) -> bool:
        """Indique si la severite fait partie des valeurs connues.

        Permet de signaler une valeur inedite en supervision sans rejeter l'alerte.

        Returns:
            True si severity appartient a KNOWN_ALERT_SEVERITIES.
        """
        return self.severity in KNOWN_ALERT_SEVERITIES
