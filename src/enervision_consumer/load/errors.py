"""Erreurs de la couche de chargement."""


class PersistenceError(Exception):
    """Une ecriture en base n'a pas abouti.

    L'offset Kafka du message concerne ne doit pas etre acquitte : le message sera
    represente, plutot que perdu sans que personne ne le sache.
    """

    def __init__(self, table: str, reason: str) -> None:
        """Rassemble la table visee et la cause du refus.

        Args:
            table: Table dans laquelle l'ecriture a echoue.
            reason: Description technique rendue par le pilote.
        """
        super().__init__(f"write to table {table!r} failed: {reason}")
        self.table = table
        self.reason = reason


class UnknownSiteReferenceError(PersistenceError):
    """Un fait reference un site absent du referentiel.

    Kafka ne garantit aucun ordre entre topics : le site peut arriver juste apres. Ce
    n'est donc pas une donnee invalide mais une course, et l'offset ne doit pas etre
    acquitte pour que le message soit represente une fois le referentiel rattrape.
    """

    def __init__(self, table: str, site_id: str) -> None:
        """Rassemble la table visee et le site introuvable.

        Args:
            table: Table dans laquelle l'ecriture a echoue.
            site_id: Identifiant du site absent du referentiel.
        """
        super().__init__(table, f"site {site_id!r} is not in the registry yet")
        self.site_id = site_id
