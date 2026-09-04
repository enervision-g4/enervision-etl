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
