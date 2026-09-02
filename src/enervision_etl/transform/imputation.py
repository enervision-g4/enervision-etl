"""Reconstruction des valeurs manquantes d'une serie de mesures.

Ce module ne modifie jamais la donnee brute. Il produit une serie parallele, de meme
longueur et de memes horodatages, ou les trous courts sont combles et ou chaque ligne
declare la methode qui lui a ete appliquee.

Le choix de la strategie par type de site est une hypothese de l'equipe, non validee
par la mesure. Voir le commentaire de IMPUTATION_METHOD_BY_SITE_TYPE.

Deux principes structurent le traitement :

Les trous sont evalues champ par champ. Une panne du thermometre ne justifie pas de
recalculer une consommation parfaitement mesuree au meme instant.

Un trou plus long que la limite configuree n'est pas comble. Au dela de quelques
mesures, une valeur reconstruite n'est plus une estimation mais une invention.

Toutes les fonctions sont pures : meme entree, meme sortie, aucun effet de bord.
"""

from collections.abc import Callable, Mapping
from datetime import datetime
from itertools import pairwise
from typing import Final, Optional

from ..contracts.energy_reading import MEASUREMENT_FIELD_NAMES, EnergyReading
from ..contracts.imputed_reading import ImputationMethod, ImputedReading

DEFAULT_IMPUTATION_METHOD: Final[ImputationMethod] = ImputationMethod.LINEAR_INTERPOLATION
"""Strategie retenue pour tout type de site absent de la table ci dessous."""

# Hypothese retenue par l'equipe, et non consigne du cahier des charges : un site a
# consommation reputee stable, datacenter ou hopital, gagnerait moins a l'interpolation
# qu'a la recopie de la derniere valeur connue.
#
# Cette hypothese n'est pas confirmee par la mesure. Le protocole et les resultats sont
# reproductibles avec scripts/bilan_strategies.py : sur l'instance mock, l'interpolation
# faisait aussi bien ou mieux y compris sur SITE005, l'hopital. Deux limites empechent
# de conclure dans un sens ou dans l'autre. Les series generees se comportent en marche
# aleatoire plutot qu'en profil de charge, la notion de site stable n'y a donc pas de
# realite. Et les pannes du simulateur sont si etendues que le parc entier n'a fourni
# que douze trous exploitables, echantillon trop faible pour departager.
#
# La table est donc conservee comme hypothese de travail, exposee en donnee modifiable
# plutot que codee en dur, et surchargeable par site via le parametre overrides. Le
# critere reellement determinant est ailleurs : sans connaissance de la mesure suivante,
# seule la recopie est applicable. Voir impute_series et son parametre lookahead_available.
IMPUTATION_METHOD_BY_SITE_TYPE: Final[dict[str, ImputationMethod]] = {
    "datacenter": ImputationMethod.FORWARD_FILL,
    "hospital": ImputationMethod.FORWARD_FILL,
    "office": ImputationMethod.LINEAR_INTERPOLATION,
    "factory": ImputationMethod.LINEAR_INTERPOLATION,
    "retail": ImputationMethod.LINEAR_INTERPOLATION,
}


def method_for_site_type(
    site_type: Optional[str],
    overrides: Optional[Mapping[str, ImputationMethod]] = None,
) -> ImputationMethod:
    """Determine la strategie de reconstruction adaptee a un type de site.

    Args:
        site_type: Type de site issu du referentiel, insensible a la casse et aux
            espaces superflus. Un type inconnu ou absent prend la strategie par defaut.
        overrides: Table prioritaire sur la correspondance documentee, permettant
            d'ajuster un site sans modifier le code.

    Returns:
        La strategie a appliquer aux series de ce type de site.
    """
    if site_type is None:
        return DEFAULT_IMPUTATION_METHOD

    normalized_site_type = site_type.strip().lower()
    if overrides and normalized_site_type in overrides:
        return overrides[normalized_site_type]
    return IMPUTATION_METHOD_BY_SITE_TYPE.get(normalized_site_type, DEFAULT_IMPUTATION_METHOD)


def impute_series(
    readings: list[EnergyReading],
    site_type: Optional[str],
    max_gap_measures: int,
    overrides: Optional[Mapping[str, ImputationMethod]] = None,
    lookahead_available: bool = True,
) -> list[ImputedReading]:
    """Reconstruit une serie en choisissant la strategie adaptee au site.

    Point d'entree du module. L'appelant fournit le type de site plutot que la
    strategie, ce qui evite de disperser la regle metier dans le code du collecteur.

    L'interpolation exige de connaitre la mesure suivante. Un collecteur temps reel
    ne la connait pas au moment ou il traite la mesure courante : passer
    lookahead_available a False replie alors la strategie sur le forward fill, qui
    ne consulte que le passe, au lieu de renoncer a toute reconstruction.

    Args:
        readings: Releves d'un meme site, tries par horodatage croissant.
        site_type: Type de site issu du referentiel.
        max_gap_measures: Longueur maximale d'un trou encore comblable.
        overrides: Table prioritaire sur la correspondance documentee.
        lookahead_available: Faux lorsque la mesure suivante est inconnue.

    Returns:
        Une serie reconstruite de meme longueur, aux memes horodatages.

    Raises:
        ValueError: Si les invariants de la serie ne sont pas respectes.
    """
    method = method_for_site_type(site_type, overrides)
    if not lookahead_available and method is ImputationMethod.LINEAR_INTERPOLATION:
        method = ImputationMethod.FORWARD_FILL
    return _impute_series(readings, max_gap_measures, method)


def forward_fill_series(
    readings: list[EnergyReading],
    max_gap_measures: int,
) -> list[ImputedReading]:
    """Comble les trous en recopiant la derniere valeur connue.

    Strategie adaptee aux sites a consommation stable, datacenter ou hopital, ou la
    valeur precedente reste une bonne approximation. Elle ne consulte que le passe,
    ce qui la rend applicable en flux temps reel sans attendre la mesure suivante.

    Un trou situe en tout debut de serie n'a aucune valeur anterieure a recopier :
    il reste tel quel.

    Args:
        readings: Releves d'un meme site, tries par horodatage croissant.
        max_gap_measures: Longueur maximale, en nombre de mesures consecutives,
            d'un trou encore considere comme comblable.

    Returns:
        Une serie reconstruite de meme longueur, aux memes horodatages.

    Raises:
        ValueError: Si max_gap_measures n'est pas strictement positif, si la serie
            melange plusieurs sites, ou si elle n'est pas triee chronologiquement.
    """
    return _impute_series(readings, max_gap_measures, ImputationMethod.FORWARD_FILL)


def linear_interpolation_series(
    readings: list[EnergyReading],
    max_gap_measures: int,
) -> list[ImputedReading]:
    """Comble les trous en tracant une droite entre les deux mesures qui les encadrent.

    La position sur cette droite est ponderee par le temps reellement ecoule, et non
    par le rang de la mesure. Un collecteur temps reel ne tient jamais exactement sa
    periode, et supposer des intervalles egaux introduirait un biais.

    Contrairement au forward fill, cette strategie a besoin d'un point d'ancrage des
    deux cotes. Un trou situe en debut ou en fin de serie reste donc tel quel.

    Args:
        readings: Releves d'un meme site, tries par horodatage croissant.
        max_gap_measures: Longueur maximale, en nombre de mesures consecutives,
            d'un trou encore considere comme comblable.

    Returns:
        Une serie reconstruite de meme longueur, aux memes horodatages.

    Raises:
        ValueError: Si max_gap_measures n'est pas strictement positif, si la serie
            melange plusieurs sites, ou si elle n'est pas triee chronologiquement.
    """
    return _impute_series(readings, max_gap_measures, ImputationMethod.LINEAR_INTERPOLATION)


def _impute_series(
    readings: list[EnergyReading],
    max_gap_measures: int,
    method: ImputationMethod,
) -> list[ImputedReading]:
    """Applique une strategie de reconstruction a chaque champ de mesure.

    Args:
        readings: Releves d'un meme site, tries par horodatage croissant.
        max_gap_measures: Longueur maximale d'un trou comblable.
        method: Strategie a appliquer et a declarer sur les lignes reconstruites.

    Returns:
        Une serie reconstruite de meme longueur, aux memes horodatages.

    Raises:
        ValueError: Si les invariants d'entree ne sont pas respectes.
    """
    _validate_series(readings, max_gap_measures)
    if not readings:
        return []

    fill_gap = _GAP_FILLERS[method]
    timestamps = [reading.timestamp for reading in readings]
    reconstructed_values: dict[str, list[Optional[float]]] = {}
    imputed_field_names: list[list[str]] = [[] for _ in readings]

    for field_name in MEASUREMENT_FIELD_NAMES:
        measured_values = [getattr(reading, field_name) for reading in readings]
        filled_values = list(measured_values)

        for gap_start, gap_end in _gap_ranges(measured_values):
            if gap_end - gap_start > max_gap_measures:
                continue
            for position, value in fill_gap(
                measured_values, timestamps, gap_start, gap_end
            ).items():
                filled_values[position] = value
                imputed_field_names[position].append(field_name)

        reconstructed_values[field_name] = filled_values

    return [
        ImputedReading(
            site_id=reading.site_id,
            timestamp=reading.timestamp,
            imputation_method=method if imputed_field_names[position] else ImputationMethod.NONE,
            imputed_fields=tuple(imputed_field_names[position]),
            **{
                field_name: reconstructed_values[field_name][position]
                for field_name in MEASUREMENT_FIELD_NAMES
            },
        )
        for position, reading in enumerate(readings)
    ]


def _fill_gap_by_forward_fill(
    measured_values: list[Optional[float]],
    timestamps: list[datetime],
    gap_start: int,
    gap_end: int,
) -> dict[int, float]:
    """Recopie la derniere valeur connue sur toute la longueur du trou.

    Args:
        measured_values: Valeurs mesurees du champ, dans l'ordre chronologique.
        timestamps: Horodatages correspondants, inutilises par cette strategie.
        gap_start: Position de la premiere valeur absente.
        gap_end: Position suivant la derniere valeur absente.

    Returns:
        Les valeurs a ecrire, indexees par position. Un dictionnaire vide signale
        un trou non comblable, faute de valeur anterieure.
    """
    if gap_start == 0:
        return {}

    anchor_value = measured_values[gap_start - 1]
    if anchor_value is None:
        return {}

    return dict.fromkeys(range(gap_start, gap_end), anchor_value)


def _fill_gap_by_linear_interpolation(
    measured_values: list[Optional[float]],
    timestamps: list[datetime],
    gap_start: int,
    gap_end: int,
) -> dict[int, float]:
    """Repartit les valeurs sur la droite reliant les deux mesures encadrant le trou.

    Args:
        measured_values: Valeurs mesurees du champ, dans l'ordre chronologique.
        timestamps: Horodatages correspondants, servant a la ponderation.
        gap_start: Position de la premiere valeur absente.
        gap_end: Position suivant la derniere valeur absente.

    Returns:
        Les valeurs a ecrire, indexees par position. Un dictionnaire vide signale
        un trou non comblable, faute d'ancrage des deux cotes ou faute de duree
        mesurable entre eux.
    """
    if gap_start == 0 or gap_end >= len(measured_values):
        return {}

    value_before = measured_values[gap_start - 1]
    value_after = measured_values[gap_end]
    if value_before is None or value_after is None:
        return {}

    time_before = timestamps[gap_start - 1]
    elapsed_span = (timestamps[gap_end] - time_before).total_seconds()
    if elapsed_span <= 0:
        return {}

    return {
        position: value_before
        + (value_after - value_before)
        * ((timestamps[position] - time_before).total_seconds() / elapsed_span)
        for position in range(gap_start, gap_end)
    }


GapFiller = Callable[[list[Optional[float]], list[datetime], int, int], dict[int, float]]
"""Signature commune des strategies de comblement d'un trou."""

_GAP_FILLERS: dict[ImputationMethod, GapFiller] = {
    ImputationMethod.FORWARD_FILL: _fill_gap_by_forward_fill,
    ImputationMethod.LINEAR_INTERPOLATION: _fill_gap_by_linear_interpolation,
}


def _gap_ranges(measured_values: list[Optional[float]]) -> list[tuple[int, int]]:
    """Localise les suites de valeurs absentes dans une colonne de mesures.

    Args:
        measured_values: Valeurs d'un champ, dans l'ordre chronologique.

    Returns:
        Les bornes de chaque trou sous forme (debut inclus, fin exclue).
    """
    gaps: list[tuple[int, int]] = []
    gap_start: Optional[int] = None

    for position, value in enumerate(measured_values):
        if value is None and gap_start is None:
            gap_start = position
        elif value is not None and gap_start is not None:
            gaps.append((gap_start, position))
            gap_start = None

    if gap_start is not None:
        gaps.append((gap_start, len(measured_values)))
    return gaps


def _validate_series(readings: list[EnergyReading], max_gap_measures: int) -> None:
    """Verifie les invariants attendus d'une serie avant traitement.

    Une serie melangeant plusieurs sites ou mal ordonnee produirait des valeurs
    reconstruites silencieusement fausses. L'echec explicite est preferable.

    Args:
        readings: Releves a controler.
        max_gap_measures: Longueur maximale d'un trou comblable.

    Raises:
        ValueError: Si un invariant est viole.
    """
    if max_gap_measures <= 0:
        raise ValueError(
            f"max_gap_measures must be strictly positive, received {max_gap_measures}"
        )

    site_identifiers = {reading.site_id for reading in readings}
    if len(site_identifiers) > 1:
        raise ValueError(
            f"a series must belong to a single site, received {sorted(site_identifiers)}"
        )

    for earlier, later in pairwise(readings):
        if earlier.timestamp > later.timestamp:
            raise ValueError(
                "readings must be sorted by ascending timestamp, "
                f"{later.timestamp.isoformat()} follows {earlier.timestamp.isoformat()}"
            )
