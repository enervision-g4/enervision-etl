"""La periode demandee doit tenir en une seule requete par defaut.

L'instance mock se degrade sous une rafale de requetes. Comme limit fixe le nombre de
points repartis dans la fenetre, une seule requete suffit a couvrir n'importe quelle
periode : c'est la resolution qui s'ajuste, pas le nombre d'appels.
"""

import pytest

from enervision_etl.cli import MAX_POINTS_PER_REQUEST, resolve_sampling


def test_a_window_is_covered_by_a_single_request_by_default() -> None:
    resolution = resolve_sampling(hours=24.0, points=MAX_POINTS_PER_REQUEST, resolution=None)

    assert resolution == pytest.approx(24 * 3600 / MAX_POINTS_PER_REQUEST)


def test_asking_fewer_points_widens_the_spacing() -> None:
    resolution = resolve_sampling(hours=24.0, points=100, resolution=None)

    assert resolution == pytest.approx(24 * 3600 / 100)


def test_an_explicit_resolution_takes_precedence() -> None:
    assert resolve_sampling(hours=24.0, points=1000, resolution=60.0) == 60.0


def test_a_short_window_keeps_a_fine_resolution() -> None:
    resolution = resolve_sampling(hours=1.0, points=MAX_POINTS_PER_REQUEST, resolution=None)

    assert resolution == pytest.approx(3.6)


@pytest.mark.parametrize("invalid_points", [0, -1, MAX_POINTS_PER_REQUEST + 1])
def test_an_impossible_number_of_points_is_rejected(invalid_points: int) -> None:
    # Au dela du plafond documente, l'API repond 422 : autant echouer avant l'appel.
    with pytest.raises(ValueError):
        resolve_sampling(hours=24.0, points=invalid_points, resolution=None)


@pytest.mark.parametrize("invalid_resolution", [0.0, -1.0])
def test_a_non_positive_resolution_is_rejected(invalid_resolution: float) -> None:
    with pytest.raises(ValueError):
        resolve_sampling(hours=24.0, points=1000, resolution=invalid_resolution)


@pytest.mark.parametrize("invalid_hours", [0.0, -1.0])
def test_a_non_positive_period_is_rejected(invalid_hours: float) -> None:
    with pytest.raises(ValueError):
        resolve_sampling(hours=invalid_hours, points=1000, resolution=None)
