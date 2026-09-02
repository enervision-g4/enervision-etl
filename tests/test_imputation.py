from datetime import datetime, timedelta
from typing import Optional

import pytest

from enervision_etl.contracts.energy_reading import MEASUREMENT_FIELD_NAMES, EnergyReading
from enervision_etl.transform.imputation import (
    ImputationMethod,
    forward_fill_series,
    linear_interpolation_series,
)

SERIES_START = datetime(2026, 9, 2, 10, 0, 0)


def build_series(
    consumptions: list[Optional[float]],
    site_id: str = "SITE002",
    interval_seconds: int = 60,
) -> list[EnergyReading]:
    """Construit une serie ou seul consumption_kw varie, les autres capteurs restant sains."""
    return [
        EnergyReading(
            timestamp=SERIES_START + timedelta(seconds=interval_seconds * index),
            site_id=site_id,
            site_type="factory",
            consumption_kw=consumption,
            consumption_kwh=consumption,
            voltage_v=400.0,
            current_a=800.0,
            power_factor=0.92,
            temperature_celsius=18.0,
            humidity_percent=60.0,
            null_reasons=[] if consumption is not None else ["consumption_sensor_failure"],
            data_quality="good" if consumption is not None else "degraded",
        )
        for index, consumption in enumerate(consumptions)
    ]


def consumptions_of(imputed_series) -> list[Optional[float]]:
    return [reading.consumption_kw for reading in imputed_series]


def methods_of(imputed_series) -> list[str]:
    return [reading.imputation_method for reading in imputed_series]


def test_a_series_without_any_gap_is_reported_as_untouched() -> None:
    series = build_series([875.4, 880.1, 902.1])

    imputed = forward_fill_series(series, max_gap_measures=3)

    assert consumptions_of(imputed) == [875.4, 880.1, 902.1]
    assert methods_of(imputed) == [ImputationMethod.NONE] * 3


def test_a_single_gap_is_filled_with_the_previous_value() -> None:
    series = build_series([875.4, None, 902.1])

    imputed = forward_fill_series(series, max_gap_measures=3)

    assert consumptions_of(imputed) == [875.4, 875.4, 902.1]
    assert methods_of(imputed) == [
        ImputationMethod.NONE,
        ImputationMethod.FORWARD_FILL,
        ImputationMethod.NONE,
    ]


def test_a_gap_of_two_measures_carries_the_same_value_forward() -> None:
    series = build_series([875.4, None, None, 902.1])

    imputed = forward_fill_series(series, max_gap_measures=3)

    assert consumptions_of(imputed) == [875.4, 875.4, 875.4, 902.1]


def test_a_gap_longer_than_the_limit_is_left_untouched() -> None:
    series = build_series([875.4, None, None, None, None, 902.1])

    imputed = forward_fill_series(series, max_gap_measures=3)

    assert consumptions_of(imputed) == [875.4, None, None, None, None, 902.1]
    assert methods_of(imputed) == [ImputationMethod.NONE] * 6


def test_a_gap_at_the_very_beginning_cannot_be_filled() -> None:
    # Aucune valeur precedente n'existe : le trou reste tel quel.
    series = build_series([None, None, 902.1])

    imputed = forward_fill_series(series, max_gap_measures=3)

    assert consumptions_of(imputed) == [None, None, 902.1]
    assert methods_of(imputed) == [ImputationMethod.NONE] * 3


def test_a_gap_at_the_very_end_is_filled() -> None:
    # Le forward fill ne regarde que le passe : la fin de serie ne le gene pas.
    series = build_series([875.4, 880.1, None, None])

    imputed = forward_fill_series(series, max_gap_measures=3)

    assert consumptions_of(imputed) == [875.4, 880.1, 880.1, 880.1]


def test_a_series_entirely_null_stays_entirely_null() -> None:
    series = build_series([None, None, None])

    imputed = forward_fill_series(series, max_gap_measures=3)

    assert consumptions_of(imputed) == [None, None, None]


def test_only_the_failing_sensor_is_imputed(
    partial_reading_payload: dict,
) -> None:
    healthy = EnergyReading.model_validate(partial_reading_payload).model_copy(
        update={"timestamp": SERIES_START, "temperature_celsius": 22.1}
    )
    degraded = EnergyReading.model_validate(partial_reading_payload).model_copy(
        update={"timestamp": SERIES_START + timedelta(seconds=60)}
    )

    imputed = forward_fill_series([healthy, degraded], max_gap_measures=3)

    assert imputed[1].temperature_celsius == 22.1
    assert imputed[1].consumption_kw == 91.20
    assert imputed[1].humidity_percent == 60.2
    assert imputed[1].imputed_fields == ("temperature_celsius",)


def test_each_gap_is_evaluated_independently_per_field() -> None:
    first = build_series([875.4])[0]
    second = build_series([None])[0].model_copy(
        update={
            "timestamp": SERIES_START + timedelta(seconds=60),
            "temperature_celsius": None,
        }
    )
    third = build_series([902.1])[0].model_copy(
        update={"timestamp": SERIES_START + timedelta(seconds=120)}
    )

    imputed = forward_fill_series([first, second, third], max_gap_measures=3)

    assert imputed[1].consumption_kw == 875.4
    assert imputed[1].temperature_celsius == 18.0
    assert set(imputed[1].imputed_fields) == {
        "consumption_kw",
        "consumption_kwh",
        "temperature_celsius",
    }


def test_output_has_exactly_one_row_per_input_row() -> None:
    series = build_series([875.4, None, None, 902.1, None])

    imputed = forward_fill_series(series, max_gap_measures=3)

    assert len(imputed) == len(series)
    assert [row.timestamp for row in imputed] == [row.timestamp for row in series]
    assert {row.site_id for row in imputed} == {"SITE002"}


def test_an_empty_series_yields_an_empty_result() -> None:
    assert forward_fill_series([], max_gap_measures=3) == []


def test_the_source_series_is_never_modified() -> None:
    series = build_series([875.4, None, 902.1])

    forward_fill_series(series, max_gap_measures=3)

    assert series[1].consumption_kw is None


def test_an_unsorted_series_is_rejected() -> None:
    series = build_series([875.4, 880.1, 902.1])
    unsorted_series = [series[2], series[0], series[1]]

    with pytest.raises(ValueError):
        forward_fill_series(unsorted_series, max_gap_measures=3)


def test_a_series_mixing_several_sites_is_rejected() -> None:
    first_site = build_series([875.4, None])
    second_site = build_series([None, 902.1], site_id="SITE003")
    mixed_series = [first_site[0], second_site[1]]

    with pytest.raises(ValueError):
        forward_fill_series(mixed_series, max_gap_measures=3)


@pytest.mark.parametrize("invalid_limit", [0, -1])
def test_a_non_positive_gap_limit_is_rejected(invalid_limit: int) -> None:
    with pytest.raises(ValueError):
        forward_fill_series(build_series([875.4, None]), max_gap_measures=invalid_limit)


def test_every_measurement_field_is_covered_by_the_strategy() -> None:
    series = [
        build_series([875.4])[0],
        build_series([None])[0].model_copy(
            update={
                "timestamp": SERIES_START + timedelta(seconds=60),
                **{field_name: None for field_name in MEASUREMENT_FIELD_NAMES},
            }
        ),
    ]

    imputed = forward_fill_series(series, max_gap_measures=3)

    assert set(imputed[1].imputed_fields) == set(MEASUREMENT_FIELD_NAMES)


def build_series_at(
    offsets_seconds: list[int],
    consumptions: list[Optional[float]],
) -> list[EnergyReading]:
    """Construit une serie aux intervalles volontairement irreguliers."""
    return [
        EnergyReading(
            timestamp=SERIES_START + timedelta(seconds=offset),
            site_id="SITE002",
            site_type="factory",
            consumption_kw=consumption,
            consumption_kwh=consumption,
            voltage_v=400.0,
            current_a=800.0,
            power_factor=0.92,
            temperature_celsius=18.0,
            humidity_percent=60.0,
            null_reasons=[] if consumption is not None else ["consumption_sensor_failure"],
            data_quality="good" if consumption is not None else "degraded",
        )
        for offset, consumption in zip(offsets_seconds, consumptions, strict=True)
    ]


def test_interpolation_leaves_a_complete_series_untouched() -> None:
    series = build_series([875.4, 880.1, 902.1])

    imputed = linear_interpolation_series(series, max_gap_measures=3)

    assert consumptions_of(imputed) == [875.4, 880.1, 902.1]
    assert methods_of(imputed) == [ImputationMethod.NONE] * 3


def test_a_single_gap_lands_midway_between_its_two_anchors() -> None:
    series = build_series([100.0, None, 200.0])

    imputed = linear_interpolation_series(series, max_gap_measures=3)

    assert consumptions_of(imputed) == [100.0, 150.0, 200.0]
    assert methods_of(imputed)[1] == ImputationMethod.LINEAR_INTERPOLATION


def test_a_gap_of_two_measures_is_split_in_thirds() -> None:
    series = build_series([100.0, None, None, 400.0])

    imputed = linear_interpolation_series(series, max_gap_measures=3)

    assert consumptions_of(imputed) == [100.0, 200.0, 300.0, 400.0]


def test_interpolation_is_weighted_by_elapsed_time_not_by_position() -> None:
    # Le trou est a 60 s du point de gauche et a 240 s du point de droite. Une moyenne
    # naive donnerait 150. La ponderation temporelle donne 120.
    series = build_series_at([0, 60, 300], [100.0, None, 200.0])

    imputed = linear_interpolation_series(series, max_gap_measures=3)

    assert imputed[1].consumption_kw == pytest.approx(120.0)


def test_a_gap_at_the_very_end_cannot_be_interpolated() -> None:
    # Contrairement au forward fill, l'interpolation exige un point d'ancrage a droite.
    series = build_series([875.4, 880.1, None, None])

    imputed = linear_interpolation_series(series, max_gap_measures=3)

    assert consumptions_of(imputed) == [875.4, 880.1, None, None]
    assert methods_of(imputed) == [ImputationMethod.NONE] * 4


def test_a_gap_at_the_very_beginning_cannot_be_interpolated() -> None:
    series = build_series([None, None, 902.1])

    imputed = linear_interpolation_series(series, max_gap_measures=3)

    assert consumptions_of(imputed) == [None, None, 902.1]


def test_the_two_strategies_differ_on_a_trailing_gap() -> None:
    series = build_series([875.4, 880.1, None])

    filled_forward = forward_fill_series(series, max_gap_measures=3)
    interpolated = linear_interpolation_series(series, max_gap_measures=3)

    assert filled_forward[2].consumption_kw == 880.1
    assert interpolated[2].consumption_kw is None


def test_interpolation_refuses_a_gap_longer_than_the_limit() -> None:
    series = build_series([100.0, None, None, None, None, 600.0])

    imputed = linear_interpolation_series(series, max_gap_measures=3)

    assert consumptions_of(imputed) == [100.0, None, None, None, None, 600.0]


def test_interpolation_handles_a_decreasing_signal() -> None:
    series = build_series([900.0, None, 700.0])

    imputed = linear_interpolation_series(series, max_gap_measures=3)

    assert imputed[1].consumption_kw == pytest.approx(800.0)


def test_interpolation_applies_field_by_field() -> None:
    first = build_series([100.0])[0]
    second = build_series([None])[0].model_copy(
        update={
            "timestamp": SERIES_START + timedelta(seconds=60),
            "temperature_celsius": None,
        }
    )
    third = build_series([200.0])[0].model_copy(
        update={
            "timestamp": SERIES_START + timedelta(seconds=120),
            "temperature_celsius": 20.0,
        }
    )

    imputed = linear_interpolation_series([first, second, third], max_gap_measures=3)

    assert imputed[1].consumption_kw == pytest.approx(150.0)
    assert imputed[1].temperature_celsius == pytest.approx(19.0)
    assert imputed[1].humidity_percent == 60.0
    assert "humidity_percent" not in imputed[1].imputed_fields


def test_interpolation_output_matches_the_input_shape() -> None:
    series = build_series([100.0, None, 200.0, None, 300.0])

    imputed = linear_interpolation_series(series, max_gap_measures=3)

    assert len(imputed) == len(series)
    assert [row.timestamp for row in imputed] == [row.timestamp for row in series]


def test_interpolation_of_an_empty_series() -> None:
    assert linear_interpolation_series([], max_gap_measures=3) == []


def test_interpolation_rejects_an_unsorted_series() -> None:
    series = build_series([100.0, 200.0, 300.0])

    with pytest.raises(ValueError):
        linear_interpolation_series([series[2], series[0], series[1]], max_gap_measures=3)


def test_duplicated_timestamps_cannot_produce_a_division_by_zero() -> None:
    series = build_series_at([0, 0, 0], [100.0, None, 200.0])

    imputed = linear_interpolation_series(series, max_gap_measures=3)

    assert imputed[1].consumption_kw is None
