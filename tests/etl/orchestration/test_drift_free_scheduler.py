import pytest

from enervision_etl.orchestration.drift_free_scheduler import DriftFreeScheduler


class ControlledClock:
    """Horloge et sommeil pilotes par le test, pour observer la derive sans attendre."""

    def __init__(self) -> None:
        self.elapsed = 0.0
        self.sleep_durations: list[float] = []

    def monotonic(self) -> float:
        return self.elapsed

    def sleep(self, duration: float) -> None:
        self.sleep_durations.append(duration)
        self.elapsed += duration

    def spend(self, duration: float) -> None:
        """Simule un cycle de traitement qui consomme du temps."""
        self.elapsed += duration


@pytest.fixture
def clock() -> ControlledClock:
    return ControlledClock()


@pytest.fixture
def scheduler(clock: ControlledClock) -> DriftFreeScheduler:
    return DriftFreeScheduler(interval_seconds=60.0, monotonic=clock.monotonic, sleep=clock.sleep)


def test_the_first_tick_happens_immediately(
    scheduler: DriftFreeScheduler,
    clock: ControlledClock,
) -> None:
    # Un collecteur doit produire des la premiere seconde, pas apres une periode d'attente.
    tick = scheduler.wait_for_next_tick()

    assert tick.index == 0
    assert clock.sleep_durations == []


def test_a_short_cycle_is_compensated_by_a_longer_wait(
    scheduler: DriftFreeScheduler,
    clock: ControlledClock,
) -> None:
    scheduler.wait_for_next_tick()
    clock.spend(4.0)

    scheduler.wait_for_next_tick()

    assert clock.sleep_durations == [56.0]


def test_a_long_cycle_is_compensated_by_a_shorter_wait(
    scheduler: DriftFreeScheduler,
    clock: ControlledClock,
) -> None:
    # C'est tout le defaut du sleep fixe : il ajoute la periode APRES le traitement,
    # et le cycle reel dure alors periode plus duree de traitement.
    scheduler.wait_for_next_tick()
    clock.spend(25.0)

    scheduler.wait_for_next_tick()

    assert clock.sleep_durations == [35.0]


def test_ticks_stay_aligned_whatever_the_cycle_durations(
    scheduler: DriftFreeScheduler,
    clock: ControlledClock,
) -> None:
    scheduler.wait_for_next_tick()
    observed_instants = [clock.elapsed]

    for cycle_duration in (5.0, 42.0, 1.0, 30.0):
        clock.spend(cycle_duration)
        scheduler.wait_for_next_tick()
        observed_instants.append(clock.elapsed)

    assert observed_instants == [0.0, 60.0, 120.0, 180.0, 240.0]


def test_an_overrunning_cycle_skips_the_missed_ticks(
    scheduler: DriftFreeScheduler,
    clock: ControlledClock,
) -> None:
    # Ticks a 0, 60, 120, 180. Un cycle de 150 s fait manquer celui de 60 : on repart
    # immediatement au tick 2 plutot que d'attendre 180, un collecteur en retard devant
    # reprendre au plus tot. Empiler les ticks manques le ferait tourner en continu.
    scheduler.wait_for_next_tick()
    clock.spend(150.0)

    tick = scheduler.wait_for_next_tick()

    assert tick.skipped_ticks == 1
    assert tick.index == 2
    assert clock.elapsed == 150.0
    assert clock.sleep_durations == []


def test_a_cycle_within_the_period_skips_nothing(
    scheduler: DriftFreeScheduler,
    clock: ControlledClock,
) -> None:
    scheduler.wait_for_next_tick()
    clock.spend(30.0)

    assert scheduler.wait_for_next_tick().skipped_ticks == 0


def test_the_lateness_of_a_tick_is_reported(
    scheduler: DriftFreeScheduler,
    clock: ControlledClock,
) -> None:
    scheduler.wait_for_next_tick()
    clock.spend(150.0)

    assert scheduler.wait_for_next_tick().lateness_seconds == pytest.approx(30.0)


def test_an_on_time_tick_reports_no_lateness(
    scheduler: DriftFreeScheduler,
    clock: ControlledClock,
) -> None:
    scheduler.wait_for_next_tick()
    clock.spend(10.0)

    assert scheduler.wait_for_next_tick().lateness_seconds == 0.0


def test_a_cycle_lasting_exactly_one_period_skips_nothing(
    scheduler: DriftFreeScheduler,
    clock: ControlledClock,
) -> None:
    scheduler.wait_for_next_tick()
    clock.spend(60.0)

    tick = scheduler.wait_for_next_tick()

    assert tick.index == 1
    assert tick.skipped_ticks == 0
    assert clock.sleep_durations == []


@pytest.mark.parametrize("invalid_interval", [0, -1])
def test_a_non_positive_interval_is_rejected(invalid_interval: float) -> None:
    with pytest.raises(ValueError):
        DriftFreeScheduler(interval_seconds=invalid_interval)
