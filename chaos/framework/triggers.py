import random


class CallCountTrigger:
    """Fire on the Nth call (1-indexed)."""

    def __init__(self, fire_on: int):
        self._fire_on = fire_on

    def should_fire(self, call_count: int, bytes_written: int) -> bool:
        return call_count == self._fire_on


class ByteOffsetTrigger:
    """Fire when bytes_written crosses the given offset."""

    def __init__(self, offset: int):
        self._offset = offset
        self._fired = False

    def should_fire(self, call_count: int, bytes_written: int) -> bool:
        if not self._fired and bytes_written >= self._offset:
            self._fired = True
            return True
        return False


class RandomTrigger:
    """Fire with the given probability on each call."""

    def __init__(self, probability: float):
        if not 0.0 <= probability <= 1.0:
            raise ValueError("probability must be in [0.0, 1.0]")
        self._probability = probability

    def should_fire(self, call_count: int, bytes_written: int) -> bool:
        return random.random() < self._probability
