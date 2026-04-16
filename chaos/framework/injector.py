import functools
from typing import Any, Protocol


class Trigger(Protocol):
    def should_fire(self, call_count: int, bytes_written: int) -> bool: ...


class Fault(Protocol):
    def execute(self) -> None: ...


class FaultInjector:
    def __init__(self, method: str, trigger: Trigger, fault: Fault):
        self.method = method
        self.trigger = trigger
        self.fault = fault
        self._call_count = 0
        self._bytes_written = 0

    def wrap_backend(self, backend: Any) -> Any:
        """Return a proxy object that intercepts self.method and fires fault on trigger."""
        injector = self

        class Proxy:
            def __getattr__(self, name: str):
                original = getattr(backend, name)
                if name != injector.method:
                    return original

                @functools.wraps(original)
                def intercepted(*args, **kwargs):
                    injector._call_count += 1
                    if injector.trigger.should_fire(injector._call_count, injector._bytes_written):
                        injector.fault.execute()
                    return original(*args, **kwargs)

                return intercepted

        proxy = Proxy()
        # Copy non-callable attributes (e.g. _objects, _parts paths used by sweep/tests)
        for attr in vars(backend):
            try:
                object.__setattr__(proxy, attr, getattr(backend, attr))
            except AttributeError:
                pass

        return proxy
