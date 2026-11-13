"""Optional, no-op-by-default hooks used to make interleavings deterministic.

The migration engine and the business write path expose a handful of named
checkpoints.  Nothing is registered in production, so ``trigger`` is a cheap
dict lookup.  Tests register callbacks (usually blocking on an
``threading.Event``) to pause a worker exactly between two steps and force the
races these guards are meant to prevent:

* ``cutover_state_flipped``    - state is ``cutover``; old DB lock not yet held
* ``cutover_old_db_locked``    - old DB write lock held; final drain not started
* ``cutover_before_completed`` - drain + completeness check passed; the flip
                                 to ``completed`` not yet committed
* ``cutover_completed``        - completion committed; old lock not released
* ``write_before_routing``     - a report write has been accepted, before it
                                 takes the old-DB lock and re-checks the state
* ``write_holds_old_lock``     - old-DB write lock held and state checked as
                                 ``running``; the wide row is not written yet
* ``write_old_committed``      - the wide row is committed; the request has
                                 not returned and no long-table write follows
"""
from __future__ import annotations

import threading
from typing import Callable

_lock = threading.Lock()
_hooks: dict[str, Callable[..., None]] = {}


def set_hook(name: str, callback: Callable[..., None] | None) -> None:
    with _lock:
        if callback is None:
            _hooks.pop(name, None)
        else:
            _hooks[name] = callback


def clear_hooks() -> None:
    with _lock:
        _hooks.clear()


def trigger(name: str, **kwargs) -> None:
    with _lock:
        callback = _hooks.get(name)
    if callback is not None:
        callback(**kwargs)
