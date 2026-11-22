"""Process registry and lock specification utilities.

This module implements a lightweight, file‑backed registry that reads the
``config/processes.json`` configuration file during initialisation and exposes
``ProcessRegistry.get_required_locks`` – a pure‑Python method that calculates the
set of composite lock keys needed to execute a given process for a particular
``target_date``.

The registry also keeps the configuration in memory and can be refreshed on
the fly if the underlying file is modified.  This is sufficient for the unit
tests used in the kata and mirrors the behaviour described in the
architecture document (section 4.2).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import json
import os
from pathlib import Path
from typing import Dict, Iterable, List, Optional

__all__ = ["ResourceLockSpec", "ProcessRegistry"]


@dataclass(frozen=True)
class ResourceLockSpec:
    """Specification of a composite lock.

    Attributes
    ----------
    resource_id: str
        Identifier of the resource – e.g. ``src_transactions``.
    date_from: date
        Inclusive start of the date window required by the process.
    date_to: date
        Inclusive end of the date window.
    access_mode: str
        Either ``"READ"`` or ``"WRITE"``.
    """

    resource_id: str
    date_from: date
    date_to: date
    access_mode: str

    def __repr__(self) -> str:  # pragma: no cover – trivial
        return (
            f"ResourceLockSpec(resource_id={self.resource_id!r}, "
            f"date_from={self.date_from!r}, date_to={self.date_to!r}, "
            f"access_mode={self.access_mode!r})"
        )


class ProcessRegistry:
    """Load ``processes.json`` and compute required lock sets.

    The configuration is loaded lazily during the first call to
    :py:meth:`get_required_locks`.  Subsequent calls reuse the cached
    configuration unless the file has been modified – the registry will reload
    automatically.
    """

    _config_path: Path
    _last_mtime: Optional[float]
    _config: Optional[Dict]

    def __init__(self, config_path: str | Path = "config/processes.json") -> None:
        self._config_path = Path(config_path)
        self._last_mtime = None
        self._config = None

    # ---------------------------------------------------------------------
    # internal helpers
    # ---------------------------------------------------------------------
    def _load_config(self) -> None:
        """Load JSON configuration from disk.

        The method records the file modification time so that subsequent
        calls can decide whether a reload is necessary.
        """

        if not self._config_path.is_file():
            raise FileNotFoundError(self._config_path)
        mtime = self._config_path.stat().st_mtime
        if self._last_mtime is None or mtime != self._last_mtime:
            with self._config_path.open("r", encoding="utf-8") as fp:
                self._config = json.load(fp)
            self._last_mtime = mtime

    # ---------------------------------------------------------------------
    # public API
    # ---------------------------------------------------------------------
    def get_required_locks(
        self, process_id: str, target_date: date
    ) -> List[ResourceLockSpec]:
        """Return a list of :class:`ResourceLockSpec` for ``process_id``.

        Parameters
        ----------
        process_id:
            Identifier of the process as defined in ``processes.json``.
        target_date:
            The logical target date of the run – interpreted as the *last*
            day of the sliding window.

        Returns
        -------
        list[ResourceLockSpec]
            One element per resource described by the process definition.
        """

        # Load config lazily / reload if needed.
        self._load_config()
        if self._config is None:
            raise RuntimeError("Registry configuration could not be loaded")

        processes = self._config.get("processes", {})
        proc_cfg = processes.get(process_id)
        if not proc_cfg:
            raise KeyError(f"Unknown process_id: {process_id}")
        window_days: int = int(proc_cfg.get("window_days", 0))
        if window_days <= 0:
            raise ValueError(
                f"Process {process_id!r} has invalid window_days: {window_days}"
            )
        # The architecture spec allows either ``target_date - window_days`` or
        # ``target_date - (window_days-1)``.  We use the latter – it matches the
        # typical interpretation where the window is inclusive of the target
        # date.  This behaviour is exercised by the tests in the kata.
        date_from = target_date - timedelta(days=window_days - 1)
        date_to = target_date

        resources = proc_cfg.get("resources", [])
        locks: List[ResourceLockSpec] = []
        for r in resources:
            lock = ResourceLockSpec(
                resource_id=r["resource_id"],
                date_from=date_from,
                date_to=date_to,
                access_mode=r["mode"],
            )
            locks.append(lock)
        return locks
