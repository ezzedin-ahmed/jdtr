from __future__ import annotations

import contextlib
import json
import logging
from typing import Any, Self, final
from uuid import uuid4

from pydantic import BaseModel
from rocksdict import DbClosedError, Rdict

logger = logging.getLogger(__name__)

# Column family names
RUN_INPUT_CF = "runinput"
RUN_PROGRESS_CF = "runprogress"
RUN_WORKFLOW_CF = "runworkflow"
RUN_ATTEMPTS_CF = "runattempts"
STEP_OUTPUT_CF = "stepoutput"

# Progress states
RUN_PROGRESS_FAILED = -3
RUN_PROGRESS_FINISHED = -2
RUN_PROGRESS_NOT_STARTED = -1


def _json_default(obj: Any) -> Any:
    """JSON encoder hook that serializes pydantic models."""
    if isinstance(obj, BaseModel):
        return obj.model_dump(mode="json")
    raise TypeError(
        f"Object of type {type(obj).__name__} is not JSON serializable. "
        f"Workflow step outputs must be JSON-serializable (or pydantic models)."
    )


@final
class Run:
    """
    Represents a single execution of a workflow.

    Tracks input, progress, and outputs for each step.
    Persists state to database for resumability.

    A run is associated with a ``workflow_id`` so that a given workflow only
    ever resumes its own runs when multiple workflows share a database.
    """

    def __init__(self, run_id: str, db: Database) -> None:
        """
        Initialize a run instance.

        Args:
            run_id: Unique identifier for this run
            db: Database instance for persistence
        """
        self._id = run_id
        self._db = db
        self._input_key = Key(column_family=RUN_INPUT_CF, record_id=run_id)
        self._progress_key = Key(column_family=RUN_PROGRESS_CF, record_id=run_id)
        self._workflow_key = Key(column_family=RUN_WORKFLOW_CF, record_id=run_id)
        self._attempts_key = Key(column_family=RUN_ATTEMPTS_CF, record_id=run_id)

    @classmethod
    def new(cls, input: list[Any], db: Database, workflow_id: str) -> Self:
        """
        Create a new run with the given input.

        Args:
            input: Initial input values for the workflow
            db: Database instance
            workflow_id: Identifier of the owning workflow

        Returns:
            New Run instance
        """
        run = cls(str(uuid4()), db)
        run.set_input(Value(input))
        run._db.set(run._workflow_key, Value([workflow_id]))
        run.set_progress(RUN_PROGRESS_NOT_STARTED, [])
        return run

    @classmethod
    def get_unfinished(cls, db: Database, workflow_id: str | None = None) -> list[Run]:
        """
        Get all runs that are still resumable (neither finished nor failed).

        Args:
            db: Database instance
            workflow_id: If given, only return runs owned by this workflow.

        Returns:
            List of unfinished Run instances
        """
        run_ids = db.get_all_ids(RUN_PROGRESS_CF)
        runs = [Run(run_id, db) for run_id in run_ids]
        result = []
        for r in runs:
            if r.is_finished() or r.is_failed():
                continue
            if workflow_id is not None and r.get_workflow_id() != workflow_id:
                continue
            result.append(r)
        return result

    def get_workflow_id(self) -> str | None:
        """Return the owning workflow id, or None for legacy runs."""
        try:
            return self._db.get(self._workflow_key).get(0, str)
        except KeyError:
            return None

    def is_finished(self) -> bool:
        """
        Check if this run has completed successfully.

        Returns:
            True if run is finished, False otherwise
        """
        try:
            return self.get_progress() == RUN_PROGRESS_FINISHED
        except KeyError:
            # If progress key doesn't exist, run is not finished
            return False

    def is_failed(self) -> bool:
        """Check if this run has permanently failed (retries exhausted)."""
        try:
            return self.get_progress() == RUN_PROGRESS_FAILED
        except KeyError:
            return False

    def get_input(self) -> Value:
        """
        Get the initial input for this run.

        Returns:
            Value containing the input

        Raises:
            KeyError: If input was never set
        """
        return self._db.get(self._input_key)

    def set_input(self, val: Value) -> None:
        """
        Set the initial input for this run.

        Args:
            val: Value containing the input
        """
        self._db.set(self._input_key, val)

    def get_progress(self) -> int:
        """
        Get the current progress (last completed step index).

        Returns:
            Step index, or one of the RUN_PROGRESS_* sentinels

        Raises:
            KeyError: If progress was never set
        """
        progress_value = self._db.get(self._progress_key)
        return progress_value.get(0, int)

    def get_attempts(self) -> int:
        """Return how many times this run has been (re)started."""
        try:
            return self._db.get(self._attempts_key).get(0, int)
        except KeyError:
            return 0

    def increment_attempts(self) -> int:
        """Increment and persist the attempt counter, returning the new value."""
        attempts = self.get_attempts() + 1
        self._db.set(self._attempts_key, Value([attempts]))
        return attempts

    def get_step_output(self, step_id: int) -> Value:
        """
        Get the output from a specific step.

        Args:
            step_id: Index of the step

        Returns:
            Value containing the step's output

        Raises:
            KeyError: If step output doesn't exist
        """
        return self._db.get(self._get_step_output_key(step_id))

    def set_progress(self, step_id: int, val: list[Any]) -> None:
        """
        Update progress to a specific step and save its output.

        Args:
            step_id: Index of the completed step
            val: Output values from the step
        """
        self._set_step_output(step_id, Value(val))
        self._set_progress(step_id)

    def set_finished(self) -> None:
        """Mark this run as finished."""
        self._set_progress(RUN_PROGRESS_FINISHED)

    def set_failed(self) -> None:
        """Mark this run as permanently failed (no further resumption)."""
        self._set_progress(RUN_PROGRESS_FAILED)

    def _set_progress(self, step_id: int) -> None:
        """Internal method to update progress value."""
        self._db.set(self._progress_key, Value([step_id]))

    def _set_step_output(self, step_id: int, val: Value) -> None:
        """Internal method to save step output."""
        self._db.set(self._get_step_output_key(step_id), val)

    def _get_step_output_key(self, step_id: int) -> Key:
        """Generate key for storing step output."""
        return Key(column_family=STEP_OUTPUT_CF, record_id=f"{self._id}_{step_id}")


class Key(BaseModel):
    """
    Key for database operations.

    Combines column family and record ID.
    """

    column_family: str
    record_id: str


@final
class Value:
    """
    Value wrapper for database storage.

    Handles serialization/deserialization and type-safe access.

    Note: values are stored as JSON. On deserialization, pydantic models come
    back as plain ``dict`` objects; steps that need typed models should
    re-validate their inputs.
    """

    def __init__(self, inner: list[Any]) -> None:
        """
        Create a value from a list.

        Args:
            inner: List of values to store
        """
        self._inner = inner

    @property
    def inner(self) -> list[Any]:
        """The wrapped list of values."""
        return self._inner

    def get[T](self, index: int, out_t: type[T]) -> T:
        """
        Get a value at a specific index with type checking.

        Args:
            index: Index in the list
            out_t: Expected type

        Returns:
            The value at the index

        Raises:
            IndexError: If index is out of range
            TypeError: If value is not of expected type
        """
        val = self._inner[index]
        if isinstance(val, out_t):
            return val
        raise TypeError(
            f"Value at index {index} is of type {type(val).__name__}, "
            f"not {out_t.__name__}"
        )

    def to_str(self) -> str:
        """
        Serialize value to JSON string.

        Returns:
            JSON string representation

        Raises:
            TypeError: If any element is not JSON-serializable
        """
        return json.dumps(self._inner, default=_json_default)

    @classmethod
    def from_str(cls, string: str) -> Self:
        """
        Deserialize value from JSON string.

        Args:
            string: JSON string

        Returns:
            Value instance
        """
        return cls(json.loads(string))


class Database:
    """
    Database wrapper for RocksDB with column families.

    Provides key-value storage with automatic serialization.

    Can be used as a context manager to ensure the database is closed::

        with Database("./data") as db:
            ...
    """

    def __init__(self, path: str) -> None:
        """
        Initialize database at the given path.

        Creates required column families if they don't exist.

        Args:
            path: Path to database directory
        """
        self._path = path
        self._db = Rdict(path)
        self._ensure_column_families()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        """Close the underlying database. Safe to call multiple times."""
        with contextlib.suppress(DbClosedError):
            self._db.close()

    def _ensure_column_families(self) -> None:
        """
        Ensure all required column families exist.

        Creates them if they don't exist.
        """
        required_cfs = [
            RUN_INPUT_CF,
            RUN_PROGRESS_CF,
            RUN_WORKFLOW_CF,
            RUN_ATTEMPTS_CF,
            STEP_OUTPUT_CF,
        ]

        for cf_name in required_cfs:
            try:
                # Try to access the column family
                self._db.get_column_family(cf_name)
            except Exception:
                # If it doesn't exist, create it
                try:
                    self._db.create_column_family(cf_name)
                except Exception:
                    # Might already exist (race condition), ignore
                    logger.debug("Column family %r already exists", cf_name)

    def get(self, key: Key) -> Value:
        """
        Get a value from the database.

        Args:
            key: Key to retrieve

        Returns:
            Value at the key

        Raises:
            KeyError: If key doesn't exist
            ValueError: If the stored value cannot be deserialized
        """
        cf = self._db.get_column_family(key.column_family)
        value = cf.get(key.record_id)

        if value is None:
            raise KeyError(
                f"Key '{key.record_id}' not found in column family "
                f"'{key.column_family}'"
            )

        try:
            return Value.from_str(str(value))
        except json.JSONDecodeError as e:
            raise ValueError(
                f"Corrupt value for key '{key.record_id}' in column family "
                f"'{key.column_family}': {e}"
            ) from e

    def exists(self, key: Key) -> bool:
        """
        Check if a key exists in the database.

        Args:
            key: Key to check

        Returns:
            True if key exists, False otherwise
        """
        cf = self._db.get_column_family(key.column_family)
        return cf.get(key.record_id) is not None

    def get_all_ids(self, column_family: str) -> list[str]:
        """
        Get all record IDs in a column family.

        Args:
            column_family: Name of the column family

        Returns:
            List of record IDs
        """
        try:
            cf = self._db.get_column_family(column_family)
            return [str(k) for k in cf.keys()]  # noqa: SIM118 (rocksdict iterator)
        except Exception:
            # Column family might not exist or be empty
            return []

    def set(self, key: Key, value: Value) -> None:
        """
        Set a value in the database.

        Args:
            key: Key to store at
            value: Value to store
        """
        cf = self._db.get_column_family(key.column_family)
        cf[key.record_id] = value.to_str()
