from __future__ import annotations

import json
from typing import Any, Self, final
from uuid import uuid4

from pydantic import BaseModel
from rocksdict import DbClosedError, Rdict

# Column family names
RUN_INPUT_CF = "runinput"
RUN_PROGRESS_CF = "runprogress"
STEP_OUTPUT_CF = "stepoutput"

# Progress states
RUN_PROGRESS_FINISHED = -2
RUN_PROGRESS_NOT_STARTED = -1


@final
class Run:
    """
    Represents a single execution of a workflow.

    Tracks input, progress, and outputs for each step.
    Persists state to database for resumability.
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

    @classmethod
    def new(cls, input: list[Any], db: Database) -> Self:
        """
        Create a new run with the given input.

        Args:
            input: Initial input values for the workflow
            db: Database instance

        Returns:
            New Run instance
        """
        run = cls(str(uuid4()), db)
        run.set_input(Value(input))
        run.set_progress(RUN_PROGRESS_NOT_STARTED, [])
        return run

    @classmethod
    def get_unfinished(cls, db: Database) -> list[Run]:
        """
        Get all runs that have not finished.

        Args:
            db: Database instance

        Returns:
            List of unfinished Run instances
        """
        run_ids = db.get_all_ids(RUN_PROGRESS_CF)
        runs = [Run(run_id, db) for run_id in run_ids]
        return [r for r in runs if not r.is_finished()]

    def is_finished(self) -> bool:
        """
        Check if this run has completed.

        Returns:
            True if run is finished, False otherwise
        """
        try:
            return self.get_progress() == RUN_PROGRESS_FINISHED
        except KeyError:
            # If progress key doesn't exist, run is not finished
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
            Step index, or RUN_PROGRESS_NOT_STARTED or RUN_PROGRESS_FINISHED

        Raises:
            KeyError: If progress was never set
        """
        progress_value = self._db.get(self._progress_key)
        return progress_value.get(0, int)

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
        self._set_progress(step_id)
        self._set_step_output(step_id, Value(val))

    def set_finished(self) -> None:
        """Mark this run as finished."""
        self._set_progress(RUN_PROGRESS_FINISHED)

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
    """

    def __init__(self, inner: list[Any]) -> None:
        """
        Create a value from a list.

        Args:
            inner: List of values to store
        """
        self._inner = inner

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
        """
        try:
            return json.dumps(self._inner)
        except TypeError as e:
            if isinstance(self._inner[0], BaseModel):
                return json.dumps([self._inner[0].model_dump()])
            else:
                raise e

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
    """

    def __init__(self, path: str) -> None:
        """
        Initialize database at the given path.

        Creates required column families if they don't exist.

        Args:
            path: Path to database directory
        """
        # Initialize RocksDB with column families
        self._path = path
        self._db = Rdict(path)
        self._ensure_column_families()

    def close(self) -> None:
        try:
            self._db.close()
        except DbClosedError:
            pass

    def _ensure_column_families(self) -> None:
        """
        Ensure all required column families exist.

        Creates them if they don't exist.
        """
        required_cfs = [RUN_INPUT_CF, RUN_PROGRESS_CF, STEP_OUTPUT_CF]

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
                    pass

    def get(self, key: Key) -> Value:
        """
        Get a value from the database.

        Args:
            key: Key to retrieve

        Returns:
            Value at the key

        Raises:
            KeyError: If key doesn't exist
        """
        try:
            cf = self._db.get_column_family(key.column_family)
            value = cf.get(key.record_id)

            if value is None:
                raise KeyError(
                    f"Key '{key.record_id}' not found in column family '{key.column_family}'"
                )

            return Value.from_str(str(value))
        except KeyError:
            raise
        except Exception as e:
            raise KeyError(
                f"Error retrieving key '{key.record_id}' from column family "
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
        try:
            cf = self._db.get_column_family(key.column_family)
            value = cf.get(key.record_id)
            return value is not None
        except Exception:
            return False

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
            return [str(k) for k in cf.keys()]
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
