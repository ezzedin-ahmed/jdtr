import asyncio
import json
import shutil
import tempfile
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from jdtr import (
    RUN_INPUT_CF,
    RUN_PROGRESS_FINISHED,
    RUN_PROGRESS_NOT_STARTED,
    Database,
    Key,
    Run,
    Value,
    Workflow,
    type_compatible,
)


# Test Fixtures
@pytest.fixture
def temp_db_path():
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def db(temp_db_path):
    return Database(temp_db_path)


# ============================================================================
# Database Layer Tests
# ============================================================================


class TestValue:
    def test_value_creation(self):
        val = Value([1, "test", {"key": "value"}])
        assert val._inner == [1, "test", {"key": "value"}]

    def test_value_get_correct_type(self):
        val = Value([42, "hello"])
        assert val.get(0, int) == 42
        assert val.get(1, str) == "hello"

    def test_value_get_wrong_type(self):
        val = Value([42])
        with pytest.raises(TypeError):
            val.get(0, str)

    def test_value_to_str(self):
        val = Value([1, "test", [2, 3]])
        result = val.to_str()
        assert json.loads(result) == [1, "test", [2, 3]]

    def test_value_from_str(self):
        val = Value.from_str('[1, "test", {"key": "value"}]')
        assert val._inner == [1, "test", {"key": "value"}]

    def test_value_roundtrip(self):
        original = Value([1, 2.5, "test", None, {"a": 1}])
        serialized = original.to_str()
        deserialized = Value.from_str(serialized)
        assert deserialized._inner == original._inner


class TestKey:
    def test_key_creation(self):
        key = Key(column_family="test_cf", record_id="test_id")
        assert key.column_family == "test_cf"
        assert key.record_id == "test_id"

    def test_key_equality(self):
        key1 = Key(column_family="cf", record_id="id")
        key2 = Key(column_family="cf", record_id="id")
        key3 = Key(column_family="cf", record_id="other")
        assert key1 == key2
        assert key1 != key3


class TestDatabase:
    def test_database_creation(self, temp_db_path):
        db = Database(temp_db_path)
        assert db._db is not None

    def test_set_and_get(self, db):
        key = Key(column_family=RUN_INPUT_CF, record_id="test_run")
        value = Value([1, 2, 3])

        db.set(key, value)
        retrieved = db.get(key)

        assert retrieved._inner == [1, 2, 3]

    def test_get_nonexistent_key(self, db):
        key = Key(column_family=RUN_INPUT_CF, record_id="nonexistent")
        with pytest.raises(KeyError):
            db.get(key)

    def test_get_all_ids_empty(self, db):
        ids = db.get_all_ids(RUN_INPUT_CF)
        assert ids == []

    def test_get_all_ids_multiple(self, db):
        keys = [
            Key(column_family=RUN_INPUT_CF, record_id="id1"),
            Key(column_family=RUN_INPUT_CF, record_id="id2"),
            Key(column_family=RUN_INPUT_CF, record_id="id3"),
        ]

        for key in keys:
            db.set(key, Value([1]))

        ids = db.get_all_ids(RUN_INPUT_CF)
        assert set(ids) == {"id1", "id2", "id3"}

    def test_overwrite_value(self, db):
        key = Key(column_family=RUN_INPUT_CF, record_id="test")
        db.set(key, Value([1]))
        db.set(key, Value([2]))

        retrieved = db.get(key)
        assert retrieved._inner == [2]


class TestRun:
    def test_run_new(self, db):
        run = Run.new([1, 2, 3], db)
        assert run._id is not None
        assert len(run._id) > 0

        # Verify input was stored
        input_val = run.get_input()
        assert input_val._inner == [1, 2, 3]

    def test_run_initial_progress(self, db):
        run = Run.new([1], db)
        assert run.get_progress() == RUN_PROGRESS_NOT_STARTED
        assert not run.is_finished()

    def test_run_set_progress(self, db):
        run = Run.new([1], db)
        run.set_progress(0, ["step0_output"])

        assert run.get_progress() == 0
        output = run.get_step_output(0)
        assert output._inner == ["step0_output"]

    def test_run_set_finished(self, db):
        run = Run.new([1], db)
        run.set_finished()

        assert run.is_finished()
        assert run.get_progress() == RUN_PROGRESS_FINISHED

    def test_run_multiple_steps(self, db):
        run = Run.new([1], db)

        run.set_progress(0, ["output0"])
        run.set_progress(1, ["output1"])
        run.set_progress(2, ["output2"])

        assert run.get_progress() == 2
        assert run.get_step_output(0)._inner == ["output0"]
        assert run.get_step_output(1)._inner == ["output1"]
        assert run.get_step_output(2)._inner == ["output2"]

    def test_get_unfinished_empty(self, db):
        runs = Run.get_unfinished(db)
        assert runs == []

    def test_get_unfinished_single(self, db):
        run = Run.new([1], db)
        run.set_progress(0, ["output"])

        unfinished = Run.get_unfinished(db)
        assert len(unfinished) == 1
        assert unfinished[0]._id == run._id

    def test_get_unfinished_excludes_finished(self, db):
        run1 = Run.new([1], db)
        run2 = Run.new([2], db)

        run1.set_finished()
        run2.set_progress(0, ["output"])

        unfinished = Run.get_unfinished(db)
        assert len(unfinished) == 1
        assert unfinished[0]._id == run2._id

    def test_get_unfinished_multiple(self, db):
        run1 = Run.new([1], db)
        run2 = Run.new([2], db)
        run3 = Run.new([3], db)

        run1.set_progress(0, ["out1"])
        run2.set_progress(1, ["out2"])
        run3.set_finished()

        unfinished = Run.get_unfinished(db)
        unfinished_ids = {r._id for r in unfinished}

        assert len(unfinished) == 2
        assert run1._id in unfinished_ids
        assert run2._id in unfinished_ids
        assert run3._id not in unfinished_ids


# ============================================================================
# Type Compatibility Tests
# ============================================================================


class TestTypeCompatible:
    def test_none_return_with_no_params(self):
        def f() -> None:
            pass

        def g():
            pass

        assert type_compatible(f, g)

    def test_single_return_single_param_compatible(self):
        def f() -> int:
            return 1

        def g(x: int):
            pass

        assert type_compatible(f, g)

    def test_single_return_single_param_incompatible(self):
        def f() -> str:
            return "test"

        def g(x: int):
            pass

        assert not type_compatible(f, g)

    def test_tuple_return_multiple_params_compatible(self):
        def f() -> tuple[int, str]:
            return (1, "test")

        def g(x: int, y: str):
            pass

        assert type_compatible(f, g)

    def test_tuple_return_wrong_length(self):
        def f() -> tuple[int, str]:
            return (1, "test")

        def g(x: int):
            pass

        assert not type_compatible(f, g)

    def test_tuple_return_incompatible_types(self):
        def f() -> tuple[str, int]:
            return ("test", 1)

        def g(x: int, y: str):
            pass

        assert not type_compatible(f, g)

    def test_no_type_hints(self):
        def f():
            return 1

        def g(x):
            pass

        # Should handle Any type
        assert type_compatible(f, g)

    def test_subclass_compatibility(self):
        class Parent:
            pass

        class Child(Parent):
            pass

        def f() -> Child:
            return Child()

        def g(x: Parent):
            pass

        assert type_compatible(f, g)


# ============================================================================
# Workflow Tests
# ============================================================================


@pytest.mark.asyncio
class TestWorkflow:
    async def test_workflow_single_step(self, db):
        async def step1(value: int) -> int:
            return value * 2

        workflow = Workflow("test", [step1], db)

        await workflow.run(5)

        # Verify run completed
        runs = Run.get_unfinished(db)
        assert len(runs) == 0

    async def test_workflow_multiple_steps(self, db):
        async def step1(value: int) -> int:
            return value * 2

        async def step2(x: int) -> str:
            return f"Result: {x}"

        async def step3(s: str) -> tuple[str, int]:
            return (s.upper(), len(s))

        workflow = Workflow("test", [step1, step2, step3], db)

        await workflow.run(5)

        runs = Run.get_unfinished(db)
        assert len(runs) == 0

    async def test_workflow_type_check_incompatible(self, db):
        async def step1() -> int:
            return 1

        async def step2(x: str):
            pass

        with pytest.raises(TypeError, match="Incompatible types"):
            workflow = Workflow("test", [step1, step2], db)

    async def test_workflow_type_check(self, db):
        async def step1() -> int:
            return 1

        async def step2(x: int) -> str:
            return "test"

        async def step3(x: int):  # Incompatible with step2
            pass

        with pytest.raises(TypeError):
            workflow = Workflow("test", [step1, step2, step3], db)

    async def test_workflow_resume_unfinished(self, db):
        step2_called = False

        async def step1(input) -> int:
            return input.value * 2

        async def step2(x: int) -> str:
            nonlocal step2_called
            step2_called = True
            return f"Result: {x}"

        # Create a run that's partially complete
        run = Run.new([5, "test"], db)
        run.set_progress(0, [10])  # step1 completed

        # Create workflow - should resume the unfinished run
        workflow = Workflow("test", [step1, step2], db)
        await workflow.initialize()

        # Wait for resume to complete
        await asyncio.sleep(0.1)

        assert step2_called

    async def test_workflow_step_indexing(self, db):
        outputs = []

        async def step1(value) -> int:
            return value

        async def step2(x: int) -> int:
            return x * 2

        async def step3(x: int) -> int:
            outputs.append(x)
            return x * 3

        # Create partially complete run
        run = Run.new([5], db)
        run.set_progress(0, [5])
        run.set_progress(1, [10])

        progress = run.get_progress()
        assert progress == 1

        workflow = Workflow("test", [step1, step2, step3], db)
        await workflow.initialize()
        await asyncio.sleep(0.1)

        progress = run.get_progress()
        assert progress == RUN_PROGRESS_FINISHED

    async def test_workflow_parallel_execution(self, db):
        counter = 0

        async def step1(value) -> int:
            nonlocal counter
            counter += 1
            await asyncio.sleep(0.01)
            return value

        workflow = Workflow("test", [step1], db)

        # Start multiple runs
        tasks = [workflow.run(i) for i in range(5)]

        await asyncio.gather(*tasks)
        assert counter == 5

    async def test_workflow_error_handling(self, db):
        async def failing_step(input) -> int:
            raise ValueError("Step failed")

        workflow = Workflow("test", [failing_step], db)

        with pytest.raises(ValueError):
            await workflow.run("test")

        # Run should be in inconsistent state
        runs = Run.get_unfinished(db)
        assert len(runs) > 0  # Run not properly cleaned up

    async def test_workflow_race_condition(self, db):
        async def step1(input) -> int:
            await asyncio.sleep(0.01)
            return input["value"]

        # Create unfinished run
        run = Run.new([5], db)
        run.set_progress(RUN_PROGRESS_NOT_STARTED, [])

        # Create two workflow instances
        workflow1 = Workflow("test", [step1], db)
        workflow2 = Workflow("test", [step1], db)

        await asyncio.sleep(0.1)

        # TODO: test both not process same run


# ============================================================================
# FastAPI Router Tests
# ============================================================================


@pytest.mark.asyncio
class TestWorkflowRouter:
    async def test_router_creation(self, db):
        async def step1(value: int) -> int:
            return value

        workflow = Workflow("test", [step1], db)
        router = workflow.as_router(prefix="/api")

        assert router.prefix == "/api/test"

    async def test_router_endpoint(self, db):
        async def step1(value: int) -> int:
            await asyncio.sleep(0.01)
            return value * 2

        workflow = Workflow("test", [step1], db)
        router = workflow.as_router()

        app = FastAPI()
        app.include_router(router)

        client = TestClient(app)
        response = client.post("/test/", json={"value": 5, "value2": "hello"})

        print(response.json())
        assert response.status_code == 200

    async def test_router_async_execution(self, db):
        execution_started = False

        async def step1(value: int, value2: str) -> int:
            nonlocal execution_started
            execution_started = True
            await asyncio.sleep(0.05)
            return value

        workflow = Workflow("test", [step1], db)
        router = workflow.as_router()

        app = FastAPI()
        app.include_router(router)

        client = TestClient(app)
        response = client.post("/test/", json={"value": 5})
        assert response.status_code == 422

        response = client.post("/test/", json={"value": 5, "value2": "hello"})

        # Should return immediately
        print(response.json())
        assert response.status_code == 200

        # But execution might not have started yet
        # Wait and check
        await asyncio.sleep(0.1)
        assert execution_started

    async def test_router_error_logging(self, db):
        async def failing_step(value: int) -> int:
            raise ValueError("Test error")

        workflow = Workflow("test", [failing_step], db)
        router = workflow.as_router()

        app = FastAPI()
        app.include_router(router)

        with patch("logging.error") as mock_log:
            client = TestClient(app)
            response = client.post("/test/", json={"value": 5})

            await asyncio.sleep(0.1)

            # Error should be logged
            assert mock_log.called


# ============================================================================
# Integration Tests
# ============================================================================


@pytest.mark.asyncio
class TestIntegration:
    async def test_complete_workflow_lifecycle(self, db):
        async def sum_numbers(numbers: list[int]) -> int:
            return sum(numbers)

        async def double_result(x: int) -> int:
            return x * 2

        async def format_output(x: int) -> str:
            return f"Final result: {x}"

        workflow = Workflow(
            "math_workflow", [sum_numbers, double_result, format_output], db
        )

        input_data = [1, 2, 3, 4, 5]
        await workflow.run(input_data)

        # Verify completion
        runs = Run.get_unfinished(db)
        assert len(runs) == 0

    async def test_workflow_persistence_and_recovery(self, temp_db_path):
        step2_executed = False

        async def step1(value: int) -> int:
            return value * 10

        async def step2(x: int) -> str:
            nonlocal step2_executed
            step2_executed = True
            return str(x)

        # First session: create workflow and partially execute
        db1 = Database(temp_db_path)
        run = Run.new([5], db1)
        run.set_progress(0, [50])

        # Close database (simulate restart)
        db1.close()

        # Second session: recreate workflow
        db2 = Database(temp_db_path)
        workflow = Workflow("test", [step1, step2], db2)
        await workflow.initialize()

        await asyncio.sleep(0.1)

        # step2 should have been executed
        assert step2_executed

    async def test_multiple_workflows_same_database(self, db):
        async def workflow1_step(x: int) -> int:
            return x + 1

        async def workflow2_step(y: str) -> str:
            return y.upper()

        wf1 = Workflow("workflow1", [workflow1_step], db)
        wf2 = Workflow("workflow2", [workflow2_step], db)

        await wf1.run(10)
        await wf2.run("hello")

        runs = Run.get_unfinished(db)
        assert len(runs) == 0

    async def test_workflow_with_complex_data(self, db):
        async def process_data(data: dict[str, list[int]]) -> tuple[int, int]:
            total = sum(sum(vals) for vals in data.values())
            count = sum(len(vals) for vals in data.values())
            return (total, count)

        async def calculate_average(total: int, count: int) -> float:
            return total / count if count > 0 else 0.0

        workflow = Workflow("complex", [process_data, calculate_average], db)

        input_data = {"a": [1, 2, 3], "b": [4, 5], "c": [6, 7, 8, 9]}

        await workflow.run(input_data)

        runs = Run.get_unfinished(db)
        assert len(runs) == 0


@pytest.mark.parametrize("num_steps", [1, 2, 5, 10])
@pytest.mark.asyncio
async def test_workflow_varying_length(db, num_steps):
    async def increment(x: int) -> int:
        return x + 1

    # First step takes Input
    async def first_step(value: int) -> int:
        return value + 1

    steps = [first_step] + [increment for _ in range(num_steps - 1)]

    workflow = Workflow(f"test_{num_steps}", steps, db)
    await workflow.run(0)

    runs = Run.get_unfinished(db)
    assert len(runs) == 0
