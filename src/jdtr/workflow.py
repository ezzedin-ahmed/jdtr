from __future__ import annotations

import asyncio
import functools
import inspect
import logging
from collections.abc import Awaitable
from typing import (
    Annotated,
    Any,
    Callable,
    final,
    get_type_hints,
)

from fastapi import APIRouter, Body, Request
from fastapi.responses import JSONResponse

from jdtr.data import RUN_PROGRESS_NOT_STARTED, Database, Run


def _log_callback(task: asyncio.Task[Any]) -> None:
    """Callback to log exceptions from fire-and-forget tasks."""
    exc = task.exception()
    if exc:
        logging.error(f"Task failed with exception: {exc}", exc_info=exc)


def with_body_signature(func, handler=None):
    """
    Create a new function whose signature uses FastAPI Body parameters,
    while delegating execution to `handler` (or `func` if handler is None).
    """
    if handler is None:
        handler = func

    sig = inspect.signature(func)
    new_params = []

    for param in sig.parameters.values():
        # Skip FastAPI-injected parameters
        if param.annotation in (Request,):
            new_params.append(param)
            continue

        if param.annotation is inspect._empty:
            raise TypeError(f"Parameter '{param.name}' must have a type annotation")

        annotated = Annotated[
            param.annotation,
            Body(param.default if param.default is not inspect._empty else ...),
        ]

        new_params.append(
            param.replace(
                annotation=annotated,
                default=inspect._empty,
            )
        )

    new_params.append(
        inspect.Parameter(
            "phantom",
            kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
            default=Body("__phantom"),
        )
    )
    new_sig = sig.replace(parameters=new_params, return_annotation=JSONResponse)

    # Create wrapper with correct sync/async behavior
    if inspect.iscoroutinefunction(handler):

        async def wrapper(*args, **kwargs):
            return await handler(*args, **kwargs)
    else:

        def wrapper(*args, **kwargs):
            return handler(*args, **kwargs)

    functools.update_wrapper(wrapper, func)
    wrapper.__signature__ = new_sig

    return wrapper


def type_compatible(f: Callable[..., Any], g: Callable[..., Any]) -> bool:
    """
    Check if the return type of f is compatible with the parameters of g.

    Returns True if:
    - f returns None and g has no parameters
    - f returns a single type compatible with g's single parameter
    - f returns a tuple whose types match g's parameters in order
    """
    f_hints = get_type_hints(f)
    f_return = f_hints.get("return", Any)

    g_hints = get_type_hints(g)
    g_params = list(inspect.signature(g).parameters.values())
    g_types = [g_hints.get(p.name, Any) for p in g_params]

    # If g has no parameters, f should return None or have no return annotation
    if not g_types:
        return f_return is type(None) or f_return is None or f_return is Any

    # Check if f returns a tuple
    if getattr(f_return, "__origin__", None) is tuple:
        f_types = getattr(f_return, "__args__", ())

        # Number of return values must match number of parameters
        if len(f_types) != len(g_types):
            return False

        # Check each return type against corresponding parameter type
        for ft, gt in zip(f_types, g_types):
            # If either is Any, consider it compatible
            if ft is Any or gt is Any:
                continue
            # Both must be types and ft must be subclass of gt
            if not (
                isinstance(ft, type) and isinstance(gt, type) and issubclass(ft, gt)
            ):
                return False
        return True

    # Single return value case
    if len(g_types) != 1:
        return False

    gt = g_types[0]

    # If either is Any, consider compatible
    if f_return is Any or gt is Any:
        return True

    # Both must be types and f_return must be subclass of gt
    if isinstance(f_return, type) and isinstance(gt, type):
        try:
            return issubclass(f_return, gt)
        except TypeError:
            # issubclass can raise TypeError for some generic types
            return False

    return False


type Step = Callable[..., Awaitable[Any]]
type StepInput = list[Any]


@final
class Workflow:
    """
    A workflow that executes a series of async steps in sequence.

    Steps are type-checked to ensure compatibility between outputs and inputs.
    Workflows can be paused and resumed using persistent storage.
    """

    def __init__(
        self,
        workflow_id: str,
        steps: list[Step],
        db: Database,
    ) -> None:
        """
        Initialize a workflow.

        Args:
            workflow_id: Unique identifier for this workflow
            steps: List of async functions to execute in sequence
            db: Database for persisting workflow state

        Raises:
            TypeError: If step types are incompatible
        """
        self._steps: list[Step] = steps
        self._workflow_id = workflow_id
        self._db = db
        self._check_types()
        self._resume_lock = asyncio.Lock()

    async def initialize(self) -> None:
        """
        Initialize the workflow by resuming any unfinished runs.

        This must be called after __init__ to start background resume tasks.
        Call this method once after creating the workflow.
        """
        unfinished = Run.get_unfinished(self._db)
        for run in unfinished:
            # Create background task for each unfinished run
            task = asyncio.create_task(self._resume_with_lock(run))
            task.add_done_callback(_log_callback)

    async def _resume_with_lock(self, run: Run) -> None:
        """Resume a run with locking to prevent race conditions."""
        async with self._resume_lock:
            # Check if still unfinished (another instance might have completed it)
            if not run.is_finished():
                await self._resume(run)

    async def _resume(self, run: Run) -> None:
        """
        Resume an unfinished run from where it left off.

        Args:
            run: The run to resume
        """
        last_done_step = run.get_progress()

        if last_done_step == RUN_PROGRESS_NOT_STARTED:
            # Run never started, get initial input
            input_value = run.get_input()
            input_list = input_value._inner
            await self._run_steps(input_list, run, start_from=0)
        else:
            # Resume from next step after last completed
            input_value = run.get_step_output(last_done_step)
            input_list = input_value._inner
            await self._run_steps(input_list, run, start_from=last_done_step + 1)

    async def run(self, *args) -> None:
        """
        Start a new workflow run with the given input.

        Args:
            input: Input data conforming to workflow's input type
        """
        input = list(args)
        run_state = Run.new(input, self._db)
        await self._run_steps(input, run_state, start_from=0)

    async def _run_steps(
        self,
        input: list[Any],
        run: Run,
        start_from: int = 0,
    ) -> None:
        """
        Execute workflow steps starting from a given position.

        Args:
            input: Input values for the first step to execute
            run: Run object for tracking progress
            start_from: Index of first step to execute (0-based)

        Raises:
            Exception: Any exception raised by a step is propagated
        """
        result = input
        i = 0

        try:
            for i, step in enumerate(self._steps[start_from:]):
                # Calculate absolute step index
                absolute_step_id = start_from + i

                # Execute step
                step_result = await step(*result)

                # Convert result to list (handle both single values and tuples)
                if isinstance(step_result, tuple):
                    result = list(step_result)
                else:
                    result = [step_result]

                # Save progress
                run.set_progress(absolute_step_id, result)

            # Mark run as finished
            run.set_finished()

        except Exception as e:
            # Log the error and re-raise
            logging.error(
                f"Workflow {self._workflow_id} failed at step {start_from + i}: {e}",
                exc_info=e,
            )
            # Note: Run is left in unfinished state with progress at last successful step
            raise

    def _check_types(self) -> None:
        """
        Validate that all consecutive steps have compatible types.

        Raises:
            TypeError: If any two consecutive steps have incompatible types
        """
        prev = None
        for step in self._steps:
            if prev and not type_compatible(prev, step):
                prev_sig = inspect.signature(prev)
                step_sig = inspect.signature(step)
                raise TypeError(
                    f"Incompatible types between steps. "
                    f"Return type of {prev.__name__}{prev_sig} "
                    f"is not compatible with parameters of {step.__name__}{step_sig}."
                )
            prev = step

    def as_router(self, prefix: str = "") -> APIRouter:
        """
        Create a FastAPI router for this workflow.

        Args:
            prefix: URL prefix for the router (default: "")

        Returns:
            APIRouter with a POST endpoint to trigger workflow runs
        """

        async def handler_logic(**kwargs) -> dict[str, str]:
            """Trigger a new workflow run."""
            kwargs.pop("phantom")
            task = asyncio.create_task(self.run(*kwargs.values()))
            task.add_done_callback(_log_callback)
            return {"status": "started", "workflow_id": self._workflow_id}

        handler = with_body_signature(self._steps[0], handler_logic)

        router = APIRouter(prefix=f"{prefix}/{self._workflow_id}")
        _ = router.post("/")(handler)

        return router
