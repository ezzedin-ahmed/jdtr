from __future__ import annotations

import asyncio
import functools
import inspect
import logging
import types
from collections.abc import Awaitable, Callable
from typing import (
    Annotated,
    Any,
    Union,
    final,
    get_args,
    get_origin,
    get_type_hints,
)

from fastapi import APIRouter, Body, Request
from fastapi.responses import JSONResponse

from jdtr.data import RUN_PROGRESS_NOT_STARTED, Database, Run

logger = logging.getLogger(__name__)

DEFAULT_MAX_RETRIES = 3


def _log_callback(task: asyncio.Task[Any]) -> None:
    """Callback to log exceptions from fire-and-forget tasks."""
    if task.cancelled():
        return
    exc = task.exception()
    if exc:
        logger.error("Task failed with exception: %s", exc, exc_info=exc)


def with_body_signature(
    func: Callable[..., Any],
    handler: Callable[..., Any] | None = None,
) -> Callable[..., Any]:
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
    wrapper: Callable[..., Any]
    if inspect.iscoroutinefunction(handler):

        async def _async_wrapper(*args: Any, **kwargs: Any) -> Any:
            return await handler(*args, **kwargs)

        wrapper = _async_wrapper
    else:

        def _sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            return handler(*args, **kwargs)

        wrapper = _sync_wrapper

    functools.update_wrapper(wrapper, func)
    wrapper.__signature__ = new_sig  # type: ignore[attr-defined]

    return wrapper


def _subtype(sub: Any, sup: Any) -> bool:
    """
    Best-effort structural compatibility check between two type annotations.

    Handles plain classes, parameterized generics (e.g. ``list[int]``),
    and unions/Optional. When a type cannot be reasoned about, this errs on
    the side of compatibility (returns True) to avoid rejecting valid steps.
    """
    if sub is Any or sup is Any or sup is object:
        return True

    # Normalize NoneType annotations.
    none_t = type(None)
    if sub is None:
        sub = none_t
    if sup is None:
        sup = none_t

    sub_origin = get_origin(sub)
    sup_origin = get_origin(sup)

    # Union / Optional handling (both typing.Union and PEP 604 `X | Y`):
    # every member of sub must fit sup; sup as a union is satisfied if any
    # member matches.
    unions = (Union, types.UnionType)
    if sup_origin in unions:
        return any(_subtype(sub, option) for option in get_args(sup))
    if sub_origin in unions:
        return all(_subtype(option, sup) for option in get_args(sub))

    # Parameterized generics: compare origins, then arguments positionally.
    if sup_origin is not None:
        if sub_origin is None:
            # e.g. bare `list` provided where `list[int]` expected.
            sub_base = sub if isinstance(sub, type) else None
            sup_base = sup_origin if isinstance(sup_origin, type) else None
            if sub_base is None or sup_base is None:
                return True
            return issubclass(sub_base, sup_base)
        if not (isinstance(sub_origin, type) and isinstance(sup_origin, type)):
            return True
        if not issubclass(sub_origin, sup_origin):
            return False
        sub_args, sup_args = get_args(sub), get_args(sup)
        if not sub_args or not sup_args or len(sub_args) != len(sup_args):
            return True  # unparameterized on one side; don't over-constrain
        return all(_subtype(sa, pa) for sa, pa in zip(sub_args, sup_args, strict=True))

    # sup is a plain class; sub should be a subclass of it.
    if isinstance(sub, type) and isinstance(sup, type):
        try:
            return issubclass(sub, sup)
        except TypeError:
            return True
    # sub is a generic (e.g. list[int]) where sup is a plain class.
    if (
        sub_origin is not None
        and isinstance(sub_origin, type)
        and isinstance(sup, type)
    ):
        return issubclass(sub_origin, sup)

    # Unresolvable annotations: don't block the workflow.
    return True


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

    # Check if f returns a tuple that fans out into multiple parameters.
    if get_origin(f_return) is tuple:
        f_types = get_args(f_return)

        # A tuple return maps to multiple params only when g takes >1 param;
        # otherwise treat the tuple as a single value passed through.
        if len(g_types) > 1:
            if len(f_types) != len(g_types):
                return False
            return all(
                _subtype(ft, gt) for ft, gt in zip(f_types, g_types, strict=True)
            )

    # Single return value case
    if len(g_types) != 1:
        return False

    return _subtype(f_return, g_types[0])


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
        max_retries: int = DEFAULT_MAX_RETRIES,
    ) -> None:
        """
        Initialize a workflow.

        Args:
            workflow_id: Unique identifier for this workflow
            steps: List of async functions to execute in sequence
            db: Database for persisting workflow state
            max_retries: Number of times a run may be resumed after a failure
                before it is marked permanently failed. Guards against poison
                runs that would otherwise retry forever.

        Raises:
            TypeError: If step types are incompatible
            ValueError: If steps is empty
        """
        if not steps:
            raise ValueError("A workflow must have at least one step")
        self._steps: list[Step] = steps
        self._workflow_id = workflow_id
        self._db = db
        self._max_retries = max_retries
        self._check_types()
        self._resume_lock = asyncio.Lock()

    async def initialize(self) -> None:
        """
        Initialize the workflow by resuming any unfinished runs.

        This must be called after __init__ to start background resume tasks.
        Call this method once after creating the workflow.
        """
        unfinished = Run.get_unfinished(self._db, workflow_id=self._workflow_id)
        for run in unfinished:
            # Create background task for each unfinished run
            task = asyncio.create_task(self._resume_with_lock(run))
            task.add_done_callback(_log_callback)

    async def _resume_with_lock(self, run: Run) -> None:
        """Resume a run with locking to prevent concurrent resumes in-process."""
        async with self._resume_lock:
            # Re-read state: another task/instance may have advanced it.
            if run.is_finished() or run.is_failed():
                return
            attempts = run.increment_attempts()
            try:
                await self._resume(run)
            except Exception:
                if attempts > self._max_retries:
                    logger.error(
                        "Run %s of workflow %s exhausted %d retries; marking as failed",
                        run._id,
                        self._workflow_id,
                        self._max_retries,
                    )
                    run.set_failed()
                # Exception already logged in _run_steps; swallow so one poison
                # run does not take down the resume loop.

    async def _resume(self, run: Run) -> None:
        """
        Resume an unfinished run from where it left off.

        Args:
            run: The run to resume
        """
        last_done_step = run.get_progress()

        if last_done_step == RUN_PROGRESS_NOT_STARTED:
            # Run never started, get initial input
            input_list = run.get_input().inner
            await self._run_steps(input_list, run, start_from=0)
        else:
            # Resume from next step after last completed
            input_list = run.get_step_output(last_done_step).inner
            await self._run_steps(input_list, run, start_from=last_done_step + 1)

    async def run(self, *args: Any) -> None:
        """
        Start a new workflow run with the given input.

        Args:
            *args: Input values for the workflow's first step.
        """
        input = list(args)
        run_state = Run.new(input, self._db, self._workflow_id)
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
            logger.error(
                "Workflow %s failed at step %d: %s",
                self._workflow_id,
                start_from + i,
                e,
                exc_info=e,
            )
            # Run is left unfinished, with progress at the last successful step.
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
