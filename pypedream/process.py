from typing import Callable, Tuple, Set, Dict
import functools
from enum import Enum
from collections import namedtuple
import sys
import traceback
import multiprocessing as mp
from multiprocessing.managers import Namespace
from queue import Full, Empty

_MANAGER = mp.Manager()

class _QueueStatus(Enum):
    DONE = 1
    CONTINUE = 2
    TIMEOUT = 3
    UNDEFINED = 4

class _QueueItem(object):
    def __eq__(self, other):
        if isinstance(other, _QueueStatus):
            return False
        else:
            return super(_QueueItem, self).__eq__(other)

    def __hash__(self):
        if isinstance(self, _QueueStatus):
            return hash(self._name_)
        return hash((self.args[0], self.target))

def _get_namespace():
    return _MANAGER.Namespace()

class _Stage(_QueueItem):
    def __init__(self, worker_constructor, workers, maxsize, target, args, dependencies):
        self.worker_constructor = worker_constructor
        self.workers = workers
        self.maxsize = maxsize
        self.target = target
        self.args = args
        self.dependencies = dependencies

    def __iter__(self):
        return to_iterable(self)

    def __or__(self, f):
        return f(self)

    def __repr__(self):
        return ("_Stage(worker_constructor = {worker_constructor}, workers = {workers}, maxsize = {maxsize},"
                "target = {target}, args = {args}, dependencies = {dependencies})".format(
                    worker_constructor=self.worker_constructor,
                    workers=self.workers,
                    maxsize=self.maxsize,
                    target=self.target,
                    args=self.args,
                    dependencies=len(self.dependencies),
                ))

class Partial(_QueueItem):
    def __init__(self, fn: Callable):
        self.fn = fn

    def __or__(self, stage: _Stage):
        return self.fn(stage)

    def __ror__(self, stage: _Stage):
        return self.fn(stage)

    def __call__(self, stage):
        return self.fn(stage)

class _StageParams(
        namedtuple("_StageParams", [
            "input_queue",
            "output_queues",
            "pipeline_namespace",
            "pipeline_error_queue",
            "index",
        ])):
    pass

WorkerInfo = namedtuple("WorkerInfo", ["index"])

class StageStatus(object):
    """Object passed to various `on_done` callbacks. It contains information about the stage in case book
    keeping is needed.
    """
    def __init__(self, namespace, lock):
        self._namespace = namespace
        self._lock = lock

    @property
    def done(self) -> bool:
        """If all workers finished."""
        with self._lock:
            return self._namespace.active_workers == 0

    @property
    def active_workers(self) -> int:
        """Number of active workers."""
        with self._lock:
            return self._namespace.active_workers

    def __str__(self):
        return "StageStatus(done = {done}, active_workers = {active_workers})".format(
            done=self.done,
            active_workers=self.active_workers,
        )

class _InputQueue(object):
    def __init__(self, maxsize, total_done, pipeline_namespace, **kwargs):
        self.queue: mp.Queue = mp.Queue(maxsize=maxsize, **kwargs)
        self.lock = mp.Lock()
        self.namespace = _MANAGER.Namespace()
        self.namespace.remaining = total_done
        self.pipeline_namespace = pipeline_namespace

    def __iter__(self):
        while not self.is_done():
            x = self.get()
            if self.pipeline_namespace.error:
                return
            if x != _QueueStatus.CONTINUE:
                yield x

    def get(self):
        try:
            x = self.queue.get(block=False)
        except (Empty, Full):
            return _QueueStatus.CONTINUE
        if x != _QueueStatus.DONE:
            return x
        else:
            with self.lock:
                self.namespace.remaining -= 1
            return _QueueStatus.CONTINUE

    def is_done(self):
        return self.namespace.remaining == 0 and self.queue.empty()

    def put(self, x):
        self.queue.put(x)

    def done(self):
        self.queue.put(_QueueStatus.DONE)

class _OutputQueueList(list):
    def put(self, x):
        for queue in self:
            queue.put(x)

    def done(self):
        for queue in self:
            queue.put(_QueueStatus.DONE)

def _handle_exceptions(params):
    def handle_exceptions(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except BaseException as e:
                params.pipeline_error_queue.put((type(e), e, "".join(traceback.format_exception(*sys.exc_info()))))
                params.pipeline_namespace.error = True
        return wrapper
    return handle_exceptions

def _run_task(f_task, params):
    try:
        if params.input_queue:
            for x in params.input_queue:
                f_task(x)
        else:
            f_task()
        params.output_queues.done()
    except BaseException as e:
        try:
            params.pipeline_error_queue.put((type(e), e, "".join(traceback.format_exception(*sys.exc_info()))))
            params.pipeline_namespace.error = True
        except BaseException as e:
            print(e)

def _map(f, params):
    @_handle_exceptions(params)
    def f_task(x):
        y = f(x)
        params.output_queues.put(y)

    _run_task(f_task, params)

def map(f, stage=_QueueStatus.UNDEFINED, workers=1, maxsize=0):
    """Creates a stage that maps a function `f` over the data. Its intended to behave like
    python's built-in `map` function but with the added concurrency.
    Note that because of concurrency order is not guaranteed.
    Args:
        f: a function with signature `f(x, *args) -> y`, where `args` is the return of `on_start`
            if present, else the signature is just `f(x) -> y`.
        stage: a stage or iterable.
        workers: the number of workers the stage should contain.
        maxsize: the maximum number of objects the stage can hold simultaneously, if set to `0`
            (default) then the stage can grow unbounded.
        on_start: a function with signature `on_start() -> args`, where `args` can be any object
            different than `None` or a tuple of objects. The returned `args` are passed to `f` and `on_done`. This
            function is executed once per worker at the beggining.
        on_done: a function with signature `on_done(stage_status, *args)`, where `args` is the
            return of `on_start` if present, else the signature is just `on_done(stage_status)`, and `stage_status`
            is of type `pypeln.process.StageStatus`. This function is executed once per worker when the worker is done.
    Returns:
        If the `stage` parameters is given then this function returns a new stage, else it returns a `Partial`.
    """
    if stage == _QueueStatus.UNDEFINED:
        return Partial(lambda stage: map(f, stage, workers=workers, maxsize=maxsize))
    stage = _to_stage(stage)
    return _Stage(
        worker_constructor=mp.Process,
        workers=workers,
        maxsize=maxsize,
        target=_map,
        args=(f, ),
        dependencies=[stage],
    )

def _filter(f, params):
    @_handle_exceptions(params)
    def f_task(x):
        if f(x):
            params.output_queues.put(x)

    _run_task(f_task, params)

def filter(f, stage=_QueueStatus.UNDEFINED, workers=1, maxsize=0):
    """
    Creates a stage that filter the data given a predicate function `f`. It is intended to behave
    like python's built-in `filter` function but with the added concurrency.
    Note that because of concurrency order is not guaranteed.
    Args:
        f: a function with signature `f(x, *args) -> bool`, where `args` is the return of `on_start`
            if present, else the signature is just `f(x)`.
        stage: a stage or iterable.
        workers: the number of workers the stage should contain.
        maxsize: the maximum number of objects the stage can hold simultaneously, if set to 0
            then the stage can grow unbounded.
        on_start: a function with signature `on_start() -> args`, where `args` can be any object different
            than `None` or a tuple of objects. The returned `args` are passed to `f` and `on_done`.
            This function is executed once per worker at the beggining.
        on_done: a function with signature `on_done(stage_status, *args)`, where `args` is the return of
            `on_start` if present, else the signature is just `on_done(stage_status)`, and `stage_status`
            is of type `pypeln.process.StageStatus`. This function is executed once per worker when the worker is done.
    Returns:
        If the `stage` parameters is given then this function returns a new stage, else it returns a `Partial`.
    """
    if stage == _QueueStatus.UNDEFINED:
        return Partial(lambda stage: filter(
            f, stage, workers=workers, maxsize=maxsize))
    stage = _to_stage(stage)
    return _Stage(
        worker_constructor=mp.Process,
        workers=workers,
        maxsize=maxsize,
        target=_filter,
        args=(f, ),
        dependencies=[stage],
    )

def _concat(params):
    def f_task(x):
        params.output_queues.put(x)
    _run_task(f_task, params)

def concat(stages, maxsize=0):
    """Concatenates / merges many stages into a single one by appending elements from each stage as they come,
    order is not preserved.
    Args:
        stages: a list of stages or iterables.
        maxsize: the maximum number of objects the stage can hold simultaneously, if set to `0` (default) then the stage
            can grow unbounded.
    Returns:
        A stage object.
    """
    stages = [_to_stage(s) for s in stages]
    return _Stage(
        worker_constructor=mp.Process,
        workers=1,
        maxsize=maxsize,
        target=_concat,
        args=tuple(),
        dependencies=stages,
    )

def _to_stage(obj):
    if isinstance(obj, _Stage):
        return obj
    elif hasattr(obj, "__iter__"):
        return from_iterable(obj)
    else:
        raise ValueError("Object {obj} is not iterable".format(obj=obj))

def _from_iterable(iterable, params):
    def f_task():
        for x in iterable:
            params.output_queues.put(x)
    _run_task(f_task, params)

def from_iterable(iterable=_QueueStatus.UNDEFINED, maxsize=None):
    """
    Creates a stage from an iterable. All functions that accept stages or iterables use this function
    when an iterable is passed to convert it into a stage using the default arguments.
    Args:
        iterable: a source iterable.
        maxsize: this parameter is not used and only kept for API compatibility with the other modules.
        worker_constructor: defines the worker type for the producer stage.
    Returns:
        If the `iterable` parameters is given then this function returns a new stage, else it returns a `Partial`.
    """
    if iterable == _QueueStatus.UNDEFINED:
        return Partial(lambda iterable: from_iterable(iterable, maxsize=maxsize))
    return _Stage(
        worker_constructor=mp.Process,
        workers=1,
        maxsize=None,
        target=_from_iterable,
        args=(iterable, ),
        dependencies=[],
    )

_InputQueueDict = Dict[_Stage, _InputQueue]
_OutputQueueDict = Dict[_Stage, _OutputQueueList]

def _build_queues(stage: _Stage, stage_input_queue: _InputQueueDict,
                  stage_output_queues: _OutputQueueDict, visited: Set[_Stage],
                  pipeline_namespace: Namespace) -> Tuple[_InputQueueDict, _OutputQueueDict]:
    if stage in visited:
        return stage_input_queue, stage_output_queues
    else:
        visited.add(stage)
    if len(stage.dependencies) > 0:
        total_done = sum([s.workers for s in stage.dependencies])
        input_queue = _InputQueue(stage.maxsize, total_done, pipeline_namespace)
        stage_input_queue[stage] = input_queue
        for _stage in stage.dependencies:
            if _stage not in stage_output_queues:
                stage_output_queues[_stage] = _OutputQueueList([input_queue])
            else:
                stage_output_queues[_stage].append(input_queue)
            stage_input_queue, stage_output_queues = _build_queues(
                _stage,
                stage_input_queue,
                stage_output_queues,
                visited,
                pipeline_namespace=pipeline_namespace,
            )
    return stage_input_queue, stage_output_queues

def _to_iterable(stage, maxsize):
    pipeline_namespace = _MANAGER.Namespace()
    pipeline_namespace.error = False  # type: ignore
    pipeline_error_queue: mp.Queue = mp.Queue()
    input_queue = _InputQueue(maxsize, stage.workers, pipeline_namespace)
    stage_input_queue, stage_output_queues = _build_queues(
        stage=stage,
        stage_input_queue=dict(),
        stage_output_queues=dict(),
        visited=set(),
        pipeline_namespace=pipeline_namespace,
    )
    stage_output_queues[stage] = _OutputQueueList([input_queue])
    processes = []
    for _stage in stage_output_queues:
        for index in range(_stage.workers):
            stage_params = _StageParams(
                output_queues=stage_output_queues[_stage],
                input_queue=stage_input_queue.get(_stage, None),
                pipeline_namespace=pipeline_namespace,
                pipeline_error_queue=pipeline_error_queue,
                index=index,
            )
            process = _stage.worker_constructor(target=_stage.target, args=_stage.args + (stage_params, ))
            processes.append(process)
    for p in processes:
        p.daemon = True
        p.start()
    try:
        for x in input_queue:
            yield x
        if pipeline_namespace.error:  # type: ignore
            error_class, _, trace = pipeline_error_queue.get()
            raise error_class("\n\nOriginal {trace}".format(trace=trace))
        for p in processes:
            p.join()
    except BaseException as e:
        for q in stage_input_queue.values():
            q.done()
        raise e

def to_iterable(stage=_QueueStatus.UNDEFINED, maxsize=0):
    """
    Creates an iterable from a stage. This function is used by the stage's `__iter__` method with the default arguments.

    Args:
        stage: a stage object.
        maxsize: the maximum number of objects the stage can hold simultaneously, if set to `0` (default) then
    the stage can grow unbounded.

    Returns:
        If the `stage` parameters is given then this function returns an iterable, else it returns a `Partial`.
    """

    if stage == _QueueStatus.UNDEFINED:
        return Partial(lambda stage: _to_iterable(stage, maxsize))
    else:
        return _to_iterable(stage, maxsize)
