import cffi
from collections import namedtuple
from multiprocessing.pool import Pool
import os
import secrets
import threading
import time

from cffi_util import read_stripped_header


SELF_DIR = os.path.dirname(os.path.abspath(__file__))
TOPLEVEL = os.path.abspath(SELF_DIR + "/../") + "/"

FFI = cffi.FFI()

EXACT_HEADERS = ["bench/exact_test.h"]
for header in EXACT_HEADERS:
    FFI.cdef(read_stripped_header(TOPLEVEL + header))

try:
    EXACT = FFI.dlopen(TOPLEVEL + "/exact.so")
except Exception as e:
    print("Failed to load exact.so: %s" % e)
    EXACT = None


Sample = namedtuple("Sample", ["a_class", "b_class"])


# A statistic has a name, and is defined by the preprocessing for the
# data under the null (probability that values from A is lower than
# that from B [likely not quite what one expects], and offsets to add
# to the u63 values for A and B), by the C statistic computation
# function, and by any additional argument for that function.
Statistic = namedtuple(
    "Statistic",
    ["name", "probability_a_lower", "a_offset", "b_offset", "fn_name", "fn_args"],
)


def actual_data_results(sample, statistics):
    """Computes the actual sample value for all `statistics`, for the
    sample values in `sample.a_class` and `sample.b_class`.
    """
    a = sample.a_class
    b = sample.b_class

    def _make_buf():
        buf = FFI.new("uint64_t[]", len(a) + len(b))
        for i, x in enumerate(a + b):
            buf[i] = x
        return buf

    results = dict()
    m = len(a)
    n = len(b)
    buf = _make_buf()
    total = m + n
    copy = FFI.new("uint64_t[]", total)
    FFI.memmove(copy, buf, total * FFI.sizeof("uint64_t"))

    xoshiro = EXACT.exact_test_prng_create()
    EXACT.exact_test_offset_sort(xoshiro, copy, m, n, 0, 0)
    EXACT.exact_test_prng_destroy(xoshiro)

    for stat in statistics:
        value = getattr(EXACT, stat.fn_name)(copy, m, n, *stat.fn_args)
        results[stat.name] = value
    return results


def _resampled_data_results_1(sample, grouped_statistics):
    """Yields values for all the statistics in `grouped_statistics` after
    shuffling values from `sample.a_class` and `sample.b_class`.
    """

    # Reseed to avoid exploring the same random sequence multiple
    # times when multiprocessing.
    EXACT.exact_test_prng_seed(secrets.randbits(64))

    a = sample.a_class
    b = sample.b_class

    def _make_buf():
        buf = FFI.new("uint64_t[]", len(a) + len(b))
        for i, x in enumerate(a + b):
            buf[i] = x
        return buf

    m = len(a)
    n = len(b)
    buf = _make_buf()
    total = m + n
    shuffled_buf = FFI.new("uint64_t[]", total)
    sorted_buf = FFI.new("uint64_t[]", total)
    error_ptr = FFI.new("char**")
    xoshiro = EXACT.exact_test_prng_create()

    def compute_results():
        results = dict()
        for p_a_lt, stats_for_p in grouped_statistics.items():
            FFI.memmove(shuffled_buf, buf, total * FFI.sizeof("uint64_t"))
            if not EXACT.exact_test_shuffle(
                xoshiro, shuffled_buf, m, n, p_a_lt, error_ptr
            ):
                raise "Shuffle failed: %s" % str(FFI.string(error_ptr[0]), "utf-8")

            for (a_offset, b_offset), stats_for_offset in stats_for_p.items():
                FFI.memmove(sorted_buf, shuffled_buf, total * FFI.sizeof("uint64_t"))
                EXACT.exact_test_offset_sort(
                    xoshiro, sorted_buf, m, n, a_offset, b_offset
                )
                for stat in stats_for_offset:
                    results[stat.name] = getattr(EXACT, stat.fn_name)(
                        sorted_buf, m, n, *stat.fn_args
                    )
        return results

    try:
        while True:
            yield compute_results()
    finally:
        EXACT.exact_test_prng_destroy(xoshiro)


def _generate_in_parallel_worker(generator_fn, generator_args, max_results, max_delay):
    """Toplevel worker for a process pool.  Batches values yielded by
    `generator_fn(*generator_args)` until we have too many values, or
    we hit `max_delay`, and then return that list of values.
    """
    results = []
    end = time.monotonic() + max_delay
    for value in generator_fn(*generator_args):
        results.append(value)
        if len(results) >= max_results or time.monotonic() >= end:
            return results


# At first, return as soon as we have INITIAL_BATCH_SIZE results
INITIAL_BATCH_SIZE = 10
# And let that limit grow up to MAX_BATCH_SIZE
MAX_BATCH_SIZE = 100 * 1000
# Growth rate for the batch size
BATCH_SIZE_GROWTH_FACTOR = 2

# We wait for up to this fraction of the total computation runtime
# before returning values
PROPORTIONAL_DELAY = 0.05

# Wait for at least MIN_DELAY seconds before returning the values we have
MIN_DELAY = 0.01
# And wait for up to MAX_DELAY seconds before returning.
MAX_DELAY = 10

# We lazily create a pool of POOL_SIZE workers.
POOL_SIZE = os.cpu_count() - 1

POOL_LOCK = threading.Lock()
POOL = None


def _get_pool():
    global POOL
    with POOL_LOCK:
        if POOL is None:
            POOL = Pool(POOL_SIZE)
        return POOL


def _generate_in_parallel(generator_fn, generator_args_fn):
    """Merges values yielded by `generator_fn(*generator_args_fn())` in
    arbitrary order.
    """
    # We want multiprocessing to avoid the GIL.  We use relatively
    # coarse-grained futures (instead of a managed queue) to simplify
    # the transition to RPCs.

    # We queue up futures, with up to `max_waiting` not yet running.
    max_waiting = 2
    pending = []

    begin = time.monotonic()
    batch_size = INITIAL_BATCH_SIZE
    pool = _get_pool()

    def generate_values():
        """Calls the generator fn to get new values, while recycling the
        arguments from time to time."""
        while True:
            for i, value in enumerate(generator_fn(*generator_args_fn())):
                if i >= batch_size:
                    break
                yield value

    def consume_completed_futures():
        active = []
        completed = []
        for future in pending:
            if future.ready():
                completed.append(future)
            else:
                active.append(future)
        pending.clear()
        pending.extend(active)
        return [future.get(0) for future in completed]

    # Adds a new work unit to the pending list.
    def add_work_unit():
        delay = PROPORTIONAL_DELAY * (time.monotonic() - begin)
        if delay < MIN_DELAY:
            delay = MIN_DELAY
        if delay > MAX_DELAY:
            delay = MAX_DELAY
        future_results = pool.apply_async(
            _generate_in_parallel_worker,
            (generator_fn, generator_args_fn(), batch_size, delay),
        )
        pending.append(future_results)

    def fill_pending_list():
        for _ in range(POOL_SIZE + max_waiting):
            # Yeah, we're using internals, but this one hasn't
            # changed since 3.5 (or earlier), and I don't know why
            # this value isn't exposed.
            if pool._taskqueue.qsize() >= max_waiting:
                return
            add_work_unit()

    fill_pending_list()
    for value in generate_values():
        yield value
        any_completed = False
        for completed in consume_completed_futures():
            for value in completed:
                yield value
            any_completed = True
        if any_completed:
            batch_size = min(BATCH_SIZE_GROWTH_FACTOR * batch_size, MAX_BATCH_SIZE)
            fill_pending_list()


def resampled_data_results(sample, grouped_statistics_fn):
    """Yields values computed by the Statistics in `grouped_statistics_fn()`
    after reshuffling values from `sample.a_class` and
    `sample.b_class`.
    """
    return _generate_in_parallel(
        _resampled_data_results_1, lambda: (sample, grouped_statistics_fn())
    )
