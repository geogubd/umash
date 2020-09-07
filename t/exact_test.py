from collections import defaultdict, namedtuple
from multiprocessing import Manager
from multiprocessing.pool import Pool
import cffi
import math
import os
import secrets
import sys

from csm import csm
from cffi_util import read_stripped_header

__all__ = [
    "exact_test",
    "lte_prob",
    "gt_prob",
    "mean",
    "quantile",
    "median",
    "q99",
]


# exact_test returns a dict of Statistic name to Result.
# The `actual_value` is the value of the statistic on the sample data
# `m` and `n` are the size of each class
# `judgement` is -1 if the actual value is lower than the resampled ones,
#    1 if higher, 0 if we can't say
# `num_trials` is the number of resampling iterations we needed to find
#    this out.
Result = namedtuple("Result", ["actual_value", "judgement", "m", "n", "num_trials"])


# A statistic has a name, and is defined by the preprocessing for the
# data under the null (probability that values from A is lower than
# that from B [likely not quite what one expects], and offsets to add
# to the u63 values for A and B), by the C statistic computation
# function, and by any additional argument for that function.
Statistic = namedtuple(
    "Statistic",
    ["name", "probability_a_lower", "a_offset", "b_offset", "fn_name", "fn_args"],
)

DEFAULT_STATISTIC = Statistic(None, 0.5, 0, 0, None, ())


def lte_prob(name, p_a_lower=0.5, a_offset=0, b_offset=0):
    """Returns a statistic that computes the probability that a value
    chosen uniformly at random from A is <= a value uniformly chosen from
    B."""
    return DEFAULT_STATISTIC._replace(
        name=name,
        probability_a_lower=p_a_lower,
        a_offset=a_offset,
        b_offset=b_offset,
        fn_name="exact_test_lte_prob",
        fn_args=(),
    )


def gt_prob(name, p_a_lower=0.5, a_offset=0, b_offset=0):
    """Returns a statistic that computes the probability that a value
    chosen uniformly at random from A is > a value uniformly chosen from
    B."""
    return DEFAULT_STATISTIC._replace(
        name=name,
        probability_a_lower=p_a_lower,
        a_offset=a_offset,
        b_offset=b_offset,
        fn_name="exact_test_gt_prob",
        fn_args=(),
    )


def mean(name, truncate_tails=0.0, p_a_lower=0.5, a_offset=0, b_offset=0):
    """Returns a statistic that computes the difference between the
    (potentially truncated) arithmetic means of A and B.

    If truncate_tail > 0, we remove that fraction (rounded up) of the
    observations at both tails.  For example, truncate_tail=0.01 considers
    only the most central 98% of the data points in the mean.
    """
    return DEFAULT_STATISTIC._replace(
        name=name,
        probability_a_lower=p_a_lower,
        a_offset=a_offset,
        b_offset=b_offset,
        fn_name="exact_test_truncated_mean_diff",
        fn_args=(truncate_tails,),
    )


def quantile(name, q, p_a_lower=0.5, a_offset=0, b_offset=0):
    """Returns a Statistic that computes the difference between the qth
    quantile of A and B, where 0 <= q <= 1.
    """
    return DEFAULT_STATISTIC._replace(
        name=name,
        probability_a_lower=p_a_lower,
        a_offset=a_offset,
        b_offset=b_offset,
        fn_name="exact_test_quantile_diff",
        fn_args=(q,),
    )


def median(name, p_a_lower=0.5, a_offset=0, b_offset=0):
    """Returns a Statistic that computes the difference between the
    medians of A and B."""
    return quantile(name, 0.5, p_a_lower, a_offset, b_offset)


def q99(name, p_a_lower=0.5, a_offset=0, b_offset=0):
    """Returns a Statistic that computes the difference between the 99th
    percentile of A and B."""
    return quantile(name, 0.99, p_a_lower, a_offset, b_offset)


# We internally group statistics in order to reuse generated data when
# possible.
def _group_statistics_in_plan(statistics):
    """Groups statistics in a trie, by probability_a_lower, then by
    [ab]_offset.

    This structure reflects the execution order when using exact_test.h."""
    plan = defaultdict(lambda: defaultdict(list))
    for stat in statistics:
        p_a_lower = stat.probability_a_lower
        offsets = (stat.a_offset, stat.b_offset)
        plan[p_a_lower][offsets].append(stat)

    # Convert defaultdicts to regular dicts, for pickling.
    def undefaultdict(x):
        if not isinstance(x, defaultdict):
            return x
        return {k: undefaultdict(v) for k, v in x.items()}

    return undefaultdict(plan)


SELF_DIR = os.path.dirname(os.path.abspath(__file__))
TOPLEVEL = os.path.abspath(SELF_DIR + "/../") + "/"

EXACT_HEADERS = ["bench/exact_test.h"]

FFI = cffi.FFI()


for header in EXACT_HEADERS:
    FFI.cdef(read_stripped_header(TOPLEVEL + header))

try:
    EXACT = FFI.dlopen(TOPLEVEL + "/exact.so")
except Exception as e:
    print("Failed to load exact.so: %s" % e)
    EXACT = None


Sample = namedtuple("Sample", ["a_class", "b_class"])


def _actual_data_results(sample, statistics):
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


def _generate_in_parallel_worker(
    queue,
    generator_fn,
    generator_args,
    initial_batch_size,
    max_batch_size,
    return_after,
):
    """Toplevel worker for a process pool.  Batches values yielded by
    `generator_fn(*generator_args)` and pushes batches to `queue`."""
    batch = []
    # Let the batch size grow linearly to improve responsiveness when
    # we only need a few results to stop the analysis.
    batch_size = initial_batch_size
    total = 0
    for value in generator_fn(*generator_args):
        batch.append(value)
        if len(batch) >= batch_size:
            total += len(batch)
            queue.put(batch)
            if total >= return_after:
                return
            batch = []
            if batch_size < max_batch_size:
                batch_size += 1


def _generate_in_parallel(generator_fn, generator_args_fn, batch_size=None):
    """Merges values yielded by `generator_fn(*generator_args_fn())` in
    arbitrary order.
    """
    ncpu = os.cpu_count()
    # Use a managed queue and multiprocessing to avoid the GIL.
    # Overall, this already seems like a net win at 4 cores, compared
    # to multithreading: we lose some CPU time to IPC and the queue
    # manager process, but less than what we wasted waiting on the GIL
    # (~10-20% on all 4 cores).
    queue = Manager().Queue(maxsize=4 * ncpu)

    # Queue up npu + 2 work units.
    pending = []

    if batch_size is None:
        batch_size = 10 * ncpu

    def generate_values():
        """Calls the generator fn to get new values, while recycling the
        arguments from time to time."""
        while True:
            for i, value in enumerate(generator_fn(*generator_args_fn())):
                if i >= batch_size:
                    break
                yield value

    def get_nowait():
        try:
            return queue.get_nowait()
        except:
            return None

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

    with Pool(ncpu - 1) as pool:
        # Adds a new work unit to the pending list.
        def add_work_unit(initial_batch_size=batch_size, return_after=2 * batch_size):
            pending.append(
                pool.apply_async(
                    _generate_in_parallel_worker,
                    (
                        queue,
                        generator_fn,
                        generator_args_fn(),
                        initial_batch_size,
                        batch_size,
                        return_after,
                    ),
                )
            )

        try:
            # Initial work units ramp up.
            for _ in range(ncpu):
                add_work_unit(0)
            for _ in range(2):
                add_work_unit()
            for value in generate_values():
                # Let work units run for longer without communications
                # when we keep going after the initial batch: we're
                # probably in this for the long run.
                for _ in consume_completed_futures():
                    add_work_unit(return_after=5 * batch_size)
                values = [value]
                while values is not None:
                    yield from values
                    values = get_nowait()
        finally:
            pool.terminate()


def _resampled_data_results(sample, grouped_statistics_fn):
    """Yields values computed by the Statistics in `grouped_statistics_fn()`
    after reshuffling values from `sample.a_class` and
    `sample.b_class`.
    """
    return _generate_in_parallel(
        _resampled_data_results_1, lambda: (sample, grouped_statistics_fn())
    )


ResultAccumulator = namedtuple(
    "ResultAccumulator", ["trials", "lte_actual", "gte_actual"], defaults=[0, 0, 0]
)


def _significance_test(
    ret,
    eps,
    log_inner_eps,
    name,
    monte_carlo_value,
    actual_data,
    actual,
    lte_actual,
    gte_actual,
    trials,
    log,
):
    """Performs a CSM test for `name`, with `lte_actual` Monte Carlo
    values less than or equal to the actual sample value and
    `gte_actual` greater than or equal, over a total of `trials`
    iterations.

    If a statistically significant result is found, writes it to
    `ret`.
    """
    lt_significant, lt_level = csm(trials, eps, lte_actual, log_inner_eps)
    gt_significant, gt_level = csm(trials, eps, gte_actual, log_inner_eps)

    if log:
        print(
            "%i\t%s:\t%i\t%i\t(%f %f / %f)"
            % (
                trials,
                name,
                lte_actual,
                gte_actual,
                max(lt_level, gt_level),
                monte_carlo_value,
                actual,
            ),
            file=log,
        )

    partial_result = Result(
        actual, None, len(actual_data.a_class), len(actual_data.b_class), trials
    )
    count_in_middle = 0
    if lt_significant:
        # We're pretty sure the actual stat is too low to
        # realistically happen under then null
        if lte_actual / trials < eps:
            ret[name] = partial_result._replace(judgement=-1)
            return
        count_in_middle += 1

    if gt_significant:
        # We're pretty sure the actual stat is too high.
        if gte_actual / trials < eps:
            ret[name] = partial_result._replace(judgement=1)
            return
        count_in_middle += 1

    if count_in_middle == 2:
        # We're sure the actual stat isn't too low nor too
        # high for the null.
        ret[name] = partial_result._replace(judgement=0)


def exact_test(
    a, b, statistics, eps=1e-4, log=sys.stderr,
):
    """Performs an exact significance test for every statistic in
    `statistics`, on u63-valued observations in a and b, with false
    positive rate eps.

    Returns a dict of results.  For each statistic, the result will
    have one entry mapping the statistic's name to a Result.
    """

    if not statistics:
        return dict()

    actual_data = Sample(a, b)
    num_stats = len(statistics)
    # Apply a fudged Bonferroni correction for the two-sided quantile
    # test we perform on each statistic.
    eps /= 2 * num_stats * 1.1
    # And use up some of the extra headroom for errors in the inner
    # Bernoulli tests.
    log_inner_eps = math.log(eps / 10)

    actual_stats = _actual_data_results(actual_data, statistics)
    if log:
        print("actual: %s" % actual_stats, file=log)
    accumulators = dict()
    for stat_name in actual_stats:
        accumulators[stat_name] = ResultAccumulator()

    ret = dict()

    def group_unfathomed_statistics():
        return _group_statistics_in_plan(
            [stat for stat in statistics if stat.name not in ret]
        )

    seen = 0
    test_every = 250
    for sample in _resampled_data_results(actual_data, group_unfathomed_statistics):
        for name, stat in sample.items():
            actual = actual_stats[name]
            current = accumulators[name]
            if stat <= actual:
                current = current._replace(lte_actual=current.lte_actual + 1)
            if stat >= actual:
                current = current._replace(gte_actual=current.gte_actual + 1)

            accumulators[name] = current._replace(trials=current.trials + 1)

        seen += 1
        if (seen % test_every) != 0:
            continue

        if seen >= 40 * test_every:
            test_every *= 10
        for name, acc in accumulators.items():
            if name in ret:  # We already have a result -> skip
                continue
            _significance_test(
                ret,
                eps,
                log_inner_eps,
                name,
                sample[name],
                actual_data,
                actual_stats[name],
                acc.lte_actual,
                acc.gte_actual,
                acc.trials,
                log,
            )
        if len(ret) == len(actual_stats):
            return {name: ret[name] for name in actual_stats}
