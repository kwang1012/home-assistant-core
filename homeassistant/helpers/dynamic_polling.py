"""Dynamic polling helper."""

from logging import getLogger
import math

import numdifftools
import numpy as np
import scipy.stats as st

LOGGER = getLogger(__name__)

dist_list = [
    "alpha",
    "anglit",
    "arcsine",
    "argus",
    "beta",
    "betaprime",
    "bradford",
    "burr",
    "burr12",
    "cauchy",
    "chi",
    "chi2",
    "cosine",
    "crystalball",
    "dgamma",
    "dweibull",
    "erlang",
    "expon",
    "exponnorm",
    "exponweib",
    "exponpow",
    "f",
    "fatiguelife",
    "fisk",
    "foldcauchy",
    "foldnorm",
    "genlogistic",
    "gennorm",
    "genpareto",
    "genexpon",
    "genextreme",
    "gausshyper",
    "gamma",
    "gengamma",
    "genhalflogistic",
    "genhyperbolic",
    "geninvgauss",
    "gibrat",
    "gompertz",
    "gumbel_r",
    "gumbel_l",
    "halfcauchy",
    "halflogistic",
    "halfnorm",
    "halfgennorm",
    "hypsecant",
    "invgamma",
    "invgauss",
    "invweibull",
    "jf_skew_t",
    "johnsonsb",
    "johnsonsu",
    "kappa4",
    "kappa3",
    "ksone",
    "kstwo",
    "kstwobign",
    "laplace",
    "laplace_asymmetric",
    "levy",
    "levy_l",
    "levy_stable",
    "logistic",
    "loggamma",
    "loglaplace",
    "lognorm",
    "loguniform",
    "lomax",
    "maxwell",
    "mielke",
    "moyal",
    "nakagami",
    "ncx2",
    "ncf",
    "nct",
    "norm",
    "norminvgauss",
    "pareto",
    "pearson3",
    "powerlaw",
    "powerlognorm",
    "powernorm",
    "rdist",
    "rayleigh",
    "rel_breitwigner",
    "rice",
    "recipinvgauss",
    "semicircular",
    "skewcauchy",
    "skewnorm",
    "studentized_range",
    "t",
    "trapezoid",
    "triang",
    "truncexpon",
    "truncnorm",
    "truncpareto",
    "truncweibull_min",
    "tukeylambda",
    "uniform",
    "vonmises",
    "vonmises_line",
    "wald",
    "weibull_min",
    "weibull_max",
    "wrapcauchy",
]


def get_best_distribution(data: list[float]) -> st.rv_continuous:
    """Get distribution based on p value."""
    if len(set(data)) == 1:
        return st.uniform(0, data[0])
    dist_names = [
        "uniform",
        "norm",
        "gamma",
        "genlogistic",
    ]
    # dist_names = dist_list
    dist_results = []
    params = {}
    for dist_name in dist_names:
        dist: st.rv_continuous = getattr(st, dist_name)
        param = dist.fit(data)

        params[dist_name] = param
        # Applying the Kolmogorov-Smirnov test. Pass the frozen distribution's
        # cdf directly rather than `dist_name` + `args=param`: scipy's kstest
        # fast-paths some well-known distribution names (e.g. "norm") to raw
        # C functions like scipy.special.ndtr, which don't accept loc/scale
        # as extra positional args and raise
        # `TypeError: ndtr() takes from 1 to 2 positional arguments but 3
        # were given` on newer scipy. The frozen .cdf callable is
        # equivalent and bypasses that fast path.
        _, p = st.kstest(data, dist(*param).cdf)
        dist_results.append((dist_name, p))

    # select the best fitted distribution
    best_dist, _ = max(dist_results, key=lambda item: item[1])
    # store the name of the best fit and its p value

    LOGGER.debug(
        "Best fitting distribution: %s%s", str(best_dist), str(params[best_dist])
    )
    # print("Best p value: " + str(best_p))
    # print(params[best_dist])

    return getattr(st, best_dist)(*params[best_dist])


def _get_polling_interval(
    dist: st.rv_continuous, num_poll: int, upper_bound: float
) -> list[float]:
    return _get_polling_interval_r(dist, num_poll, upper_bound, 0, upper_bound)


def _get_polling_interval_r(
    dist: st.rv_continuous,
    num_poll: int,
    upper_bound: float,
    left: float,
    right: float,
) -> list[float]:
    def integral(x: float, y: float) -> float:
        return dist.cdf(y) - dist.cdf(x)  # type: ignore[no-any-return]

    L = [0.0 for _ in range(num_poll + 1)]
    # L0 is 0
    # randomized L1
    L[0] = 0.0
    L[1] = (left + right) / 2
    if left == right:
        raise ValueError("left == right")
    too_large = -1
    for n in range(2, num_poll + 1):
        if dist.pdf(L[n - 1]) == 0 and dist.cdf(L[n - 1]) == 0:
            break
        L[n] = 1 / dist.pdf(L[n - 1]) * (integral(L[n - 2], L[n - 1])) + L[n - 1]
        if L[n] > upper_bound:
            too_large = n
            break

    if np.isclose(L[num_poll], upper_bound):
        L[num_poll] = upper_bound
        return L[1:]

    # L1 is too large
    if too_large != -1:
        return _get_polling_interval_r(
            dist,
            num_poll,
            upper_bound,
            left,
            L[1],
        )

    # L1 is too small
    return _get_polling_interval_r(
        dist,
        num_poll,
        upper_bound,
        L[1],
        right,
    )


def _examine_2nd_derivate(dist: st.rv_continuous, L: list[float]) -> bool:
    # examine 2nd derivative
    pdf_prime = numdifftools.Derivative(dist.pdf)
    for i, _ in enumerate(L):
        if i == len(L) - 1:
            return True
        val = 2 * dist.pdf(L[i]) - (L[i + 1] - L[i]) * pdf_prime(L[i])
        if val <= 0:
            return False
    return True


def _examine_Q(dist: st.rv_continuous, L: list[float]) -> float:
    L = [0, *L]
    Q = 0
    for i in range(1, len(L)):
        Q += L[i] * (dist.cdf(L[i]) - dist.cdf(L[i - 1]))
    Q -= dist.expect(lambda x: x, lb=0.0, ub=L[-1])
    # print(f"Q = {round(Q, 2)}, k = {len(L)-1}")
    return Q  # type: ignore[no-any-return]


def _examine_delta(
    dist: st.rv_continuous, L: list[float], delta: float, SLO: float = 0.95
) -> bool:
    L = [0, *L]
    prob = 0
    for i in range(1, len(L)):
        prob += dist.cdf(L[i]) - dist.cdf(max(L[i - 1], L[i] - delta))

    return prob >= float(SLO - dist.cdf(0))


def _cumtrapz(y: np.ndarray, x: np.ndarray) -> np.ndarray:
    """Cumulative integral using the trapezoidal rule with y[0]=0 convention for integral at x[0]."""
    # integral from x[0] to x[i]
    out = np.zeros_like(y)
    out[1:] = np.cumsum(0.5 * (y[1:] + y[:-1]) * (x[1:] - x[:-1]))
    return out


def _get_vopt_interval(
    dist: st.rv_continuous,
    num_poll: int,
    upper_bound: float,  # number of polls (the last one is at U)
    use_dist_cdf: bool = False,  # if True, use dist.cdf for F instead of numeric integration
) -> list:
    """Generalized V-optimal segmentation for polling.

    Returns (poll_times, expected_delay). poll_times has length k with the last exactly U.
    """

    N = int(100 * upper_bound)
    # 1) Grid
    x = np.linspace(0.0, upper_bound, N)
    f = dist.pdf(x)

    # 2) Condition on [0, U] so that F(U) = 1 (finite-horizon assumption)
    #    If you *know* the event occurs by U with prob 1 already, this is a no-op.
    mass = np.trapezoid(f, x)
    if mass <= 0:
        raise ValueError("PDF integrates to ~0 on [0,U]. Check U or dist.")
    f = f / mass

    # 3) CDF F and first-moment cumulative M on the grid
    if use_dist_cdf and hasattr(dist, "cdf"):
        F = dist.cdf(x)
        # Renormalize CDF to [0,U] as well:
        F = (F - F[0]) / (F[-1] - F[0])  # guard if dist.cdf(0)>0
    else:
        F = _cumtrapz(f, x)
        F /= F[-1]  # numeric conditioning (should already be 1)

    tf = x * f
    M = _cumtrapz(tf, x)

    # 4) Precompute O(1) interval costs via cumulative arrays:
    #    C(i,j) = x[j]*(F[j]-F[i]) - (M[j]-M[i])
    # We'll implement the DP to compute these on the fly with F and M.

    # DP arrays
    INF = 1e300
    # dp[m, j] = min cost to end at grid index j using m segments (m polls);
    # we need exactly k segments and must end at j = N-1 (x = U).
    dp = np.full((num_poll + 1, N), INF)
    prv = np.full((num_poll + 1, N), -1, dtype=int)

    # Base: 0 segments at position 0 has zero cost
    dp[0, 0] = 0.0

    # Transition:
    # For m in 1..k:
    #   dp[m, j] = min over i<j { dp[m-1, i] + C(i, j) }
    # where C(i,j) uses the formula with F and M.
    for m in range(1, num_poll + 1):
        # j must be large enough to allow m segments from 0..j
        for j in range(m, N):  # at least m cuts to reach j
            # cost to make the last segment from i -> j
            # We can prune i range if needed for speed; here we do full loop.
            best_cost = INF
            best_i = -1
            Fj, Mj, xj = F[j], M[j], x[j]
            for i in range(m - 1, j):
                # segment [i -> j]
                seg_prob = Fj - F[i]
                seg_m1 = Mj - M[i]
                C_ij = xj * seg_prob - seg_m1
                cand = dp[m - 1, i] + C_ij
                if cand < best_cost:
                    best_cost = cand
                    best_i = i
            dp[m, j] = best_cost
            prv[m, j] = best_i

    # We must end at j = N-1 (x = U) with exactly num_poll segments
    if not np.isfinite(dp[num_poll, N - 1]):
        raise RuntimeError(
            "DP failed to find a valid segmentation. Increase N or check dist."
        )

    # 5) Backtrack to recover cut indices
    cuts = []
    m, j = num_poll, N - 1
    while m > 0:
        i = prv[m, j]
        if i < 0:
            raise RuntimeError("Backtrack failed.")
        cuts.append(j)
        j = i
        m -= 1
    cuts.append(0)
    cuts.reverse()  # indices from 0 to N-1

    # 6) Poll times are the *right ends* of the segments (excluding the first 0)
    #    That gives exactly k polls, with the last at U.
    poll_indices = cuts[1:]  # drop the initial 0
    polls = x[np.array(poll_indices)]

    return polls.tolist()  # type: ignore[no-any-return]


def get_detection_time(dist: st.rv_continuous, L: list[float]) -> float:
    """Get detection time of L."""
    return _examine_Q(dist, L)


def get_polls(
    dist: st.rv_continuous,
    *,
    upper_bound: float | None = None,
    worst_case_delta: float = 2.0,
    use_vopt: bool | None = False,
    SLO: float = 0.95,
    name: str = "_",
    N: int | None = None,
) -> list[float]:
    """Get polls based on distribution."""
    upper_bound = upper_bound or float(dist.ppf(0.99))
    if math.isnan(upper_bound):
        return [0.0]
    if N is not None:
        if use_vopt:
            return _get_vopt_interval(dist, N, upper_bound)

        L = _get_polling_interval(dist, N, upper_bound)
        valid = _examine_2nd_derivate(dist, L)
        if not valid:
            LOGGER.debug("The result for %s is probably not minimized", name)

        return L

    return _r_get_polls(
        dist,
        upper_bound,
        0,
        math.ceil(upper_bound / worst_case_delta),
        -1,
        worst_case_delta,
        SLO,
        name,
        use_vopt,
    )


def _r_get_polls(
    dist: st.rv_continuous,
    upper_bound: float,
    left_N: int,
    right_N: int,
    last_N: int,
    worst_case_delta: float = 2.0,
    SLO: float = 0.95,
    name: str = "_",
    use_vopt: bool | None = False,
) -> list[float]:
    N = max(1, math.floor((left_N + right_N) / 2))
    try:
        if use_vopt:
            L = _get_vopt_interval(dist, N, upper_bound)
            valid = True
        else:
            L = _get_polling_interval(dist, N, upper_bound)
            valid = _examine_2nd_derivate(dist, L)
            if not valid:
                print("The result for", name, "is probably not minimized.")  # noqa: T201
    except ValueError:
        return _r_get_polls(
            dist, upper_bound, left_N, N + 1, N, worst_case_delta, SLO, name, use_vopt
        )

    # _examine_Q(dist, L, SLO)
    valid = _examine_delta(dist, L, worst_case_delta, SLO)

    if left_N == right_N or last_N == N:
        if not valid:
            raise ValueError("Failed to find the polls")
        return L

    if valid:
        # want to further reduce polls
        return _r_get_polls(
            dist, upper_bound, left_N, N + 1, N, worst_case_delta, SLO, name, use_vopt
        )
    if right_N <= N + 1:
        return _r_get_polls(
            dist,
            upper_bound,
            N + 1,
            right_N * 2,
            N,
            worst_case_delta,
            SLO,
            name,
            use_vopt,
        )
    return _r_get_polls(
        dist, upper_bound, N + 1, right_N, N, worst_case_delta, SLO, name, use_vopt
    )


def get_uniform_polls(
    upper_bound: float, *, N: int | None = None, worst_case_delta: float = 2.0
) -> list[float]:
    """Get uniform polls."""
    if N is not None:
        return [(i + 1) * upper_bound / N for i in range(N)]
    if upper_bound < worst_case_delta:
        return [upper_bound]
    polls = [
        (i + 1) * worst_case_delta
        for i in range(math.floor(upper_bound / worst_case_delta))
    ]
    if not np.isclose(polls[-1], upper_bound):
        polls.append(upper_bound)
    return polls
