"""Shared constants, paths and IPv6 helpers for the pool-swap drill.

Time is a *simulated* clock kept as integer seconds since the drill epoch.
Every timestamp stored in SQLite is therefore an integer, never wall-clock
time. Prefixes are stored as text; slot indexes are integers. No candidate
prefix list is ever materialized.
"""
import ipaddress
import os

DB_PATH = os.environ.get("DRILL_DB", "/data/pool_drill.db")

# Simulated drill epoch (display only; all arithmetic uses integer seconds).
EPOCH_LABEL = "2026-11-23T00:00:00Z"
LEASE_SECONDS = 60 * 60  # every new lease is a 60-minute lease
ALLOWED_DELEGATION = (56, 60)
# Guard against pathological allocations while still covering /48 -> /60.
MAX_SLOTS = 1 << 20


def parse_cidr(text):
    """Parse an IPv6 network in CIDR form; host bits must be zero."""
    if text is None or not str(text).strip():
        raise ValueError("CIDR 不能为空")
    try:
        net = ipaddress.IPv6Network(str(text).strip(), strict=True)
    except ValueError as exc:
        raise ValueError("CIDR 格式非法或含主机位（%s）" % exc)
    if net.version != 6:
        raise ValueError("只支持 IPv6 网络")
    return net


def validate_pool(cidr_text, deleg_len):
    """Validate a new pool: valid IPv6 CIDR, deleg length in {56,60}.

    Returns (network, delegation_length, slot_count). Raises ValueError on
    any invalid input so callers can keep the previous state untouched.
    """
    net = parse_cidr(cidr_text)
    if deleg_len not in ALLOWED_DELEGATION:
        raise ValueError("委派长度只允许 /56 或 /60")
    if net.prefixlen > deleg_len:
        raise ValueError("委派长度不能短于池 CIDR 前缀长度")
    slots = 1 << (deleg_len - net.prefixlen)
    if slots > MAX_SLOTS:
        raise ValueError("可委派前缀数量超过上限 %d" % MAX_SLOTS)
    return net, deleg_len, slots


def slot_prefix(network, deleg_len, slot):
    """Return the delegated prefix text for slot without expanding a range."""
    base = int(network.network_address)
    size = 1 << (network.max_prefixlen - deleg_len)
    return str(ipaddress.IPv6Network((base + slot * size, deleg_len), strict=False))


def is_covered(lease_prefix_text, new_pool_network):
    """True when the delegated lease prefix is fully inside the new pool."""
    pfx = ipaddress.IPv6Network(lease_prefix_text, strict=False)
    return pfx.subnet_of(new_pool_network)
