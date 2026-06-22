"""Юнит-тесты rate limiter с фиксированным окном."""

import app.rate_limit as rate_limit_module
from app.rate_limit import FixedWindowRateLimiter


def test_allows_requests_within_limit():
    rl = FixedWindowRateLimiter(max_requests=3, window_seconds=60)
    assert [rl.check("k")[0] for _ in range(3)] == [True, True, True]


def test_blocks_requests_over_limit():
    rl = FixedWindowRateLimiter(max_requests=3, window_seconds=60)
    for _ in range(3):
        rl.check("k")
    allowed, remaining, retry_after = rl.check("k")
    assert allowed is False
    assert remaining == 0
    assert retry_after > 0


def test_keys_are_independent():
    rl = FixedWindowRateLimiter(max_requests=1, window_seconds=60)
    assert rl.check("a")[0] is True
    assert rl.check("b")[0] is True  # другой клиент, своё окно
    assert rl.check("a")[0] is False  # "a" уже израсходовал свой единственный запрос


def test_window_resets_after_expiry(monkeypatch):
    fake_now = {"t": 1000.0}
    monkeypatch.setattr(rate_limit_module.time, "monotonic", lambda: fake_now["t"])
    rl = FixedWindowRateLimiter(max_requests=1, window_seconds=10)
    assert rl.check("k")[0] is True
    assert rl.check("k")[0] is False
    fake_now["t"] += 11  # окно прошло
    assert rl.check("k")[0] is True


def test_purge_expired_drops_old_windows(monkeypatch):
    fake_now = {"t": 0.0}
    monkeypatch.setattr(rate_limit_module.time, "monotonic", lambda: fake_now["t"])
    rl = FixedWindowRateLimiter(max_requests=5, window_seconds=10)
    rl.check("k")
    fake_now["t"] += 20
    rl.purge_expired()
    assert "k" not in rl._hits
