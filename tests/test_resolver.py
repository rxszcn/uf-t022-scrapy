from __future__ import annotations

from unittest.mock import Mock

import pytest
from twisted.internet import defer
from twisted.internet.address import IPv4Address, IPv6Address
from twisted.internet.base import ThreadedResolver
from twisted.internet.error import DNSLookupError

from scrapy.resolver import CachingHostnameResolver, CachingThreadedResolver, dnscache
from scrapy.utils.defer import maybe_deferred_to_future
from scrapy.utils.misc import build_from_crawler
from scrapy.utils.test import get_crawler
from tests.utils.decorators import coroutine_test


@pytest.fixture(autouse=True)
def reset_dnscache():
    original_limit = dnscache.limit
    original_ttl = dnscache.ttl
    dnscache.clear()
    yield
    dnscache.clear()
    dnscache.limit = original_limit
    dnscache.ttl = original_ttl


def test_caching_threaded_resolver_dnscache_disabled():
    crawler = get_crawler(settings_dict={"DNSCACHE_ENABLED": False})
    build_from_crawler(CachingThreadedResolver, crawler, Mock())
    assert dnscache.limit == 0


def test_caching_threaded_resolver_from_crawler_sets_ttl():
    crawler = get_crawler(settings_dict={"DNSCACHE_TTL": 30})
    build_from_crawler(CachingThreadedResolver, crawler, Mock())
    assert dnscache.ttl == 30


@coroutine_test
async def test_caching_threaded_resolver_getHostByName_cache_hit():
    resolver = CachingThreadedResolver(Mock(), cache_size=10, timeout=5.0)
    dnscache["example.com"] = "1.2.3.4"

    result = await maybe_deferred_to_future(resolver.getHostByName("example.com"))
    assert result == "1.2.3.4"


@coroutine_test
async def test_caching_threaded_resolver_cache_entry_expires(monkeypatch):
    now = [1000.0]
    monkeypatch.setattr("scrapy.resolver.monotonic", lambda: now[0])
    underlying = Mock(side_effect=lambda *_: defer.succeed("1.2.3.4"))
    monkeypatch.setattr(ThreadedResolver, "getHostByName", underlying)
    resolver = CachingThreadedResolver(Mock(), cache_size=10, timeout=5.0, cache_ttl=60)

    result = await maybe_deferred_to_future(resolver.getHostByName("example.com"))
    assert result == "1.2.3.4"
    assert "example.com" in dnscache
    await maybe_deferred_to_future(resolver.getHostByName("example.com"))
    assert underlying.call_count == 1

    now[0] += 61
    assert "example.com" not in dnscache
    result = await maybe_deferred_to_future(resolver.getHostByName("example.com"))
    assert result == "1.2.3.4"
    assert underlying.call_count == 2


@coroutine_test
async def test_caching_threaded_resolver_failed_resolution_is_cached(monkeypatch):
    now = [1000.0]
    monkeypatch.setattr("scrapy.resolver.monotonic", lambda: now[0])
    underlying = Mock(
        side_effect=[
            defer.fail(DNSLookupError("DNS lookup failed")),
            defer.succeed("1.2.3.4"),
        ]
    )
    monkeypatch.setattr(ThreadedResolver, "getHostByName", underlying)
    resolver = CachingThreadedResolver(Mock(), cache_size=10, timeout=5.0, cache_ttl=60)

    with pytest.raises(DNSLookupError):
        await maybe_deferred_to_future(resolver.getHostByName("example.com"))
    # The cached failure is re-raised without resolving again.
    with pytest.raises(DNSLookupError):
        await maybe_deferred_to_future(resolver.getHostByName("example.com"))
    assert underlying.call_count == 1

    now[0] += 61
    result = await maybe_deferred_to_future(resolver.getHostByName("example.com"))
    assert result == "1.2.3.4"
    assert underlying.call_count == 2


@coroutine_test
async def test_caching_threaded_resolver_concurrent_resolution_coalesced(monkeypatch):
    pending: defer.Deferred[str] = defer.Deferred()
    underlying = Mock(return_value=pending)
    monkeypatch.setattr(ThreadedResolver, "getHostByName", underlying)
    resolver = CachingThreadedResolver(Mock(), cache_size=10, timeout=5.0)

    d1 = resolver.getHostByName("example.com")
    d2 = resolver.getHostByName("example.com")
    assert underlying.call_count == 1

    pending.callback("1.2.3.4")
    assert await maybe_deferred_to_future(d1) == "1.2.3.4"
    assert await maybe_deferred_to_future(d2) == "1.2.3.4"
    assert dnscache["example.com"] == "1.2.3.4"


@pytest.mark.parametrize("succeed", [True, False])
@coroutine_test
async def test_caching_threaded_resolver_coalesced_cancelled_waiter(
    monkeypatch, succeed
):
    pending: defer.Deferred[str] = defer.Deferred()
    underlying = Mock(return_value=pending)
    monkeypatch.setattr(ThreadedResolver, "getHostByName", underlying)
    resolver = CachingThreadedResolver(Mock(), cache_size=10, timeout=5.0)

    d1 = resolver.getHostByName("example.com")
    d2 = resolver.getHostByName("example.com")
    d1.cancel()
    d1.addErrback(lambda _: None)  # silence the cancelled waiter

    if succeed:
        pending.callback("1.2.3.4")
        assert await maybe_deferred_to_future(d2) == "1.2.3.4"
    else:
        pending.errback(DNSLookupError("DNS lookup failed"))
        with pytest.raises(DNSLookupError):
            await maybe_deferred_to_future(d2)


def test_caching_hostname_resolver_dnscache_disabled():
    crawler = get_crawler(settings_dict={"DNSCACHE_ENABLED": False})
    build_from_crawler(CachingHostnameResolver, crawler, Mock())
    assert dnscache.limit == 0


def test_caching_hostname_resolver_from_crawler_sets_ttl():
    crawler = get_crawler(settings_dict={"DNSCACHE_TTL": 30})
    build_from_crawler(CachingHostnameResolver, crawler, Mock())
    assert dnscache.ttl == 30


def test_caching_hostname_resolver_failed_resolution_is_cached():
    def fake_resolve(receiver, *_):
        receiver.resolutionBegan(Mock())
        receiver.resolutionComplete()
        return receiver

    reactor = Mock()
    reactor.nameResolver.resolveHostName.side_effect = fake_resolve

    resolver = CachingHostnameResolver(reactor, cache_size=10)
    resolver.resolveHostName(Mock(), "example.com")

    # A failed resolution is cached, so resolving the same name again
    # completes immediately without involving the original resolver.
    receiver = Mock()
    resolver.resolveHostName(receiver, "example.com")
    assert reactor.nameResolver.resolveHostName.call_count == 1
    receiver.resolutionBegan.assert_called_once()
    receiver.addressResolved.assert_not_called()
    receiver.resolutionComplete.assert_called_once()


def test_caching_hostname_resolver_concurrent_resolution_coalesced():
    reactor = Mock()
    resolver = CachingHostnameResolver(reactor, cache_size=10)

    receiver1 = Mock()
    receiver2 = Mock()
    resolver.resolveHostName(receiver1, "example.com", portNumber=80)
    resolver.resolveHostName(receiver2, "example.com", portNumber=443)
    assert reactor.nameResolver.resolveHostName.call_count == 1

    # Both receivers get the result of the shared resolution, each with the
    # port it asked for.
    receiving = reactor.nameResolver.resolveHostName.call_args.args[0]
    resolution = Mock()
    receiving.resolutionBegan(resolution)
    receiving.addressResolved(IPv4Address("TCP", "1.2.3.4", 80))
    receiving.resolutionComplete()

    receiver1.resolutionBegan.assert_called_once_with(resolution)
    receiver2.resolutionBegan.assert_called_once_with(resolution)
    assert [call.args[0] for call in receiver1.addressResolved.call_args_list] == [
        IPv4Address("TCP", "1.2.3.4", 80)
    ]
    assert [call.args[0] for call in receiver2.addressResolved.call_args_list] == [
        IPv4Address("TCP", "1.2.3.4", 443)
    ]
    receiver1.resolutionComplete.assert_called_once()
    receiver2.resolutionComplete.assert_called_once()
    assert [address.port for address in dnscache["example.com"]] == [0]

    # Once the shared resolution completes, its result is cached.
    receiver3 = Mock()
    resolver.resolveHostName(receiver3, "example.com", portNumber=8080)
    assert reactor.nameResolver.resolveHostName.call_count == 1
    assert [call.args[0] for call in receiver3.addressResolved.call_args_list] == [
        IPv4Address("TCP", "1.2.3.4", 8080)
    ]


def test_caching_hostname_resolver_resolution_start_failure():
    reactor = Mock()
    reactor.nameResolver.resolveHostName.side_effect = RuntimeError("boom")

    resolver = CachingHostnameResolver(reactor, cache_size=10)
    with pytest.raises(RuntimeError, match="boom"):
        resolver.resolveHostName(Mock(), "example.com")
    # The failed resolution does not stay in flight, so the next resolution
    # for the same name starts over instead of joining it.
    with pytest.raises(RuntimeError, match="boom"):
        resolver.resolveHostName(Mock(), "example.com")
    assert reactor.nameResolver.resolveHostName.call_count == 2


def test_caching_hostname_resolver_cached_addresses_have_no_port():
    def fake_resolve(receiver, *_):
        receiver.resolutionBegan(Mock())
        receiver.addressResolved(IPv4Address("TCP", "1.2.3.4", 80))
        receiver.addressResolved(IPv6Address("TCP", "::1", 80))
        receiver.resolutionComplete()
        return receiver

    reactor = Mock()
    reactor.nameResolver.resolveHostName.side_effect = fake_resolve

    receiver = Mock()
    resolver = CachingHostnameResolver(reactor, cache_size=10)
    resolver.resolveHostName(receiver, "example.com", portNumber=80)

    # The port requested on a cache miss is passed through unchanged, but it is
    # not part of what gets cached.
    resolved_ports = [
        call.args[0].port for call in receiver.addressResolved.call_args_list
    ]
    assert resolved_ports == [80, 80]
    assert [address.port for address in dnscache["example.com"]] == [0, 0]


def test_caching_hostname_resolver_cache_hit_without_port():
    cached_addresses = [
        IPv4Address("TCP", "1.2.3.4", 0),
        IPv6Address("TCP", "::1", 0),
    ]
    dnscache["example.com"] = cached_addresses

    receiver = Mock()
    resolver = CachingHostnameResolver(Mock(), cache_size=10)
    resolver.resolveHostName(receiver, "example.com")

    # Cached addresses already use the requested port, so they are reused as is.
    resolved_addresses = [
        call.args[0] for call in receiver.addressResolved.call_args_list
    ]
    assert all(
        resolved is cached
        for resolved, cached in zip(resolved_addresses, cached_addresses, strict=True)
    )


def test_caching_hostname_resolver_cache_hit_uses_requested_port():
    dnscache["example.com"] = [
        IPv4Address("TCP", "1.2.3.4", 0),
        IPv6Address("TCP", "::1", 0),
    ]

    receiver = Mock()
    resolver = CachingHostnameResolver(Mock(), cache_size=10)
    resolver.resolveHostName(receiver, "example.com", portNumber=443)

    resolved_addresses = [
        call.args[0] for call in receiver.addressResolved.call_args_list
    ]
    assert resolved_addresses == [
        IPv4Address("TCP", "1.2.3.4", 443),
        IPv6Address("TCP", "::1", 443),
    ]
    # The cached addresses must not be mutated in place.
    assert [address.port for address in dnscache["example.com"]] == [0, 0]


def test_caching_hostname_resolver_dnscache_disabled_rejects_storage():

    def fake_resolve(receiver, *_):
        receiver.resolutionBegan(Mock())
        receiver.addressResolved(IPv4Address("TCP", "1.2.3.4", 80))
        receiver.resolutionComplete()
        return receiver

    reactor = Mock()
    reactor.nameResolver.resolveHostName.side_effect = fake_resolve

    resolver = CachingHostnameResolver(reactor, cache_size=0)
    resolver.resolveHostName(Mock(), "example.com")

    assert "example.com" not in dnscache
    assert len(dnscache) == 0
