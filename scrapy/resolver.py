from __future__ import annotations

from time import monotonic
from typing import TYPE_CHECKING, Any, Self

import attr
from twisted.internet import defer
from twisted.internet.base import ReactorBase, ThreadedResolver
from twisted.internet.error import DNSLookupError
from twisted.internet.interfaces import (
    IAddress,
    IHostnameResolver,
    IHostResolution,
    IResolutionReceiver,
    IResolverSimple,
)
from twisted.python.failure import Failure
from zope.interface.declarations import implementer, provider

from scrapy.utils._datatypes import LocalCache

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from twisted.internet.defer import Deferred

    from scrapy.crawler import Crawler


class _NegativeCacheEntry:
    """A cached name resolution failure."""

    def __init__(self, error: BaseException):
        self.error: BaseException = error


class _ExpiringCache(LocalCache[str, Any]):
    """A :class:`LocalCache` whose entries expire some time after they are
    set."""

    def __init__(self, limit: int | None = None):
        super().__init__(limit)
        # Seconds that an entry stays valid, 0 means no expiration.
        self.ttl: float = 0

    def __setitem__(self, key: str, value: Any) -> None:
        expires = monotonic() + self.ttl if self.ttl else None
        super().__setitem__(key, (value, expires))

    def __getitem__(self, key: str) -> Any:
        value, expires = super().__getitem__(key)
        if expires is not None and expires <= monotonic():
            super().__delitem__(key)
            raise KeyError(key)
        return value

    def __contains__(self, key: str) -> bool:  # type: ignore[override]
        try:
            self[key]
        except KeyError:
            return False
        return True

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default


dnscache: _ExpiringCache = _ExpiringCache(10000)


@implementer(IResolverSimple)
class CachingThreadedResolver(ThreadedResolver):
    """
    Default caching resolver. IPv4 only, supports setting a timeout value for DNS requests.
    """

    def __init__(
        self,
        reactor: ReactorBase,
        cache_size: int,
        timeout: float,
        cache_ttl: float = 0,
    ):
        super().__init__(reactor)
        dnscache.limit = cache_size
        dnscache.ttl = cache_ttl
        self.timeout = timeout
        # Names with a resolution in flight, mapped to the deferreds waiting
        # for it, so that concurrent requests for the same name share a
        # single resolution.
        self._resolutions: dict[str, list[Deferred[str]]] = {}

    @classmethod
    def from_crawler(cls, crawler: Crawler, reactor: ReactorBase) -> Self:
        if crawler.settings.getbool("DNSCACHE_ENABLED"):
            cache_size = crawler.settings.getint("DNSCACHE_SIZE")
        else:
            cache_size = 0
        return cls(
            reactor,
            cache_size,
            crawler.settings.getfloat("DNS_TIMEOUT"),
            crawler.settings.getfloat("DNSCACHE_TTL"),
        )

    def install_on_reactor(self) -> None:
        self.reactor.installResolver(self)

    def getHostByName(self, name: str, timeout: Sequence[int] = ()) -> Deferred[str]:
        try:
            result = dnscache[name]
        except KeyError:
            pass
        else:
            if isinstance(result, _NegativeCacheEntry):
                return defer.fail(result.error)
            return defer.succeed(result)
        follower: Deferred[str] = defer.Deferred()
        waiters = self._resolutions.get(name)
        if waiters is not None:
            waiters.append(follower)
            return follower
        self._resolutions[name] = [follower]
        # in Twisted<=16.6, getHostByName() is always called with
        # a default timeout of 60s (actually passed as (1, 3, 11, 45) tuple),
        # so the input argument above is simply overridden
        # to enforce Scrapy's DNS_TIMEOUT setting's value
        # The timeout arg is typed as Sequence[int] but supports floats.
        timeout = (self.timeout,)  # type: ignore[assignment]
        d = super().getHostByName(name, timeout)
        d.addBoth(self._cache_result, name)
        return follower

    def _cache_result(self, result: Any, name: str) -> Any:
        waiters = self._resolutions.pop(name)
        if isinstance(result, Failure):
            assert result.value is not None
            dnscache[name] = _NegativeCacheEntry(result.value)
            for waiter in waiters:
                if not waiter.called:
                    waiter.errback(result)
            # The waiters get the failure, do not also propagate it.
            return None
        dnscache[name] = result
        for waiter in waiters:
            if not waiter.called:
                waiter.callback(result)
        return result


def _address_with_port(address: IAddress, port: int) -> IAddress:
    if getattr(address, "port", port) == port:
        return address
    return attr.evolve(address, port=port)


@implementer(IHostResolution)
class HostResolution:
    def __init__(self, name: str):
        self.name: str = name

    def cancel(self) -> None:  # pragma: no cover
        raise NotImplementedError


@provider(IResolutionReceiver)
class _CachingResolutionReceiver:
    """Caches the result of a name resolution and forwards it to the
    receivers of all requests that started it or joined it while in
    flight."""

    def __init__(self, hostName: str, on_complete: Callable[[str], Any]):
        self.hostName: str = hostName
        self.on_complete: Callable[[str], Any] = on_complete
        self.receivers: list[tuple[IResolutionReceiver, int]] = []
        self.addresses: list[IAddress] = []

    def resolutionBegan(self, resolution: IHostResolution) -> None:
        for receiver, _ in self.receivers:
            receiver.resolutionBegan(resolution)

    def addressResolved(self, address: IAddress) -> None:
        for receiver, port in self.receivers:
            receiver.addressResolved(_address_with_port(address, port))
        # Name resolution does not depend on the port, so cache entries are
        # kept port-agnostic and the requested port is set on cache hits.
        self.addresses.append(_address_with_port(address, 0))

    def resolutionComplete(self) -> None:
        for receiver, _ in self.receivers:
            receiver.resolutionComplete()
        if self.addresses:
            dnscache[self.hostName] = self.addresses
        else:
            dnscache[self.hostName] = _NegativeCacheEntry(
                DNSLookupError(f"DNS lookup failed for {self.hostName}")
            )
        self.on_complete(self.hostName)


@implementer(IHostnameResolver)
class CachingHostnameResolver:
    """
    Experimental caching resolver. Resolves IPv4 and IPv6 addresses,
    does not support setting a timeout value for DNS requests.
    """

    def __init__(self, reactor: ReactorBase, cache_size: int, cache_ttl: float = 0):
        self.reactor: ReactorBase = reactor
        self.original_resolver: IHostnameResolver = reactor.nameResolver
        dnscache.limit = cache_size
        dnscache.ttl = cache_ttl
        # Names with a resolution in flight, so that concurrent requests for
        # the same name share a single resolution.
        self._resolutions: dict[str, _CachingResolutionReceiver] = {}

    @classmethod
    def from_crawler(cls, crawler: Crawler, reactor: ReactorBase) -> Self:
        if crawler.settings.getbool("DNSCACHE_ENABLED"):
            cache_size = crawler.settings.getint("DNSCACHE_SIZE")
        else:
            cache_size = 0
        return cls(reactor, cache_size, crawler.settings.getfloat("DNSCACHE_TTL"))

    def install_on_reactor(self) -> None:
        self.reactor.installNameResolver(self)

    def resolveHostName(
        self,
        resolutionReceiver: IResolutionReceiver,
        hostName: str,
        portNumber: int = 0,
        addressTypes: Sequence[type[IAddress]] | None = None,
        transportSemantics: str = "TCP",
    ) -> IHostResolution:
        try:
            result = dnscache[hostName]
        except KeyError:
            pass
        else:
            resolutionReceiver.resolutionBegan(HostResolution(hostName))
            if not isinstance(result, _NegativeCacheEntry):
                for address in result:
                    resolutionReceiver.addressResolved(
                        _address_with_port(address, portNumber)
                    )
            resolutionReceiver.resolutionComplete()
            return resolutionReceiver
        receiving = self._resolutions.get(hostName)
        if receiving is not None:
            receiving.receivers.append((resolutionReceiver, portNumber))
            return resolutionReceiver
        receiving = _CachingResolutionReceiver(hostName, self._resolutions.pop)
        receiving.receivers.append((resolutionReceiver, portNumber))
        self._resolutions[hostName] = receiving
        try:
            return self.original_resolver.resolveHostName(
                receiving,
                hostName,
                portNumber,
                addressTypes,
                transportSemantics,
            )
        except Exception:
            del self._resolutions[hostName]
            raise
