"""The cheap validity check before any confirmation is sent: does the domain take mail?

A domain takes mail when it has an MX record that is not the null MX of RFC 7505,
or, with no MX at all, an A or AAAA record (the implicit MX of RFC 5321). A DNS
failure that says nothing about the domain (a timeout, no nameserver answering)
passes: this check filters typos and made-up domains, it is not a security gate,
and a DNS outage must not turn readers away.
"""

from __future__ import annotations

from typing import Dict


class DomainChecker:
    def accepts_mail(self, domain: str) -> bool:
        raise NotImplementedError


class DnsDomainChecker(DomainChecker):
    """dnspython, cached per container (in the `notify` extra)."""

    def __init__(self, lifetime_seconds: float = 2.0) -> None:
        # The lifetime is bounded by the request: the reader is waiting for the answer.
        self._lifetime = lifetime_seconds
        self._seen: Dict[str, bool] = {}

    def _lookup(self, domain: str) -> bool:
        import dns.exception
        import dns.resolver

        resolver = dns.resolver.Resolver()
        resolver.lifetime = self._lifetime
        try:
            answer = resolver.resolve(domain, "MX")
            exchanges = [str(record.exchange).rstrip(".") for record in answer]
            return any(exchanges) and exchanges != [""]
        except dns.resolver.NXDOMAIN:
            return False
        except dns.resolver.NoAnswer:
            pass
        except dns.exception.DNSException:
            return True
        for record_type in ("A", "AAAA"):
            try:
                resolver.resolve(domain, record_type)
                return True
            except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer):
                continue
            except dns.exception.DNSException:
                return True
        return False

    def accepts_mail(self, domain: str) -> bool:
        if domain not in self._seen:
            self._seen[domain] = self._lookup(domain)
        return self._seen[domain]


class StaticDomains(DomainChecker):
    """For tests: every domain takes mail except the ones named."""

    def __init__(self, refused: tuple = ()) -> None:
        self.refused = set(refused)

    def accepts_mail(self, domain: str) -> bool:
        return domain not in self.refused
