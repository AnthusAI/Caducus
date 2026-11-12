"""Public audiences: anonymous readers who subscribe to a host's lists by email. Design: docs/public-audiences.md."""

from .directory import AudienceDirectory
from .domains import DnsDomainChecker, DomainChecker, StaticDomains
from .service import (
    CONFIRMATION_KIND,
    SURGE_KIND,
    AudienceKeys,
    AudienceList,
    AudienceSettings,
    LinkBuilder,
    PublicAudience,
    SubscribeRequest,
)

__all__ = [
    "AudienceDirectory", "AudienceKeys", "AudienceList", "AudienceSettings", "CONFIRMATION_KIND",
    "DnsDomainChecker", "DomainChecker", "LinkBuilder", "PublicAudience", "StaticDomains",
    "SubscribeRequest", "SURGE_KIND",
]
