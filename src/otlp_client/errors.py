"""Exception hierarchy."""

from __future__ import annotations


class OTLPError(Exception):
    """Base for every error raised by this library."""


class OTLPConfigError(OTLPError):
    """Invalid configuration, or a missing optional extra."""


class OTLPTransportError(OTLPError):
    """The export could not be delivered after exhausting retries."""


class OTLPPermanentError(OTLPError):
    """The collector rejected the export and retrying cannot help."""
