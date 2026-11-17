"""Domain exceptions for the distribution connection ledger and meter fact store."""


class DistGridError(ValueError):
    """Base class for every validation / rejection raised by the package."""


class ValidationError(DistGridError):
    """Input fails a structural or range check before any business replay."""


class UnsafeConnection(DistGridError):
    """A closed-edge set produces a cycle or a component with two feeders."""


class LedgerRejected(DistGridError):
    """A submitted change version makes some point on the replayed timeline illegal."""


class MeterRejected(DistGridError):
    """A reading batch cannot be applied; the batch leaves no half-applied state."""


class ProductError(DistGridError):
    """An unknown published product / reading cut is referenced."""
