from __future__ import annotations

import pytest

from otlp_client.outcomes import (
    ExportOutcome,
    PartialSuccess,
    Permanent,
    Retryable,
    Success,
    is_credential_rejection,
)


@pytest.mark.parametrize("status", [401, 403])
def test_a_permanent_auth_status_is_a_credential_rejection(status: int) -> None:
    assert is_credential_rejection(Permanent(status=status, message="nope"))


@pytest.mark.parametrize(
    "outcome",
    [
        Permanent(status=400, message="bad payload"),
        Permanent(status=None, message="unclassified"),
        Retryable(status=401, message="not permanent"),
        Success(),
        PartialSuccess(rejected=1),
    ],
)
def test_everything_else_is_not_a_credential_rejection(outcome: ExportOutcome) -> None:
    assert not is_credential_rejection(outcome)
