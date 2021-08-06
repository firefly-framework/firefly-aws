from __future__ import annotations

from typing import Optional

import firefly as ff


class ExecutionContext(ff.DomainService):
    event: Optional[dict] = None
    context = None
