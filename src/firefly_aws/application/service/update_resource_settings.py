from __future__ import annotations

import firefly as ff


@ff.command_handler()
class UpdateResourceSettings(ff.ApplicationService):
    def __call__(self, **kwargs):
        print("Updating resource settings")
        print(kwargs)
