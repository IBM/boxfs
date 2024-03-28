import upath
import upath.registry


class BoxPath(upath.UPath):
    @property
    def path(self) -> str:
        p = super().path
        if not p.startswith("/"):
            p = "/" + p
        return p
