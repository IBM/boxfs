import upath.core
import upath.registry


class _BoxAccessor(upath.core._FSSpecAccessor):
    def mkdir(self, path, create_parents=True, **kwargs):
        if (
            not create_parents
            and not kwargs.get("exist_ok", False)
            and self._fs.exists(self._format_path(path))
        ):
            raise FileExistsError
        return super().mkdir(path, create_parents=create_parents, **kwargs)


class BoxPath(upath.core.UPath):
    _default_accessor = _BoxAccessor


upath.registry._registry.known_implementations["box"] = __name__ + ".BoxPath"
