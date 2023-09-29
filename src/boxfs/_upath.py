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


class BoxPathFlavour(upath.core.UPath._flavour.__class__):
    altsep = "\\"

    def parse_parts(self, parts):
        # Replace backslashes with forward slashes
        if self.altsep:
            replaced_parts = [p.replace(self.altsep, self.sep) for p in parts]
        else:
            replaced_parts = parts
        
        return super().parse_parts(replaced_parts)


class BoxPath(upath.core.UPath):
    _default_accessor = _BoxAccessor
    _flavour = BoxPathFlavour()

    @classmethod
    def _from_parts(cls, args, url=None, **kwargs):
        if url is not None and url.netloc != "":
            raise ValueError(
                "Network location for BoxPath must be empty. "
                "To fix, specify BoxPath with triple slashes (box:///path/to/file)"
            )
        obj = super()._from_parts(args, url, **kwargs)
        return obj

    @classmethod
    def _from_parsed_parts(cls, drv, root, parts, url=None, **kwargs):
        if url is not None and url.netloc != "":
            raise ValueError(
                "Network location for BoxPath must be empty. "
                "To fix, specify BoxPath with triple slashes (box:///path/to/file)"
            )
        obj = super()._from_parsed_parts(drv, root, parts, url=url, **kwargs)
        return obj

    @classmethod
    def _format_parsed_parts(
        cls,
        drv: str,
        root: str,
        parts: list[str],
        url=None,
        **kwargs,
    ) -> str:
        if parts:
            join_parts = parts[1:] if parts[0] == "/" else parts
        else:
            join_parts = []
        if drv or root:
            path: str = drv + root + cls._flavour.join(join_parts)
        else:
            path = cls._flavour.join(join_parts)
        if not url:
            scheme: str = kwargs.get("scheme", "file")
            netloc: str = kwargs.get("netloc", "")
        else:
            scheme, netloc = url.scheme, url.netloc
        scheme = scheme + ":"
        netloc = "//"
        formatted = scheme + netloc + path
        return formatted



upath.registry._registry.known_implementations["box"] = __name__ + ".BoxPath"
