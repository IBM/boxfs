import upath.core
import upath.registry


class _BoxAccessor(upath.core._FSSpecAccessor):
    def _format_path(self, path):
        """
        Use netloc as part of path, if specified

        box:///path/to/file and box://path/to/file will be treated the same
        """
        parts = [path._url.netloc] if path._url.netloc else []
        parts.append(path.path.lstrip('/'))
        return "/".join(parts)

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


upath.registry._registry.known_implementations["box"] = __name__ + ".BoxPath"
