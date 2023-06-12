# boxfs

Implementation of the [`fsspec`](https://filesystem-spec.readthedocs.io/en/latest/index.html)
protocol for [Box](https://www.box.com/overview) content management, enabling you to
interface with files stored on Box using a file-system-like navigation.

## Installation

You can install `boxfs` from [PyPI](https://pypi.org/project/boxfs/). Use the following
command:

```bash
pip install boxfs
```

To use install the optional `upath` dependency, use the following command

```bash
pip install boxfs[upath]
```

## Example

```python
import fsspec
from boxsdk import JWTAuth

oauth = JWTAuth.from_settings_file("PATH/TO/JWT_CONFIGURATION.json")
root_id = "<ID-of-file-system-root>"

### For simple file access, you can use `fsspec.open`
with fsspec.open("box://Documents/test_file.txt", "wb", oauth=oauth, root_id=root_id) as f:
    f.write("This file was produced using boxfs")

### For more control, you can use `fsspec.filesystem`
fs = fsspec.filesystem('box', oauth=oauth, root_id=root_id)
# List directory contents
fs.ls("Documents")
# Make new directory
fs.mkdir("Documents/Test Folder")
# Remove a directory
fs.rmdir("Documents/Test Folder")

# Open and write file
with fs.open("Documents/test_file.txt", "wb") as f:
    f.write("This file was updated using boxfs")

# Print file contents
fs.cat("Documents/test_file.txt")
# Delete file
fs.rm("Documents/test_file.txt")

# If you installed with the `upath` extra, you can also use the universal-pathlib UPath
# class.
from upath import UPath
path = UPath("Documents", fs=fs) / "test_file.txt"
path.read_text()
```

## Storage Options

The following storage options are accepted by `fsspec` when creating a `BoxFileSystem`
object:

- oauth: Box app OAuth2 configuration dictionary, e.g. loaded from
    `JWTAuth.from_settings_file`, by default None
- client: An already instantiated boxsdk `Client` object
- client_type: Type of `Client` class to use when connecting to box

If `client` is provided, it is used for handling API calls. Otherwise, the file
system to instantiate a new client connection, of type `client_type`, using the
provided `oauth` configuration.

- root_id: Box ID (as `str`) of folder where file system root is placed, by default
    None
- root_path: Path (as `str`) to Box root folder, must be relative to user's root
    (e.g. "All Files"). The client must have access to the application user's root
    folder (i.e., it cannot be downscoped to a subfolder)

If only `root_id` is provided, the `root_path` is determined from API calls. If 
only `root_path` is provided, the `root_id` is determined from API calls. If
neither is provided, the application user's root folder is used.

- path_map: Mapping of paths to object ID strings, used to populate initial lookup
    cache for quick directory navigation
- scopes: List of permissions to which the API token should be restricted. If None
    (default), no restrictions are applied. If scopes are provided, the client
    connection is (1) downscoped to use only the provided scopes, and
    (2) restricted to the directory/subdirectories of the root folder.

## Creating a Box App

Before you can use `boxfs`, you will need a Box application through which you can route
your API calls. To do so, you can follow the steps for
["Setup with JWT"](https://developer.box.com/guides/authentication/jwt/jwt-setup/)
in the Box Developer documentation. The JWT configuration `.json` file that
you generate will have to be stored locally and loaded using
`JWTAuth.from_settings_file`. You also have to add your application's
Service Account as a collaborator on the root folder of your choosing, or
you will only have access to the Box application's files.
