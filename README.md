# boxfs

Implementation of the [`fsspec`](https://filesystem-spec.readthedocs.io/en/latest/index.html) protocol for [Box](https://www.box.com/overview) content
management, enabling you to interface with files stored on Box using
file-system-like navigation.

## Installation

You can install `boxfs` from [PyPI](https://pypi.org/project/boxfs/). Use the following
command:

```bash
pip install boxfs
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
```

## Creating a Box App

Before you can use `boxfs`, you will need a Box application through which you can route
your API calls. To do so, you can follow the steps for
["Setup with JWT"](https://developer.box.com/guides/authentication/jwt/jwt-setup/)
in the Box Developer documentation. The JWT configuration `.json` file that
you generate will have to be stored locally and loaded using
`JWTAuth.from_settings_file`. You also have to add your application's
Service Account as a collaborator on the root folder of your choosing, or
you will only have access to the Box application's files.
