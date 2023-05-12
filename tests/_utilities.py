import boxsdk
import boxsdk.object.file
import boxsdk.object.folder
import boxsdk.object.search
from boxsdk.pagination.limit_offset_based_object_collection import (
    LimitOffsetBasedObjectCollection,
)
from boxsdk.session.session import Session


def ItemJSON(
    name,
    id,
    created_at,
    modified_at,
    _type="file",
    path_collection=None,
):
    if path_collection is None:
        path = {"total_count": 0, "entries": []}
    else:
        path = {
            "total_count": len(path_collection),
            "entries": path_collection,
        }

    return {
        "id": str(id),
        "etag": "1",
        "type": _type,
        "sequence_id": "3",
        "name": name,
        "sha1": "",
        "file_version": {"id": id, "type": "file_version", "sha1": ""},
        "description": "Test Description",
        "size": 629644,
        "path_collection": path,
        "created_at": created_at,
        "modified_at": modified_at,
        "content_created_at": created_at,
        "content_modified_at": modified_at,
        "parent": path["entries"][-1] if path["entries"] else None,
        "item_status": "active",
    }


class MockedClient(boxsdk.DevelopmentClient):
    def __init__(self, *args, **kwargs):
        super(boxsdk.Client, self).__init__()
        self._oauth = None
        self._session = Session()


class MockedCollection(LimitOffsetBasedObjectCollection):
    def __init__(self, *args, mock_entries=[], **kwargs):
        super().__init__(*args, **kwargs)

        self.mock_entries = mock_entries

    def _load_next_page(self) -> dict:
        if self.mock_entries is None:
            raise boxsdk.BoxAPIException(
                status=404,
                headers=None,
                code="not_found",
                message="Not Found",
                request_id=None,
                url=None,
                method=None,
                context_info={
                    "errors": [
                        {
                            "reason": "invalid_parameter",
                            "name": "folder",
                            "message": "Invalid value. Folder has no mocked items",
                        }
                    ]
                },
                network_response=None,
            )

        response = {
            "entries": self.mock_entries,
            "limit": 1000,
            "offset": 0,
            "total_count": len(self.mock_entries),
        }

        return response
