import logging
import sys

import pytest
from ._utilities import MockedClient  # noqa: F401


@pytest.fixture(autouse=True, scope="session")
def logger():
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    return logging.getLogger(__name__.split(".", maxsplit=1)[0])


@pytest.fixture(scope="module")
def client(do_mock, request, logger):
    import boxsdk
    from boxsdk import JWTAuth
    import requests

    def blank_response(self, method, url, **kwargs):
        raise NotImplementedError(
            f"Mocked request '{method}' to '{url}' not implemented"
        )

    if do_mock:
        logger.info("running mocked client")
        # Block API requests
        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setattr(requests.Session, "request", blank_response)
            yield MockedClient()
    else:
        logger.info("running real client")
        api_config = request.config.getoption("api_config")
        if api_config is not None:
            config = JWTAuth.from_settings_file(api_config)

            client = boxsdk.LoggingClient(config)
        else:
            client = None

        yield client

@pytest.fixture(scope="module")
def client_type():
    import boxsdk
    return boxsdk.LoggingClient


@pytest.fixture(
    params=[
        pytest.param(True, id="mocked"),
        pytest.param(
            False,
            marks=pytest.mark.skipif("not config.getoption('with_api')"),
            id="real",
        ),
    ],
    scope="module",
)
def do_mock(request):
    return request.param


@pytest.fixture(autouse=True)
def skip_real(request, do_mock):
    # Add marker to run test on only mocked API connection
    if request.node.get_closest_marker("mock_only") and not do_mock:
        pytest.skip("skipped on real API connection")


@pytest.fixture(scope="module")
def root_id(request):
    root_id = request.config.getoption("box_root_id")
    return root_id


@pytest.fixture(scope="module")
def root_path(request, do_mock):  # noqa: F811
    box_root_path = request.config.getoption("box_root_path")

    return box_root_path


BOX_CODES = {
    "not_found": {
        "status": 404,
        "message": "Not Found",
        "reason": "invalid_parameter",
        "error_message": (
            "Invalid value '{object_id}'. '{_type}' with value '{object_id}' not found"
        ),
    }
}

@pytest.fixture(scope="session")
def box_error():
    import boxsdk
    def _error(code, **kwargs):
        error_details = BOX_CODES[code]
        return boxsdk.BoxAPIException(
            status=error_details["status"],
            headers=None,
            code=code,
            message=error_details["message"],
            request_id=None,
            url=None,
            method=None,
            context_info={
                "errors": [
                    {
                        "reason": error_details["reason"],
                        "name": kwargs.get("_type", ""),
                        "message": error_details["error_message"].format(**kwargs),
                    }
                ]
            },
            network_response=None,
        )
    yield _error



def pytest_addoption(parser):
    parser.addoption(
        "--with_api",
        action="store_true",
        dest="with_api",
        default=False,
        help="enable testing over real Box API connection",
    )
    parser.addoption(
        "--api_config",
        action="store",
        dest="api_config",
        help="path to Box JWT config json file",
    )
    parser.addoption(
        "--box_root_id",
        action="store",
        dest="box_root_id",
        help="ID of Box root folder",
    )
    parser.addoption(
        "--box_root_path",
        action="store",
        dest="box_root_path",
        help=(
            'path of Box root folder, relative to "All Files" (optional if box_root_id '
            'specified)'
        ),
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "mock_only: run test only on mocked API connection"
    )
