"""Request IDs: x-amz-request-id and x-amz-id-2 on every response.

S3 returns a pair of IDs with every response it processes, including errors, and
repeats them as RequestId and HostId in XML error bodies. Litmus generates a fresh
pair per request in the same shape as S3's examples: 16 uppercase letters and
digits, and a base64 string encoding 56 random bytes.

AddRequestIds is the outermost ASGI layer, so every response the application sends
carries the headers: handler successes and errors, the request guard's 501s, the
framework error handlers, HEAD responses, streamed GETs, and the early error that
closes the connection after Expect: 100-continue. Responses uvicorn produces without
calling the application (HTTP parser errors) cannot carry them.
"""

import base64
import logging
import secrets
import string
from contextvars import ContextVar
from dataclasses import dataclass

logger = logging.getLogger(__name__)

REQUEST_ID_HEADER = b"x-amz-request-id"
HOST_ID_HEADER = b"x-amz-id-2"

_ALPHABET = string.ascii_uppercase + string.digits


@dataclass(frozen=True)
class RequestIds:
    request_id: str
    host_id: str


def new_request_ids() -> RequestIds:
    return RequestIds(
        request_id="".join(secrets.choice(_ALPHABET) for _ in range(16)),
        host_id=base64.b64encode(secrets.token_bytes(56)).decode(),
    )


_current: ContextVar[RequestIds | None] = ContextVar("litmus_request_ids", default=None)


def current_request_ids() -> RequestIds:
    """The IDs of the request being handled. Outside a request (for example a
    unit test rendering an error directly), a fresh pair."""
    ids = _current.get()
    return ids if ids is not None else new_request_ids()


class AddRequestIds:
    """ASGI middleware: one ID pair per HTTP request, set for the whole request,
    added to the response headers, and logged with every error response."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        ids = new_request_ids()
        token = _current.set(ids)

        async def send_with_ids(message):
            if message["type"] == "http.response.start":
                headers = [
                    (name, value)
                    for name, value in message.get("headers", [])
                    if name.lower() not in (REQUEST_ID_HEADER, HOST_ID_HEADER)
                ]
                headers.append((REQUEST_ID_HEADER, ids.request_id.encode()))
                headers.append((HOST_ID_HEADER, ids.host_id.encode()))
                message = {**message, "headers": headers}
                status = message["status"]
                if status >= 400:
                    # Server faults at ERROR; client errors, and 501 for an
                    # operation Litmus does not implement, at INFO.
                    logger.log(
                        logging.ERROR if status >= 500 and status != 501 else logging.INFO,
                        "request_id=%s host_id=%s %s %s -> %d",
                        ids.request_id,
                        ids.host_id,
                        scope.get("method"),
                        scope.get("path"),
                        status,
                    )
            await send(message)

        try:
            await self.app(scope, receive, send_with_ids)
        finally:
            _current.reset(token)
