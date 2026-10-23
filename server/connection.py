"""Connection handling for requests that send `Expect: 100-continue`.

A client that sends `Expect: 100-continue` waits for `100 Continue` before sending
the body. uvicorn sends that interim response only when the application first asks
for the body. If Litmus answers without reading the body (an early 404, 501 or 400),
no `100 Continue` is sent, so the client is entitled to withhold the body (RFC 9110
10.1.1). The connection stays open, the server still expects Content-Length bytes,
and the client's next request on that connection is parsed as the missing body,
which gets 400 Bad Request.

The fix is to close such connections: when the response starts and the application
never asked for the body, the response carries `Connection: close`, so the server
stops expecting a body and the client opens a new connection for its next request.
Requests whose body was read keep their connection; requests without
`Expect: 100-continue` are untouched, since uvicorn drains an unread body that was
sent with the request.
"""


def _expects_continue(scope) -> bool:
    return any(
        name == b"expect" and value.strip().lower() == b"100-continue"
        for name, value in scope.get("headers", [])
    )


class CloseConnectionIfContinueNotSent:
    """ASGI wrapper; must sit outside every layer that can send a response."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or not _expects_continue(scope):
            await self.app(scope, receive, send)
            return

        body_requested = False

        async def tracking_receive():
            nonlocal body_requested
            body_requested = True  # uvicorn sends 100 Continue on this first receive
            return await receive()

        async def closing_send(message):
            if message["type"] == "http.response.start" and not body_requested:
                headers = [
                    (name, value)
                    for name, value in message.get("headers", [])
                    if name.lower() != b"connection"
                ]
                headers.append((b"connection", b"close"))
                message = {**message, "headers": headers}
            await send(message)

        await self.app(scope, tracking_receive, closing_send)
