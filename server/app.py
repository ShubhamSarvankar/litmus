import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.routing import Route

from server.config import settings
from server.connection import CloseConnectionIfContinueNotSent
from server.consistency import run_consistency_sweep
from server.crash_points import validate_configured_crash_point
from server.errors.s3errors import (
    InternalError,
    InvalidArgument,
    InvalidRequest,
    InvalidURI,
    MethodNotAllowed,
    NotImplementedOperation,
    S3Error,
    render_error,
)
from server.metadata.sqlite import SQLiteMetadataStore
from server.request_ids import AddRequestIds, current_request_ids
from server.startup import (
    StartupRefused,  # noqa: F401 (re-exported: callers catch it from server.app)
    establish_store_identity,
    lock_blob_dir,
    refuse_missing_blob_dir,
    refuse_old_environment,
)
from server.storage.blobs import BlobStore

logger = logging.getLogger(__name__)


def _xml_error(exc: S3Error, headers: dict | None = None) -> Response:
    return Response(
        content=render_error(exc),
        status_code=exc.status_code,
        media_type="application/xml",
        headers=headers,
    )


def _allowed_methods(app: FastAPI, path: str) -> list[str]:
    """Every method some route accepts for this path. Starlette's own 405 names
    only the first route whose path matched."""
    methods: set[str] = set()
    for route in app.router.routes:
        if isinstance(route, Route) and route.methods and route.path_regex.match(path):
            methods |= route.methods
    return sorted(methods)


class LitmusApp(FastAPI):
    def build_middleware_stack(self):
        # Both layers sit outside Starlette's ServerErrorMiddleware, so they also see
        # the 500 responses it sends. AddRequestIds is outermost, so it also stamps
        # the responses of the layer that closes connections after
        # Expect: 100-continue (server/connection.py).
        return AddRequestIds(CloseConnectionIfContinueNotSent(super().build_middleware_stack()))


def create_app(
    storage: BlobStore | None = None,
    metadata: SQLiteMetadataStore | None = None,
    min_part_size: int | None = None,
) -> FastAPI:
    """Build the app.

    Without stores (the server), nothing is touched here; the startup hook runs the
    startup sequence of docs/design/storage-layout.md §7 and builds the stores.
    With stores (in-process tests), the blob tree is created right away, so the app
    also works when its startup hook never runs; the hook still takes the lock,
    checks the store identity and runs the sweep.

    min_part_size (bytes) overrides S3_MIN_PART_SIZE for this app; it must be a
    non-negative int.
    """
    if min_part_size is None:
        min_part_size = settings.min_part_size
    elif isinstance(min_part_size, bool) or not isinstance(min_part_size, int) or min_part_size < 0:
        raise ValueError(f"min_part_size must be a non-negative integer, got {min_part_size!r}")
    validate_configured_crash_point()
    if settings.test_crash_point:
        logger.warning(
            "TEST CRASH POINT ARMED: %s — this server will SIGKILL itself",
            settings.test_crash_point,
        )
    if storage is not None:
        storage.create_tree()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # docs/design/storage-layout.md §7, in order. Every refusal comes before the
        # migration and the sweep, so nothing in blobs/ is deleted and no database is
        # migrated on a configuration that is refused.
        refuse_old_environment()
        md = metadata or SQLiteMetadataStore(db_path=settings.db_path)
        st = storage or BlobStore(settings.blob_dir, write_delay_ms=settings.test_write_delay_ms)
        refuse_missing_blob_dir(md, st)
        lock = lock_blob_dir(st)
        try:
            establish_store_identity(md, st)
            md.migrate()
            st.create_tree()
            app.state.metadata, app.state.storage = md, st
            # The sweep runs before the server accepts any requests.
            run_consistency_sweep(md, st)
            yield
        finally:
            st.release_lock(lock)

    # No OpenAPI or docs routes: GET /docs, /redoc and /openapi.json would shadow
    # ListObjects for buckets with those names.
    application = LitmusApp(
        title="litmus",
        version="0.1.0",
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )

    application.state.storage = storage
    application.state.metadata = metadata
    application.state.min_part_size = min_part_size

    # Unimplemented S3 operations and unsafe paths are answered before routing,
    # so they can never fall through to a handler that changes state.
    from server.api.request_guard import reject_unimplemented

    application.middleware("http")(reject_unimplemented)

    @application.exception_handler(S3Error)
    async def s3_error_handler(request: Request, exc: S3Error) -> Response:
        return _xml_error(exc, headers=exc.headers or None)

    # Responses the framework would otherwise produce itself, with a JSON or plain
    # text body: every one becomes an S3 XML error.

    @application.exception_handler(StarletteHTTPException)
    async def framework_http_error(request: Request, exc: StarletteHTTPException) -> Response:
        if exc.status_code == 404:
            # No route matches the path, e.g. an empty bucket segment ("//x").
            return _xml_error(InvalidURI(extra={"URI": request.url.path}))
        if exc.status_code == 405:
            if request.method == "OPTIONS":
                # OPTIONS is S3's CORS preflight, which Litmus does not implement.
                return _xml_error(NotImplementedOperation(extra={"Method": "OPTIONS"}))
            allowed = _allowed_methods(application, request.scope["path"])
            return _xml_error(
                MethodNotAllowed(extra={"Method": request.method}),
                headers={"Allow": ", ".join(allowed)},
            )
        return _xml_error(InternalError() if exc.status_code >= 500 else InvalidRequest())

    @application.exception_handler(RequestValidationError)
    async def validation_error(request: Request, exc: RequestValidationError) -> Response:
        return _xml_error(InvalidArgument())

    @application.exception_handler(Exception)
    async def unhandled_error(request: Request, exc: Exception) -> Response:
        # Logged here with the request ID, so the client's ID matches the traceback.
        # Starlette re-raises the exception after sending this response.
        ids = current_request_ids()
        logger.error(
            "request_id=%s host_id=%s unhandled exception in %s %s",
            ids.request_id,
            ids.host_id,
            request.method,
            request.url.path,
            exc_info=exc,
        )
        return _xml_error(InternalError())

    @application.get("/health")
    async def health() -> JSONResponse:
        return JSONResponse({"status": "ok"})

    # The bucket routes come first: they also take "/{bucket}/", which the object
    # routes' "/{bucket}/{key:path}" would otherwise match with an empty key. A path
    # with nothing after the bucket and its slash names the bucket, not a key (S3
    # User Guide, "Virtual hosting of general purpose buckets": the key is the rest
    # of the Request-URI after the bucket).
    from server.api.buckets import router as buckets_router

    application.include_router(buckets_router)

    from server.api.multipart import router as multipart_router

    application.include_router(multipart_router)

    return application


app = create_app()
