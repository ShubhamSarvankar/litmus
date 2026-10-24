import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from server.config import settings
from server.connection import CloseConnectionIfContinueNotSent
from server.consistency import run_consistency_sweep
from server.crash_points import validate_configured_crash_point
from server.errors.s3errors import S3Error, render_error
from server.metadata.sqlite import SQLiteMetadataStore
from server.startup import (
    StartupRefused,  # noqa: F401 (re-exported: callers catch it from server.app)
    establish_store_identity,
    lock_blob_dir,
    refuse_missing_blob_dir,
    refuse_old_environment,
)
from server.storage.blobs import BlobStore

logger = logging.getLogger(__name__)


class LitmusApp(FastAPI):
    def build_middleware_stack(self):
        # Outermost layer, outside Starlette's ServerErrorMiddleware, so it also sees
        # the 500 responses that middleware sends (server/connection.py).
        return CloseConnectionIfContinueNotSent(super().build_middleware_stack())


def create_app(
    storage: BlobStore | None = None,
    metadata: SQLiteMetadataStore | None = None,
) -> FastAPI:
    """Build the app.

    Without stores (the server), nothing is touched here; the startup hook runs the
    startup sequence of docs/design/storage-layout.md §7 and builds the stores.
    With stores (in-process tests), the blob tree is created right away, so the app
    also works when its startup hook never runs; the hook still takes the lock,
    checks the store identity and runs the sweep.
    """
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
        # sweep, so nothing in blobs/ is deleted on a configuration that is refused.
        refuse_old_environment()
        md = metadata or SQLiteMetadataStore(db_path=settings.db_path)
        st = storage or BlobStore(settings.blob_dir, write_delay_ms=settings.test_write_delay_ms)
        refuse_missing_blob_dir(md, st)
        lock = lock_blob_dir(st)
        try:
            establish_store_identity(md, st)
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

    # Unimplemented S3 operations and unsafe paths are answered before routing,
    # so they can never fall through to a handler that changes state.
    from server.api.request_guard import reject_unimplemented

    application.middleware("http")(reject_unimplemented)

    @application.exception_handler(S3Error)
    async def s3_error_handler(request: Request, exc: S3Error) -> Response:
        return Response(
            content=render_error(exc),
            status_code=exc.status_code,
            media_type="application/xml",
        )

    @application.get("/health")
    async def health() -> JSONResponse:
        return JSONResponse({"status": "ok"})

    from server.api.multipart import router as multipart_router

    application.include_router(multipart_router)

    from server.api.buckets import router as buckets_router

    application.include_router(buckets_router)

    return application


app = create_app()
