from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from server.config import settings
from server.consistency import run_consistency_sweep
from server.errors.s3errors import S3Error, render_error
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.filesystem import FilesystemBackend


def _default_storage() -> FilesystemBackend:
    return FilesystemBackend(
        data_dir=settings.data_dir,
        parts_dir=settings.parts_dir,
        write_delay_ms=settings.test_write_delay_ms,
    )


def _default_metadata() -> SQLiteMetadataStore:
    return SQLiteMetadataStore(db_path=settings.db_path)


def create_app(
    storage: FilesystemBackend | None = None,
    metadata: SQLiteMetadataStore | None = None,
) -> FastAPI:
    _storage = storage or _default_storage()
    _metadata = metadata or _default_metadata()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Sweep runs before the server accepts any requests
        run_consistency_sweep(_metadata, _storage)
        yield

    application = FastAPI(title="litmus", version="0.1.0", lifespan=lifespan)

    application.state.storage = _storage
    application.state.metadata = _metadata

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
