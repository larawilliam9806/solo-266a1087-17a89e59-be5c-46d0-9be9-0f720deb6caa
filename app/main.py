"""FastAPI surface for the buoy data-lake compaction service.

Endpoints
---------
POST   /days/{day}/upload                   append a CSV as one parquet file
GET    /days/{day}/records                  all records (consistent snapshot)
GET    /days/{day}/files                    active file list / count
POST   /days/{day}/compactions              start background compaction (202)
GET    /days/{day}/compactions              list jobs for the day
GET    /days/{day}/compactions/{job_id}     job status
POST   /days/{day}/withdrawals              withdraw one uploaded batch by file name
GET    /days/{day}/withdrawals              list withdrawals for the day
GET    /days/{day}/uploads/{file_name}      tracked-batch / withdrawal status
GET    /health                              liveness
"""

from __future__ import annotations

import logging
import os
import re
import threading
from contextlib import asynccontextmanager
from datetime import date

from fastapi import Body, FastAPI, File, HTTPException, Request, UploadFile
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field

from .csvio import CsvError
from .lake import (
    ConflictError,
    Lake,
    NotFoundError,
    WithdrawalConflictError,
    WithdrawalNotAllowedError,
    WithdrawalWriteError,
)

log = logging.getLogger("buoy-lake")

DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class WithdrawRequest(BaseModel):
    file: str = Field(..., min_length=1, description="file name returned by the upload response")


def _valid_day(day: str) -> str:
    if not DAY_RE.match(day):
        raise HTTPException(status_code=400, detail=f"invalid day {day!r}; expected YYYY-MM-DD")
    try:
        date.fromisoformat(day)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"invalid calendar day {day!r}")
    return day


def create_app(
    data_dir: str | None = None,
    compaction_delay_sec: float | None = None,
    gc_interval_sec: float | None = None,
    withdrawals_enabled: bool | None = None,
) -> FastAPI:
    data_dir = data_dir or os.environ.get("DATA_DIR", "./data")
    if compaction_delay_sec is None:
        compaction_delay_sec = float(os.environ.get("LAKE_COMPACTION_DELAY_SEC", "0"))
    if gc_interval_sec is None:
        gc_interval_sec = float(os.environ.get("LAKE_GC_INTERVAL_SEC", "2"))
    if withdrawals_enabled is None:
        withdrawals_enabled = os.environ.get("LAKE_WITHDRAWALS_ENABLED", "1").lower() not in (
            "0",
            "false",
            "no",
            "off",
        )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        lake = Lake(
            data_dir,
            compaction_delay_sec=compaction_delay_sec,
            withdrawals_enabled=withdrawals_enabled,
        )
        stats = lake.recover()  # reconcile + resume interrupted compactions
        app.state.lake = lake
        app.state.recovery = stats
        app.state.withdrawals_enabled = withdrawals_enabled

        stop = threading.Event()

        def gc_loop():
            while not stop.wait(gc_interval_sec):
                try:
                    lake.collect_garbage()
                except Exception:
                    log.exception("periodic garbage collection failed")

        gc_thread = threading.Thread(target=gc_loop, name="retired-file-gc", daemon=True)
        gc_thread.start()
        yield
        stop.set()
        gc_thread.join(timeout=5)

    app = FastAPI(title="Buoy Data Lake Compaction Service", version="1.0.0", lifespan=lifespan)

    def lake(request: Request) -> Lake:
        return request.app.state.lake

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.get("/")
    def index():
        return {
            "service": "buoy-lake-compactor",
            "docs": "/docs",
            "withdrawals_enabled": getattr(app.state, "withdrawals_enabled", True),
            "endpoints": [
                "POST /days/{day}/upload",
                "GET /days/{day}/records",
                "GET /days/{day}/files",
                "POST /days/{day}/compactions",
                "GET /days/{day}/compactions",
                "GET /days/{day}/compactions/{job_id}",
                "POST /days/{day}/withdrawals",
                "GET /days/{day}/withdrawals",
                "GET /days/{day}/uploads/{file_name}",
            ],
        }

    @app.post("/days/{day}/upload")
    async def upload(day: str, request: Request, file: UploadFile | None = File(None)):
        """Append a CSV (multipart file or raw ``text/csv`` body) as one parquet."""
        day = _valid_day(day)
        content = await file.read() if file is not None else await request.body()
        if not content:
            raise HTTPException(status_code=400, detail="empty CSV body")
        try:
            # parquet write + fsync are blocking -> run off the event loop
            return await run_in_threadpool(lake(request).upload, day, content)
        except CsvError as exc:
            raise HTTPException(status_code=422, detail=str(exc))

    @app.get("/days/{day}/records")
    def records(day: str, request: Request):
        """All records for the day from one consistent snapshot + file count."""
        return lake(request).read_day(_valid_day(day))

    @app.get("/days/{day}/files")
    def files(day: str, request: Request):
        return lake(request).list_files(_valid_day(day))

    @app.post("/days/{day}/compactions", status_code=202)
    def start_compaction(day: str, request: Request):
        """Start a background compaction; 409 if one is already running."""
        try:
            return lake(request).start_compaction(_valid_day(day))
        except ConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc))

    @app.get("/days/{day}/compactions")
    def list_compactions(day: str, request: Request):
        day = _valid_day(day)
        return {"day": day, "jobs": lake(request).list_compactions(day)}

    @app.get("/days/{day}/compactions/{job_id}")
    def get_compaction(day: str, job_id: int, request: Request):
        day = _valid_day(day)
        try:
            job = lake(request).get_compaction(job_id)
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        if job["day"] != day:
            raise HTTPException(status_code=404, detail=f"job {job_id} does not belong to day {day}")
        return job

    @app.post("/days/{day}/withdrawals")
    async def withdraw(day: str, request: Request, payload: WithdrawRequest = Body(...)):
        """Withdraw one uploaded batch by the ``file`` name from its upload response.

        Only batches uploaded while the withdrawal feature was enabled can be
        withdrawn.  The call is idempotent: repeating it returns the existing
        withdrawal with ``status = "already_withdrawn"``.
        """
        day = _valid_day(day)
        file_name = os.path.basename(payload.file.strip())
        if not file_name:
            raise HTTPException(status_code=400, detail="file must not be empty")
        try:
            return await run_in_threadpool(lake(request).withdraw, day, file_name)
        except WithdrawalNotAllowedError as exc:
            raise HTTPException(status_code=409, detail=str(exc))
        except WithdrawalConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc))
        except WithdrawalWriteError as exc:
            # The rewrite could not be staged; the registry was never touched,
            # so all original rows and the un-withdrawn state are preserved.
            raise HTTPException(status_code=500, detail=str(exc))
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc))

    @app.get("/days/{day}/withdrawals")
    def list_withdrawals(day: str, request: Request):
        return lake(request).list_withdrawals(_valid_day(day))

    @app.get("/days/{day}/uploads/{file_name}")
    def upload_status(day: str, file_name: str, request: Request):
        """Tracked-batch state (active / withdrawn) for one upload file name."""
        day = _valid_day(day)
        try:
            return lake(request).get_batch_status(day, os.path.basename(file_name))
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc))

    return app


app = create_app()
