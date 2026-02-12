from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile, status
from fastapi.responses import JSONResponse, StreamingResponse
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.models import ImportError, ImportJob
from app.db.session import get_db
from app.schemas import ImportErrorItem, ImportJobResponse, ImportResult, PreviewResponse, TransformOptions
from app.services.excel_reader import ExcelReadError, read_excel_bytes
from app.services.excel_writer import dataframe_to_excel_bytes
from app.services.import_service import import_dataframe_to_db
from app.services.rules_engine import apply_business_rules

settings = get_settings()
router = APIRouter(prefix=settings.api_prefix, tags=["api"])


def _parse_options(config_raw: str | None) -> TransformOptions:
    if not config_raw:
        return TransformOptions()
    try:
        return TransformOptions.model_validate(json.loads(config_raw))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid config payload: {exc}") from exc


def _validate_file(file: UploadFile):
    ext = Path(file.filename or "").suffix.lower()
    if ext not in settings.allowed_extensions:
        raise HTTPException(status_code=400, detail=f"File extension {ext or 'unknown'} is not allowed.")


@router.get("/health")
def health():
    return {"status": "ok"}


@router.post("/preview", response_model=PreviewResponse)
async def preview(file: UploadFile = File(...), config: str | None = Form(default=None)):
    _validate_file(file)
    options = _parse_options(config)
    content = await file.read()
    try:
        df = read_excel_bytes(content, file.filename)
    except ExcelReadError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    result = apply_business_rules(df, options)
    preview_df = result.dataframe.head(200)
    return PreviewResponse(
        columns=[str(col) for col in preview_df.columns.tolist()],
        rows=preview_df.where(preview_df.notna(), None).to_dict(orient="records"),
        stats=result.stats,
        issues=result.issues,
    )


@router.post("/transform")
async def transform(file: UploadFile = File(...), config: str | None = Form(default=None)):
    _validate_file(file)
    options = _parse_options(config)
    content = await file.read()
    try:
        df = read_excel_bytes(content, file.filename)
    except ExcelReadError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    result = apply_business_rules(df, options)
    if result.issues:
        return JSONResponse(status_code=422, content={"issues": result.issues})
    data = dataframe_to_excel_bytes(result.dataframe)
    headers = {"Content-Disposition": 'attachment; filename="finance-screening-output.xlsx"'}
    return StreamingResponse(
        iter([data]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )


@router.post("/transform-import", response_model=ImportResult)
async def transform_import(
    request: Request,
    file: UploadFile = File(...),
    config: str | None = Form(default=None),
    db: Session = Depends(get_db),
):
    _validate_file(file)
    options = _parse_options(config)
    content = await file.read()
    try:
        df = read_excel_bytes(content, file.filename)
    except ExcelReadError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    result = apply_business_rules(df, options)
    if result.issues:
        return JSONResponse(status_code=422, content={"issues": result.issues})
    correlation_id = getattr(request.state, "correlation_id", str(uuid4()))
    return import_dataframe_to_db(
        db=db,
        frame=result.dataframe,
        filename=file.filename or "finance-screening-output.xlsx",
        correlation_id=correlation_id,
    )


@router.post("/imports/upload", response_model=ImportResult, status_code=status.HTTP_201_CREATED)
async def upload_import(
    request: Request,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    _validate_file(file)
    raw_bytes = await file.read()
    max_size = settings.max_upload_size_mb * 1024 * 1024
    if len(raw_bytes) > max_size:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"File is too large. Max size is {settings.max_upload_size_mb} MB.",
        )
    try:
        frame = read_excel_bytes(raw_bytes, file.filename)
    except ExcelReadError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    correlation_id = getattr(request.state, "correlation_id", str(uuid4()))
    return import_dataframe_to_db(
        db=db,
        frame=frame,
        filename=file.filename or "unknown.xlsx",
        correlation_id=correlation_id,
    )


@router.get("/imports/{job_id}", response_model=ImportJobResponse)
def get_import_job(job_id: int, db: Session = Depends(get_db)):
    job = db.get(ImportJob, job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found.")
    return ImportJobResponse.model_validate(job)


@router.get("/imports/{job_id}/errors", response_model=list[ImportErrorItem])
def get_import_errors(job_id: int, db: Session = Depends(get_db)):
    job = db.get(ImportJob, job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found.")
    rows = db.query(ImportError).filter(ImportError.job_id == job_id).all()
    return [
        ImportErrorItem(
            row_number=item.row_number,
            column_name=item.column_name,
            error_message=item.error_message,
        )
        for item in rows
    ]
