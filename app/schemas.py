from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class ColumnMapping(BaseModel):
    tank_no: str = Field(default="เลขตัวถัง")
    item: str = Field(default="รายการ")
    sale_price: str = Field(default="มูลค่ารวม")
    total_value: str = Field(default="มูลค่ารวม")
    product_value: str = Field(default="มูลค่าสินค้า")
    tax: str = Field(default="ภาษี")
    com_fn: str = Field(default="มูลค่าสินค้า")
    com: str = Field(default="ภาษี")


class TransformOptions(BaseModel):
    mapping: ColumnMapping = Field(default_factory=ColumnMapping)
    duplicate_mode: Literal["keep", "group"] = "keep"
    finance_sent_item_label: str = "ส่งไฟแนนซ์"
    finance_broker_item_label: str = "นายหน้าไฟแนนซ์"


class TransformStats(BaseModel):
    rows_in: int
    rows_out: int
    finance_sent_count: int
    finance_broker_count: int
    duplicate_tank_groups: int
    duplicate_rows: int


class PreviewResponse(BaseModel):
    columns: list[str]
    rows: list[dict]
    stats: TransformStats
    issues: list[str]


class ImportErrorItem(BaseModel):
    row_number: int
    column_name: str | None = None
    error_message: str


class ImportResult(BaseModel):
    job_id: int
    status: str
    filename: str
    total_rows: int
    imported_rows: int
    failed_rows: int
    message: str | None = None
    errors: list[ImportErrorItem] = []


class ImportJobResponse(BaseModel):
    id: int
    correlation_id: str
    filename: str
    status: str
    total_rows: int
    imported_rows: int
    failed_rows: int
    message: str | None = None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}
