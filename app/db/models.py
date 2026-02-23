from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import Boolean, Date, DateTime, ForeignKey, Integer, Numeric, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class SalesRecord(Base):
    __tablename__ = "sales_records"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    business_key: Mapped[str] = mapped_column(String(100), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(255))
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    record_date: Mapped[date] = mapped_column(Date)
    invoice_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    invoice_no: Mapped[str | None] = mapped_column(String(100), nullable=True)
    item_description: Mapped[str | None] = mapped_column(String(255), nullable=True)
    product_value: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    tax_value: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    total_value: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    vin_no: Mapped[str | None] = mapped_column(String(100), nullable=True)
    cancel_flag: Mapped[str | None] = mapped_column(String(50), nullable=True)
    cancel_product_value: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    cancel_tax_value: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    cancel_total_value: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    org_type_hq: Mapped[str | None] = mapped_column(String(50), nullable=True)
    org_type_branch_no: Mapped[int | None] = mapped_column(Integer, nullable=True)
    taxpayer_id: Mapped[str | None] = mapped_column(String(20), nullable=True)
    sale_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    com_fn: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    com_value: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    rule_applied: Mapped[str | None] = mapped_column(String(100), nullable=True)
    is_duplicate_tank: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    group_id: Mapped[str | None] = mapped_column(String(150), nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )


class ImportJob(Base):
    __tablename__ = "import_jobs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    correlation_id: Mapped[str] = mapped_column(String(64), index=True)
    filename: Mapped[str] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(20), index=True)
    total_rows: Mapped[int] = mapped_column(default=0)
    imported_rows: Mapped[int] = mapped_column(default=0)
    failed_rows: Mapped[int] = mapped_column(default=0)
    message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    errors: Mapped[list["ImportError"]] = relationship(
        back_populates="job", cascade="all, delete-orphan"
    )


class ImportError(Base):
    __tablename__ = "import_errors"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    job_id: Mapped[int] = mapped_column(ForeignKey("import_jobs.id"), index=True)
    row_number: Mapped[int] = mapped_column()
    column_name: Mapped[str | None] = mapped_column(String(100), nullable=True)
    error_message: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    job: Mapped["ImportJob"] = relationship(back_populates="errors")
