# Finance Upload (Unified New Project)

โปรเจคนี้เขียนใหม่ในโฟลเดอร์นี้ โดยอิง logic จาก:

- `finance` (คัดกรอง/แปลงข้อมูล Excel)
- `upload sqlsever` (อัปโหลดและ import ลง SQL Server)

และรวมให้ทำงานเป็นระบบเดียวใน backend เดียว

## ความสามารถ

- `POST /api/preview` ดูผลคัดกรองก่อน
- `POST /api/transform` ดาวน์โหลดไฟล์ Excel หลังคัดกรอง
- `POST /api/transform-import` คัดกรองแล้ว import ลง SQL Server ทันที
- `POST /api/imports/upload` อัปโหลด Excel เข้าฐานข้อมูลโดยตรง
- `GET /api/imports/{job_id}` ดูสถานะ job
- `GET /api/imports/{job_id}/errors` ดูรายการ error

## โครงสร้าง

- `app/main.py` FastAPI app + static frontend
- `app/api/routes.py` API endpoints
- `app/services/rules_engine.py` กฎคัดกรองจากโปรเจค finance
- `app/services/import_service.py` mapping + validation + upsert SQL Server
- `app/db/models.py` ตาราง `sales_records`, `import_jobs`, `import_errors`
- `app/frontend/*` หน้าเว็บใช้งานทันที

## ติดตั้ง

```bash
pip install -r requirements.txt
```

## ตั้งค่า

คัดลอก `.env.example` เป็น `.env` แล้วแก้ค่า SQL Server

## รัน

```bash
python -m uvicorn app.main:app --reload --port 8000
```

เปิดเว็บที่ `http://127.0.0.1:8000`

## หมายเหตุ

- ตอน startup จะสร้างตารางอัตโนมัติถ้ายังไม่มี
- รองรับ `.xlsx` และ `.xls`
