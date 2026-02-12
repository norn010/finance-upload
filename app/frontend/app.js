const fileInput = document.getElementById("excel-file");
const btnPreview = document.getElementById("btn-preview");
const btnDownload = document.getElementById("btn-download");
const btnImport = document.getElementById("btn-import");
const summary = document.getElementById("summary");
const table = document.getElementById("preview-table");

function buildOptions() {
  return {
    mapping: {
      tank_no: "เลขตัวถัง",
      item: "รายการ",
      sale_price: "มูลค่ารวม",
      total_value: "มูลค่ารวม",
      product_value: "มูลค่าสินค้า",
      tax: "ภาษี",
      com_fn: "มูลค่าสินค้า",
      com: "ภาษี",
    },
    duplicate_mode: document.getElementById("duplicate-mode").value,
    finance_sent_item_label: document.getElementById("label-sent").value,
    finance_broker_item_label: document.getElementById("label-broker").value,
  };
}

function getFormData() {
  const file = fileInput.files[0];
  if (!file) {
    throw new Error("กรุณาเลือกไฟล์ Excel ก่อน");
  }
  const form = new FormData();
  form.append("file", file);
  form.append("config", JSON.stringify(buildOptions()));
  return form;
}

function setBusy(busy) {
  btnPreview.disabled = busy;
  btnDownload.disabled = busy;
  btnImport.disabled = busy;
}

function renderTable(columns, rows) {
  table.innerHTML = "";
  if (!columns || columns.length === 0) return;

  const thead = document.createElement("thead");
  const trh = document.createElement("tr");
  columns.forEach((col) => {
    const th = document.createElement("th");
    th.textContent = col;
    trh.appendChild(th);
  });
  thead.appendChild(trh);

  const tbody = document.createElement("tbody");
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    columns.forEach((col) => {
      const td = document.createElement("td");
      const value = row[col];
      td.textContent = value === null || value === undefined ? "" : String(value);
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(thead);
  table.appendChild(tbody);
}

async function doPreview() {
  setBusy(true);
  try {
    const response = await fetch("/api/preview", { method: "POST", body: getFormData() });
    const payload = await response.json();
    if (!response.ok) throw new Error(JSON.stringify(payload));
    summary.textContent = JSON.stringify(payload.stats, null, 2);
    renderTable(payload.columns, payload.rows);
  } catch (error) {
    summary.textContent = error.message;
  } finally {
    setBusy(false);
  }
}

async function doDownload() {
  setBusy(true);
  try {
    const response = await fetch("/api/transform", { method: "POST", body: getFormData() });
    if (!response.ok) throw new Error(await response.text());
    const blob = await response.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "finance-screening-output.xlsx";
    a.click();
    URL.revokeObjectURL(url);
    summary.textContent = "ดาวน์โหลดไฟล์เรียบร้อย";
  } catch (error) {
    summary.textContent = error.message;
  } finally {
    setBusy(false);
  }
}

async function doTransformImport() {
  setBusy(true);
  try {
    const response = await fetch("/api/transform-import", { method: "POST", body: getFormData() });
    const payload = await response.json();
    if (!response.ok) throw new Error(JSON.stringify(payload));
    summary.textContent = JSON.stringify(payload, null, 2);
  } catch (error) {
    summary.textContent = error.message;
  } finally {
    setBusy(false);
  }
}

btnPreview.addEventListener("click", doPreview);
btnDownload.addEventListener("click", doDownload);
btnImport.addEventListener("click", doTransformImport);
