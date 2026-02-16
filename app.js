// Fuente CSV (Google Sheets publicado)
const CSV_URL =
  "https://docs.google.com/spreadsheets/d/e/2PACX-1vRy8_sTXB8AQ9BwHR_G1ZQPaoBZ-woa4I1XdCZLapf4HsyQaubride3npFnXhDN_P85dizphkccFcfh/pub?gid=0&single=true&output=csv";

const els = {
  status: document.getElementById("status"),
  lotSel: document.getElementById("lotSel"),
  q: document.getElementById("q"),
  grid: document.getElementById("grid"),
  empty: document.getElementById("empty"),
  err: document.getElementById("err"),
  lastLoad: document.getElementById("lastLoad"),
  totalRows: document.getElementById("totalRows"),
  countPill: document.getElementById("countPill"),
  refreshBtn: document.getElementById("refreshBtn"),
  tabs: document.getElementById("tabs"),
};

let RAW = [];
let FILTERED = [];
let CURRENT_CAT = "loteria"; // "loteria" | "animalitos"

function showError(msg) {
  els.err.style.display = "block";
  els.err.textContent = msg;
}
function hideError() {
  els.err.style.display = "none";
  els.err.textContent = "";
}

function parseCSV(text) {
  const rows = text
    .trim()
    .split("\n")
    .map((r) =>
      r
        .split(/,(?=(?:(?:[^\"]*\"){2})*[^\"]*$)/)
        .map((v) => v.replace(/^\"|\"$/g, "").trim())
    );

  const headers = rows.shift().map((h) => h.toLowerCase());

  return rows.map((r) => {
    const o = {};
    headers.forEach((h, i) => (o[h] = r[i] || ""));
    return {
      categoria: o.categoria || "loteria",
      fecha: o.fecha || "",
      loteria: o.loteria || "",
      horario: o.horario || "",
      triple: o.triple || "",
      terminal_a_b: o.terminal_a_b || "",
      terminal_c: o.terminal_c || "",
      numero: o.numero || "",
      signo: o.signo || "",
      cacho: o.cacho || "",
      animal: o.animal || "",
    };
  });
}

function uniq(arr) {
  return [...new Set(arr)].filter(Boolean);
}

function escapeHtml(s) {
  return String(s).replace(/[&<>\"']/g, (m) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#039;" }[m])
  );
}

function rowsForCurrentCategory() {
  return RAW.filter((r) => (r.categoria || "loteria") === CURRENT_CAT);
}

function populateLotterySelect(rows) {
  const lots = uniq(rows.map((r) => r.loteria)).sort((a, b) => a.localeCompare(b));
  els.lotSel.innerHTML =
    `<option value="">Todas</option>` +
    lots.map((l) => `<option value="${escapeHtml(l)}">${escapeHtml(l)}</option>`).join("");
}

function applyFilters() {
  const lot = els.lotSel.value;
  const q = els.q.value.toLowerCase();

  const base = rowsForCurrentCategory();

  FILTERED = base.filter((r) => {
    if (lot && r.loteria !== lot) return false;
    if (!q) return true;
    return Object.values(r).join(" ").toLowerCase().includes(q);
  });

  render();
}

function tag(label, value) {
  if (!value) return "";
  return `<span class="tag"><b>${label}:</b> ${escapeHtml(value)}</span>`;
}

function cardHtml(r) {
  const isAnimalitos = CURRENT_CAT === "animalitos";

  return `
    <div class="card">
      <div class="top">
        <div>
          <div class="lot">${escapeHtml(r.loteria || "—")}</div>
          <div class="date">${escapeHtml(r.fecha || "")}</div>
        </div>
        <div class="time">${escapeHtml(r.horario || "")}</div>
      </div>
      <div class="tags">
        ${
          isAnimalitos
            ? `${tag("Número", r.numero)}${tag("Animal", r.animal)}`
            : `
              ${tag("Triple", r.triple)}
              ${tag("Terminal A/B", r.terminal_a_b)}
              ${tag("Terminal C", r.terminal_c)}
              ${tag("Número", r.numero)}
              ${tag("Signo", r.signo)}
              ${tag("Cacho", r.cacho)}
            `
        }
      </div>
    </div>
  `;
}

function render() {
  els.countPill.textContent = String(FILTERED.length);
  els.totalRows.textContent = String(RAW.length);

  const badge = document.getElementById("countBadge");
  if (badge) badge.textContent = String(FILTERED.length);

  if (!FILTERED.length) {
    els.grid.style.display = "none";
    els.empty.style.display = "block";
    els.empty.textContent = "No hay resultados con esos filtros.";
    return;
  }

  els.empty.style.display = "none";
  els.grid.style.display = "grid";
  els.grid.innerHTML = FILTERED.map(cardHtml).join("");
}

function setCategory(cat) {
  CURRENT_CAT = cat;

  // Reset UI de filtros al cambiar de pestaña
  els.lotSel.value = "";
  els.q.value = "";

  // Cambiar placeholder según pestaña
  els.q.placeholder =
    cat === "animalitos"
      ? "Ej: 8:30 AM, 20, Cerdo, Guacharito..."
      : "Ej: 8:00 AM, 775, Aries, terminal...";

  // Re-cargar select con loterías de la categoría actual
  const base = rowsForCurrentCategory();
  populateLotterySelect(base);

  // Cambiar título de la sección
  const h3 = document.querySelector(".section-title h3");
  if (h3) h3.textContent = cat === "animalitos" ? "Resultados de Animalitos" : "Resultados";

  applyFilters();
}

async function loadData() {
  hideError();
  els.status.textContent = "Cargando datos…";
  els.refreshBtn.disabled = true;

  try {
    const res = await fetch(CSV_URL, { cache: "no-store" });
    if (!res.ok) throw new Error("No se pudo cargar el CSV");

    const text = await res.text();
    RAW = parseCSV(text);

    els.lastLoad.textContent = new Date().toLocaleString();
    els.status.textContent = "Listo. Datos cargados desde Google Sheets.";

    // Inicializa tabs + filtros con categoría por defecto
    setCategory(CURRENT_CAT);
  } catch (e) {
    showError(e.message);
    els.status.textContent = "Error cargando datos.";
    RAW = [];
    FILTERED = [];
    render();
  } finally {
    els.refreshBtn.disabled = false;
  }
}

// Eventos filtros
els.lotSel.addEventListener("change", applyFilters);
els.q.addEventListener("input", applyFilters);
els.refreshBtn.addEventListener("click", loadData);

// Eventos tabs
if (els.tabs) {
  els.tabs.addEventListener("click", (ev) => {
    const btn = ev.target.closest("button.tab");
    if (!btn) return;

    const cat = btn.dataset.cat;
    if (!cat || cat === CURRENT_CAT) return;

    // activar estilos
    els.tabs.querySelectorAll("button.tab").forEach((b) =>
      b.classList.toggle("active", b === btn)
    );

    setCategory(cat);
  });
}

// Init
loadData();
document.getElementById("year").textContent = new Date().getFullYear();
