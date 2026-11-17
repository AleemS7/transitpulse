"use strict";

// Relative to /ui/, so the page works wherever the app is mounted.
const API = "../incidents";

const $ = (id) => document.getElementById(id);
let selected = null;

function text(tag, value, className) {
  const el = document.createElement(tag);
  el.textContent = value ?? "";
  if (className) el.className = className;
  return el;
}

function utc(value) {
  return value ? String(value).replace("T", " ").slice(0, 19) : "";
}

async function call(url, options) {
  const resp = await fetch(url, { headers: { Accept: "application/json" }, ...options });
  const body = await resp.json().catch(() => ({}));
  if (!resp.ok) throw new Error(body.error?.message ?? `HTTP ${resp.status}`);
  return body.data;
}

async function loadList() {
  const status = $("status").value;
  const query = new URLSearchParams({ limit: "100" });
  if (status) query.set("status", status);
  $("list-message").textContent = "Loading…";
  try {
    const rows = await call(`${API}?${query}`);
    const body = $("rows");
    body.replaceChildren(...rows.map(row));
    $("list-message").textContent = rows.length ? "" : "No incidents.";
  } catch (err) {
    $("list-message").textContent = `Could not load incidents: ${err.message}`;
  }
}

function row(incident) {
  const tr = document.createElement("tr");
  tr.tabIndex = 0;
  tr.dataset.id = incident.incident_id;
  tr.setAttribute("aria-selected", String(incident.incident_id === selected));
  tr.append(
    text("td", utc(incident.opened_at)),
    text("td", incident.severity, `sev-${incident.severity}`),
    text("td", incident.symptom),
    text("td", incident.cause_category),
    text("td", incident.occurrences),
    text("td", incident.status),
  );
  const open = () => showDetail(incident.incident_id);
  tr.addEventListener("click", open);
  tr.addEventListener("keydown", (e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); open(); } });
  return tr;
}

function fact(name, value) {
  return [text("dt", name), text("dd", value === null || value === undefined ? "—" : String(value))];
}

async function showDetail(id) {
  selected = id;
  for (const tr of $("rows").children) tr.setAttribute("aria-selected", String(tr.dataset.id === id));
  $("ack-message").textContent = "";
  let d;
  try {
    d = await call(`${API}/${encodeURIComponent(id)}`);
  } catch (err) {
    $("list-message").textContent = `Could not load ${id}: ${err.message}`;
    return;
  }
  $("detail-title").textContent = d.symptom || d.incident_id;
  const rulesNote = d.rules_cause_category && d.rules_cause_category !== d.cause_category
    ? ` (rules said ${d.rules_cause_category})` : "";
  $("facts").replaceChildren(
    ...fact("Incident", d.incident_id),
    ...fact("Status", d.status),
    ...fact("Cause", `${d.cause_category ?? "—"}${rulesNote}`),
    ...fact("Confidence", d.confidence),
    ...fact("Severity", d.severity),
    ...fact("Classified by", d.classifier === "agent" ? `agent, ${d.model_version ?? d.model_endpoint}` : d.classifier),
    ...fact("Opened (UTC)", utc(d.opened_at)),
    ...fact("Last seen (UTC)", utc(d.last_seen_at)),
    ...fact("Times seen", d.occurrences),
    ...fact("Acknowledged", d.acknowledged_by ? `${d.acknowledged_by} at ${utc(d.acknowledged_at)}` : null),
  );
  $("runbook").textContent = d.recommended_runbook ? `Runbook: ${d.recommended_runbook}` : "No runbook matched.";
  $("step").textContent = d.recommended_step ?? "";
  $("evidence").replaceChildren(...(d.evidence ?? []).map((e) => {
    const li = document.createElement("li");
    li.append(text("span", e.source, "source"), document.createTextNode(e.statement ?? ""));
    return li;
  }));
  $("tools").textContent = JSON.stringify(d.tool_results ?? {}, null, 2);
  $("ack").disabled = d.status !== "proposed";
  $("detail").hidden = false;
}

async function acknowledge() {
  if (!selected) return;
  $("ack").disabled = true;
  try {
    await call(`${API}/${encodeURIComponent(selected)}/ack`, { method: "POST" });
    $("ack-message").textContent = "Acknowledged.";
    await loadList();
    await showDetail(selected);
    $("ack-message").textContent = "Acknowledged.";
  } catch (err) {
    $("ack-message").textContent = err.message;
  }
}

document.addEventListener("DOMContentLoaded", () => {
  $("status").addEventListener("change", loadList);
  $("ack").addEventListener("click", acknowledge);
  loadList();
});
