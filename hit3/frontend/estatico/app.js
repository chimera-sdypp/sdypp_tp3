// Página del Hit #3: manda jobs al backend (mismo host) y muestra su avance.
// El estado del job sale de GET /jobs/{id} cada 2 s; cada fragmento terminado llega al momento por
// /eventos/{id}, que el frontend alimenta con el fanout sobel.resultados.

const $ = (id) => document.getElementById(id);
const FINALES = ["done", "failed"];
const COLORES = ["#2f7ed8", "#e4572e", "#17a398", "#f3a712", "#8e44ad", "#c0392b", "#27ae60", "#d35400"];
const CADA_MS = 2000;

let token = leerToken();
let ofertas = {};
let actual = null; // el job que se está mirando

function leerToken() {
  try { return sessionStorage.getItem("token") || ""; } catch { return ""; }
}

function guardarToken(valor) {
  token = valor.trim().replace(/^Bearer\s+/i, "");
  try { sessionStorage.setItem("token", token); } catch { /* sin storage: dura hasta recargar */ }
  mostrarQuien();
  listarJobs();
}

function mostrarQuien() {
  try {
    const datos = JSON.parse(atob(token.split(".")[1].replace(/-/g, "+").replace(/_/g, "/")));
    const vence = new Date(datos.exp * 1000).toLocaleString();
    $("quien").textContent = `${datos.sub} (${datos.role}), vence ${vence}`;
  } catch {
    $("quien").textContent = token ? "el token no parece un JWT" : "sin token";
  }
}

// Pide a la API y devuelve `contenido`; si no es 2xx, lanza el error del contrato (§6) armado para leer.
async function api(ruta, opciones = {}) {
  const respuesta = await fetch(ruta, {...opciones, headers: {Authorization: `Bearer ${token}`, ...opciones.headers}});
  const sobre = await respuesta.json().catch(() => null);
  if (!respuesta.ok) {
    const error = sobre?.contenido?.error;
    const detalles = (error?.detalles || []).map((d) => `${d.campo}: ${d.problema}`).join("; ");
    throw new Error(error ? `${error.tipo}: ${error.mensaje}${detalles ? ` (${detalles})` : ""}` : `HTTP ${respuesta.status}`);
  }
  return sobre.contenido;
}

async function salud() {
  let estados = {};
  try {
    estados = (await (await fetch("/health")).json()).contenido || {};
  } catch {
    estados = {backend: "caido"};
  }
  $("salud").replaceChildren(...Object.entries(estados).map(([servicio, estado]) => {
    const li = document.createElement("li");
    li.textContent = servicio;
    li.className = estado === "ok" ? "ok" : "caido";
    li.title = estado;
    return li;
  }));
}

// --- pasos -----------------------------------------------------------------------------------------

async function cargarOfertas() {
  try {
    ofertas = (await (await fetch("/ofertas")).json()).contenido.ofertas ?? {}; // un 503 no trae ofertas
  } catch {
    ofertas = {};
  }
  for (const paso of $("pasos").children) llenarPaso(paso);
  if (!$("pasos").children.length) agregarPaso();
}

function agregarPaso() {
  if ($("pasos").children.length >= 3) return;
  const li = document.createElement("li");
  const imagen = document.createElement("select");
  const operacion = document.createElement("select");
  const quitar = document.createElement("button");
  imagen.className = "imagen";
  operacion.className = "operacion";
  quitar.type = "button";
  quitar.className = "chico";
  quitar.textContent = "quitar";
  imagen.addEventListener("change", () => llenarOperaciones(li));
  quitar.addEventListener("click", () => { if ($("pasos").children.length > 1) li.remove(); });
  li.append(operacion, imagen, quitar);
  $("pasos").append(li);
  llenarPaso(li);
}

function llenarPaso(li) {
  const select = li.querySelector(".imagen");
  const elegida = select.value;
  const imagenes = Object.keys(ofertas);
  select.replaceChildren(...(imagenes.length ? imagenes : ["(no hay workers activos)"]).map((i) => new Option(i, i)));
  if (imagenes.includes(elegida)) select.value = elegida;
  llenarOperaciones(li);
}

function llenarOperaciones(li) {
  const select = li.querySelector(".operacion");
  const elegida = select.value;
  const operaciones = ofertas[li.querySelector(".imagen").value] || [];
  select.replaceChildren(...operaciones.map((o) => new Option(o, o)));
  if (operaciones.includes(elegida)) select.value = elegida;
}

async function enviar(evento) {
  evento.preventDefault();
  $("error-job").textContent = "";
  const datos = new FormData();
  for (const archivo of $("archivos").files) datos.append("archivos", archivo);
  const pasos = [...$("pasos").children].map((li) => ({
    operacion: li.querySelector(".operacion").value, imagen: li.querySelector(".imagen").value}));
  datos.append("operaciones", JSON.stringify(pasos));
  $("enviar").disabled = true;
  try {
    const creado = await api("/jobs", {method: "POST", body: datos});
    mirar(creado.job_id);
    listarJobs();
  } catch (error) {
    $("error-job").textContent = error.message;
  } finally {
    $("enviar").disabled = false;
  }
}

// --- un job ----------------------------------------------------------------------------------------

function mirar(jobId) {
  if (actual) {
    actual.abortar.abort();
    clearTimeout(actual.temporizador);
    actual.urls.forEach(URL.revokeObjectURL);
  }
  actual = {id: jobId, total: 0, completos: 0, vistos: new Map(), colores: new Map(), urls: [],
            abortar: new AbortController(), temporizador: null, resultados: false};
  $("vista").hidden = false;
  $("job-id").textContent = jobId;
  for (const id of ["fragmentos", "leyenda", "resultados", "avisos"]) $(id).replaceChildren();
  $("error-vista").textContent = "";
  consultar(actual);
  escucharAvisos(actual);
}

async function consultar(mirado) {
  let job;
  try {
    job = await api(`/jobs/${mirado.id}`, {signal: mirado.abortar.signal});
  } catch (error) {
    if (mirado.abortar.signal.aborted) return;
    $("error-vista").textContent = error.message;
    mirado.temporizador = setTimeout(() => consultar(mirado), CADA_MS * 2);
    return;
  }
  if (mirado !== actual) return;
  mirado.total = job.fragmentos.total;
  mirado.completos = job.fragmentos.completos;
  const estado = $("estado");
  estado.textContent = job.estado;
  estado.className = `estado ${job.estado}`;
  $("avance").style.width = `${job.fragmentos.porcentaje}%`;
  const t = job.tiempos_s;
  $("resumen").textContent = [
    `${job.archivos} archivo(s), ${job.operaciones.map((p) => p.operacion).join(" → ")}`,
    `${job.fragmentos.completos}/${job.fragmentos.total} fragmentos (${job.fragmentos.porcentaje} %)`,
    job.fragmentos.en_dlq ? `${job.fragmentos.en_dlq} en la DLQ` : "",
    job.reintentos ? `${job.reintentos} reintento(s)` : "",
    t.total != null ? `total ${t.total} s (split ${t.split} s, cola ${t.cola} s, proceso ${t.procesamiento} s, join ${t.join} s)` : "",
  ].filter(Boolean).join(" · ");
  $("error-vista").textContent = job.error ? `${job.error.tipo}: ${job.error.mensaje}` : "";
  dibujar(mirado);
  if (job.estado === "done" && !mirado.resultados) {
    mirado.resultados = true;
    mostrarResultados(mirado, job.resultados);
  }
  if (FINALES.includes(job.estado)) {
    mirado.abortar.abort(); // corta también el flujo de avisos
    listarJobs();
    return;
  }
  mirado.temporizador = setTimeout(() => consultar(mirado), CADA_MS);
}

// Los avisos vienen por fetch y no por EventSource porque hace falta mandar el token en una cabecera.
async function escucharAvisos(mirado) {
  try {
    const respuesta = await fetch(`/eventos/${mirado.id}`, {
      headers: {Authorization: `Bearer ${token}`}, signal: mirado.abortar.signal});
    if (!respuesta.ok) throw new Error(`HTTP ${respuesta.status}`);
    const lector = respuesta.body.pipeThrough(new TextDecoderStream()).getReader();
    let pendiente = "";
    for (;;) {
      const {value, done} = await lector.read();
      if (done) break;
      pendiente += value;
      const eventos = pendiente.split("\n\n");
      pendiente = eventos.pop();
      for (const evento of eventos) {
        const datos = evento.split("\n").filter((l) => l.startsWith("data: ")).map((l) => l.slice(6)).join("");
        if (datos) recibir(mirado, JSON.parse(datos));
      }
    }
  } catch {
    if (mirado.abortar.signal.aborted) return;
  }
  setTimeout(() => { if (!mirado.abortar.signal.aborted) escucharAvisos(mirado); }, CADA_MS);
}

function recibir(mirado, resultado) {
  if (resultado.fragmento === undefined) {
    if (resultado.completos !== undefined) mirado.completos = resultado.completos;
    if (resultado.total !== undefined) mirado.total = resultado.total;
    if (resultado.porcentaje !== undefined) $("avance").style.width = `${resultado.porcentaje}%`;
    if (resultado.estado) {
      const estado = $("estado");
      estado.textContent = resultado.estado;
      estado.className = `estado ${resultado.estado}`;
    }
    dibujar(mirado);
    return;
  }
  const clave = `${resultado.archivo}:${resultado.fragmento}`;
  if (mirado.vistos.has(clave)) return; // una reentrega repite el aviso
  mirado.vistos.set(clave, resultado.worker);
  if (!mirado.colores.has(resultado.worker)) {
    mirado.colores.set(resultado.worker, COLORES[mirado.colores.size % COLORES.length]);
  }
  const li = document.createElement("li");
  li.textContent = `archivo ${resultado.archivo} · fragmento ${resultado.fragmento} · ${resultado.worker} · ` +
                   `${resultado.ms} ms${resultado.intento > 1 ? ` · intento ${resultado.intento}` : ""}`;
  $("avisos").prepend(li);
  while ($("avisos").children.length > 50) $("avisos").lastChild.remove();
  dibujar(mirado);
}

// Una celda por fragmento, en el orden en que terminaron, del color del worker que hizo el último paso.
// Las que el joiner contó pero llegaron antes de abrir la página quedan grises.
function dibujar(mirado) {
  const celdas = $("fragmentos");
  const total = Math.max(mirado.total, mirado.vistos.size);
  while (celdas.children.length < total) celdas.append(document.createElement("span"));
  const workers = [...mirado.vistos.values()];
  [...celdas.children].forEach((celda, i) => {
    celda.style.background = i < workers.length ? mirado.colores.get(workers[i]) : "";
    celda.className = i >= workers.length && i < mirado.completos ? "sin-aviso" : "";
    celda.title = i < workers.length ? workers[i] : "";
  });
  const cuentas = new Map();
  for (const w of workers) cuentas.set(w, (cuentas.get(w) || 0) + 1);
  $("leyenda").replaceChildren(...[...cuentas].map(([worker, n]) => {
    const li = document.createElement("li");
    const muestra = document.createElement("span");
    muestra.style.background = mirado.colores.get(worker);
    li.append(muestra, `${worker}: ${n}`);
    return li;
  }));
}

async function mostrarResultados(mirado, rutas) {
  for (const [n, ruta] of rutas.entries()) {
    try {
      const respuesta = await fetch(ruta, {headers: {Authorization: `Bearer ${token}`}});
      if (!respuesta.ok) throw new Error(`HTTP ${respuesta.status}`);
      const url = URL.createObjectURL(await respuesta.blob());
      if (mirado !== actual) return URL.revokeObjectURL(url);
      mirado.urls.push(url);
      const enlace = document.createElement("a");
      enlace.href = url;
      enlace.download = `${mirado.id}-${n}.png`;
      const img = document.createElement("img");
      img.src = url;
      img.alt = `resultado ${n}`;
      enlace.append(img);
      $("resultados").append(enlace);
    } catch (error) {
      $("error-vista").textContent = `no se pudo bajar el resultado ${n}: ${error.message}`;
    }
  }
}

async function listarJobs() {
  if (!token) return $("jobs").replaceChildren();
  try {
    const {jobs} = await api("/jobs");
    $("jobs").replaceChildren(...jobs.map((job) => {
      const li = document.createElement("li");
      const boton = document.createElement("button");
      boton.type = "button";
      boton.className = "enlace";
      boton.textContent = job.job_id;
      boton.addEventListener("click", () => mirar(job.job_id));
      const estado = document.createElement("span");
      estado.className = `estado ${job.estado}`;
      estado.textContent = job.estado;
      li.append(boton, estado, ` ${new Date(job.creado).toLocaleString()}${job.usuario ? ` · ${job.usuario}` : ""}`);
      return li;
    }));
  } catch (error) {
    const li = document.createElement("li");
    li.className = "error";
    li.textContent = error.message;
    $("jobs").replaceChildren(li);
  }
}

$("form-token").addEventListener("submit", (e) => { e.preventDefault(); guardarToken($("token").value); });
$("form-job").addEventListener("submit", enviar);
$("agregar-paso").addEventListener("click", agregarPaso);
$("refrescar").addEventListener("click", () => { listarJobs(); cargarOfertas(); });
$("token").value = token;
mostrarQuien();
salud();
setInterval(salud, 10000);
cargarOfertas();
listarJobs();
