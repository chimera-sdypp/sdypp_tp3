import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Rate, Counter } from 'k6/metrics';

// ============================================================================
// Métricas personalizadas para validación estricta de SLOs (Sección 8 Contrato)
// ============================================================================
// SLO 1: p95 de POST /jobs <= 1s
const postJobsDuration = new Trend('post_jobs_duration', true);
const sloPostOk = new Rate('slo_post_ok');

// SLO 2: p95 tiempos_s.total <= 30s (<=10MB) o <= 120s (>10MB)
const jobTotalDuration = new Trend('job_total_duration', true);
const sloTotalOk = new Rate('slo_total_ok');

// SLO 3: menos de 1% de errores entre pedidos válidos (5xx, failed o dlq no provocados)
const sloErrors = new Rate('slo_unexpected_errors');

// Métricas para casos inválidos dentro de la carga (R5)
const botRequestsHandled = new Counter('bot_requests_handled');
const invalidRequestsHandled = new Counter('invalid_requests_handled');

// ============================================================================
// Parámetros y Dimensiones de la Matriz R5 (vía variables de entorno)
// ============================================================================
const TARGET_URL = __ENV.TARGET_URL || 'http://localhost:8080';
const VA = parseInt(__ENV.VA || '1', 10);          // Archivos por job: 1, 10, 50
const VB = parseInt(__ENV.VB || '1', 10);          // Operaciones por archivo: 1, 2, 3
const VC = parseInt(__ENV.VC || '1', 10);          // Concurrencia (VUs): 1, 10, 50
const V1 = __ENV.V1 || '1mb';                      // Tamaño: 1kb, 1mb, 10mb, 50mb
const V3 = __ENV.V3 || '2';                        // Workers activos (meta-tag)

const DURATION = __ENV.DURATION || '30s';
const RAMP_UP = __ENV.RAMP_UP || '5s';
const POLL_JOB = __ENV.POLL_JOB !== 'false';
const MAX_POLL_TIMEOUT_S = parseInt(__ENV.MAX_POLL_TIMEOUT_S || '45', 10);
const DEFAULT_IMAGE_REF = __ENV.WORKER_IMAGE || 'ghcr.io/chimera-sdypp/sobel-worker:1.0.0';

// Umbral SLO según tamaño total de imagen
const SLO_TOTAL_THRESHOLD_MS = (V1 === '50mb' || (V1 === '10mb' && VA > 1)) ? 120000 : 30000;

export const options = {
  stages: [
    { duration: RAMP_UP, target: VC },      // Rampa de subida
    { duration: DURATION, target: VC },     // Carga sostenida
    { duration: '5s', target: 0 },          // Rampa de bajada
  ],
  thresholds: {
    // SLO 1: p95 POST /jobs <= 1000 ms
    post_jobs_duration: ['p(95)<1000'],
    // SLO 3: tasa de error < 1%
    slo_unexpected_errors: ['rate<0.01'],
    // Al menos 95% de los POST dentro del SLO
    slo_post_ok: ['rate>=0.95'],
  },
  tags: {
    dimension_va: `${VA}`,
    dimension_vb: `${VB}`,
    dimension_vc: `${VC}`,
    dimension_v1: `${V1}`,
    dimension_v3: `${V3}`,
  },
};

// ============================================================================
// Carga de Tokens e Imágenes
// ============================================================================
let tokens;
try {
  tokens = JSON.parse(open('./datos/tokens.json'));
} catch (_) {
  tokens = { usuarios: { user_01: 'dummy' }, admin: 'dummy', expirado: 'dummy' };
}

// Cargar imagen según dimensión V1
let imgBytes;
try {
  imgBytes = open(`./datos/img_${V1}.png`, 'b');
} catch (_) {
  try {
    imgBytes = open('./datos/img_1kb.png', 'b');
  } catch (err) {
    imgBytes = null;
  }
}

let imgCorrupta;
try {
  imgCorrupta = open('./datos/corrupta.png', 'b');
} catch (_) {
  imgCorrupta = null;
}

// Lista de usuarios disponibles para simular VC usuarios reales
const userKeys = Object.keys(tokens.usuarios || {});

// Construcción de la cadena de operaciones según dimensión VB
function construirOperaciones(vb, invalidOp = false) {
  if (invalidOp) {
    return JSON.stringify([{ operacion: 'rotar_invalida', imagen: DEFAULT_IMAGE_REF }]);
  }
  const ops = [];
  if (vb === 1) {
    ops.push({ operacion: 'sobel', imagen: DEFAULT_IMAGE_REF });
  } else if (vb === 2) {
    ops.push({ operacion: 'grises', imagen: DEFAULT_IMAGE_REF });
    ops.push({ operacion: 'sobel', imagen: DEFAULT_IMAGE_REF });
  } else {
    ops.push({ operacion: 'grises', imagen: DEFAULT_IMAGE_REF });
    ops.push({ operacion: 'blur', imagen: DEFAULT_IMAGE_REF });
    ops.push({ operacion: 'sobel', imagen: DEFAULT_IMAGE_REF });
  }
  return JSON.stringify(ops);
}

// ============================================================================
// Función de Iteración de Carga
// ============================================================================
export default function () {
  // Asignar usuario según VU actual
  const userIdx = (__VU - 1) % (userKeys.length || 1);
  const userKey = userKeys[userIdx] || 'user_01';
  const token = (tokens.usuarios && tokens.usuarios[userKey]) ? tokens.usuarios[userKey] : 'token_invalido';

  // Simulación de casos de prueba dentro de la carga (10% total de casos inválidos)
  const rand = Math.random();

  // Caso 1: Bot sin token (5% de probabilidad) -> debe responder 401 NO_AUTORIZADO
  if (rand < 0.05) {
    const res = http.post(`${TARGET_URL}/jobs`, {});
    const is401 = res.status === 401;
    check(res, {
      'Bot sin token recibe 401': () => is401,
    });
    if (is401) {
      botRequestsHandled.add(1);
    } else {
      sloErrors.add(1);
    }
    sleep(1);
    return;
  }

  // Caso 2: Operación no soportada o archivo corrupto (5% de probabilidad) -> debe responder 422
  if (rand >= 0.05 && rand < 0.10 && imgCorrupta) {
    const payload = {
      operaciones: construirOperaciones(VB, true),
      archivos: http.file(imgCorrupta, 'corrupta.png', 'image/png'),
    };
    const params = {
      headers: {
        Authorization: `Bearer ${token}`,
      },
      tags: { tipo: 'caso_invalido' },
    };
    const res = http.post(`${TARGET_URL}/jobs`, payload, params);
    const is422 = res.status === 422;
    check(res, {
      'Caso inválido recibe 422': () => is422,
    });
    if (is422) {
      invalidRequestsHandled.add(1);
    } else {
      sloErrors.add(1);
    }
    sleep(1);
    return;
  }

  // ==========================================================================
  // Petición Válida según parámetros VA x VB x VC x V1
  // ==========================================================================
  if (!imgBytes) {
    console.error('Error: no se encontró archivo de imagen para V1=' + V1);
    sleep(1);
    return;
  }

  // Construir multipart con VA archivos
  const fileArray = [];
  for (let i = 0; i < VA; i++) {
    fileArray.push(http.file(imgBytes, `imagen_${i}.png`, 'image/png'));
  }

  const payload = {
    operaciones: construirOperaciones(VB, false),
    archivos: (VA === 1) ? fileArray[0] : fileArray,
  };

  const params = {
    headers: {
      Authorization: `Bearer ${token}`,
    },
    tags: { name: 'POST_jobs' },
  };

  const startTime = Date.now();
  const res = http.post(`${TARGET_URL}/jobs`, payload, params);
  const postDuration = Date.now() - startTime;

  // Registrar latencia de POST
  postJobsDuration.add(postDuration);
  sloPostOk.add(postDuration <= 1000);

  const postSuccess = check(res, {
    'POST /jobs responde 202': (r) => r.status === 202,
    'POST /jobs devuelve job_id': (r) => {
      try {
        return r.json().codigo === 202 && r.json().contenido.job_id !== undefined;
      } catch (_) {
        return false;
      }
    },
  });

  if (!postSuccess) {
    sloErrors.add(1);
    sleep(1);
    return;
  }

  // Sin errores en POST
  sloErrors.add(0);

  // Si no se pide sondeo e2e, terminamos aquí la iteración
  if (!POLL_JOB) {
    sleep(1);
    return;
  }

  // ==========================================================================
  // Sondeo de estado del Job hasta DONE (para medir SLO 2: tiempos_s.total)
  // ==========================================================================
  const jobId = res.json().contenido.job_id;
  let jobFinished = false;
  let pollAttempts = 0;
  const pollStart = Date.now();

  while (!jobFinished && pollAttempts < MAX_POLL_TIMEOUT_S) {
    sleep(1);
    pollAttempts++;

    const pollRes = http.get(`${TARGET_URL}/jobs/${jobId}`, params);
    if (pollRes.status === 200) {
      try {
        const body = pollRes.json();
        const estado = body.contenido.estado;

        if (estado === 'done') {
          jobFinished = true;
          // Si el backend reporta tiempos_s.total, usamos ese dato oficial; si no, el tiempo de sondeo
          const e2eSeconds = body.contenido.tiempos_s && body.contenido.tiempos_s.total
            ? body.contenido.tiempos_s.total
            : (Date.now() - startTime) / 1000;

          const e2eMs = e2eSeconds * 1000;
          jobTotalDuration.add(e2eMs);
          sloTotalOk.add(e2eMs <= SLO_TOTAL_THRESHOLD_MS);
          sloErrors.add(0);
          break;
        } else if (estado === 'failed' || estado === 'dlq') {
          // Fallo inesperado en pedido válido
          jobFinished = true;
          sloErrors.add(1);
          console.warn(`[WARN] Job ${jobId} terminó en estado: ${estado}`);
          break;
        }
      } catch (_) {}
    } else if (pollRes.status >= 500) {
      sloErrors.add(1);
    }
  }

  if (!jobFinished) {
    console.warn(`[TIMEOUT] Job ${jobId} no completó en ${MAX_POLL_TIMEOUT_S}s`);
    sloTotalOk.add(false);
    sloErrors.add(1);
  }
}
