// ==============================================================================
// matriz_r5.js — Matriz de Pruebas de Carga k6 para Hit #3 (R5)
// Sistemas Distribuidos y Paralelos — Grupo Cerberus (Salva / Salvador Baez)
// ==============================================================================
// Propósito del archivo:
// Este script de k6 evalúa el comportamiento y la escalabilidad del sistema
// distribuido de procesamiento de imágenes bajo diferentes niveles de carga,
// correspondientes a las dimensiones del Requerimiento R5 y la Sección 8 del Contrato:
//   - VA: Cantidad de archivos por job (1, 10, 50).
//   - VB: Cadena de operaciones a aplicar por archivo (1=sobel, 2=grises+sobel, 3=grises+blur+sobel).
//   - VC: Nivel de concurrencia de Usuarios Virtuales (VUs) simultáneos (1, 10, 50).
//   - V1: Tamaño de las imágenes procesadas (1kb, 1mb, 10mb, 50mb).
//   - V3: Cantidad de réplicas de workers en ejecución (meta-etiqueta k6).
// Además, valida estrictamente los 3 SLOs definidos en el Contrato:
//   - SLO 1: p95 de latencia de recepción POST /jobs <= 1000 ms.
//   - SLO 2: p95 de tiempo de procesamiento distribuido total <= 30s (<=10MB) o <= 120s (>10MB).
//   - SLO 3: Tasa de errores no provocados < 1% en pedidos válidos.
// ==============================================================================

// 1. Importación del módulo HTTP de k6 para realizar peticiones de red (GET, POST multipart).
import http from 'k6/http';

// 2. Importación de 'check' para aserciones booleanas y 'sleep' para pausas de ritmo entre peticiones.
import { check, sleep } from 'k6';

// 3. Importación de tipos de métricas personalizadas de k6:
//    - Trend: para registrar histogramas y calcular percentiles (p90, p95, p99).
//    - Rate: para calcular porcentajes de éxito/fracaso (valores entre 0.0 y 1.0).
//    - Counter: para contabilizar eventos discretos acumulados durante la ejecución.
import { Trend, Rate, Counter } from 'k6/metrics';

// ==============================================================================
// Métricas personalizadas para validación estricta de SLOs (Sección 8 del Contrato)
// ==============================================================================

// Métrica de tendencia para registrar la latencia de respuesta inmediata del endpoint POST /jobs.
// El segundo parámetro 'true' indica que es una métrica de tiempo (milisegundos).
const postJobsDuration = new Trend('post_jobs_duration', true);

// Métrica de tasa para medir el cumplimiento del SLO 1 (proporción de POSTs <= 1000 ms).
const sloPostOk = new Rate('slo_post_ok');

// Métrica de tendencia para registrar la duración completa de procesamiento distribuido (tiempos_s.total).
const jobTotalDuration = new Trend('job_total_duration', true);

// Métrica de tasa para medir el cumplimiento del SLO 2 (proporción de jobs que completan en tiempo pactado).
const sloTotalOk = new Rate('slo_total_ok');

// Métrica de tasa para medir errores inesperados o fallos 5xx en pedidos válidos (SLO 3).
const sloErrors = new Rate('slo_unexpected_errors');

// Contadores para auditar la cantidad de solicitudes inválidas inyectadas sintéticamente:
// - Peticiones no autenticadas (bots) que deben responder 401 NO_AUTORIZADO.
const botRequestsHandled = new Counter('bot_requests_handled');
// - Peticiones con errores de validación (operación inexistente o archivo corrupto) que deben responder 422.
const invalidRequestsHandled = new Counter('invalid_requests_handled');

// ==============================================================================
// Parámetros y Dimensiones de la Matriz R5 (configurables vía variables de entorno)
// ==============================================================================

// URL base del servicio. Por defecto apunta al ingress / balanceador Traefik local (puerto 80).
const TARGET_URL = __ENV.TARGET_URL || 'http://localhost';

// Dimensión VA: Cantidad de imágenes subidas en una misma petición (1, 10, o 50).
const VA = parseInt(__ENV.VA || '1', 10);

// Dimensión VB: Longitud del pipeline de transformaciones (1: sobel; 2: grises+sobel; 3: grises+blur+sobel).
const VB = parseInt(__ENV.VB || '1', 10);

// Dimensión VC: Concurrencia de usuarios virtuales (VUs) simultáneos (1, 10, o 50).
const VC = parseInt(__ENV.VC || '1', 10);

// Dimensión V1: Calibre del archivo de imagen ('1kb', '1mb', '10mb', o '50mb').
const V1 = __ENV.V1 || '1mb';

// Dimensión V3: Cantidad de réplicas de workers activas (se etiqueta en k6 para correlación en Grafana).
const V3 = __ENV.V3 || '2';

// Duración de la fase de carga sostenida (por defecto 30 segundos para pruebas automáticas).
const DURATION = __ENV.DURATION || '30s';

// Duración de la rampa inicial para elevar suavemente los VUs hasta el objetivo (por defecto 5 segundos).
const RAMP_UP = __ENV.RAMP_UP || '5s';

// Bandera booleana para decidir si cada VU sondea el estado del job hasta su finalización (default: true).
const POLL_JOB = __ENV.POLL_JOB !== 'false';

// Límite máximo de tiempo en segundos para esperar que un job pase a estado 'done' (timeout de sondeo).
const MAX_POLL_TIMEOUT_S = parseInt(__ENV.MAX_POLL_TIMEOUT_S || '45', 10);

// Referencia de la imagen OCI del worker a utilizar en el pipeline (según Contrato Sección 4).
const DEFAULT_IMAGE_REF = __ENV.WORKER_IMAGE || 'ghcr.io/chimera-sdypp/sobel-worker:1.0.0';

// Cálculo del umbral máximo tolerable en milisegundos para el SLO 2:
// - Pedidos mayores a 10 MB (ej: V1='50mb' o V1='10mb' con múltiples archivos VA>1): límite 120 segundos.
// - Pedidos menores o iguales a 10 MB: límite 30 segundos (30.000 ms).
const SLO_TOTAL_THRESHOLD_MS = (V1 === '50mb' || (V1 === '10mb' && VA > 1)) ? 120000 : 30000;

// ==============================================================================
// Configuración de Ejecución de k6 (Options)
// ==============================================================================
export const options = {
  // Definición de etapas (stages) para generar la rampa de carga:
  stages: [
    { duration: RAMP_UP, target: VC },      // 1. Rampa de subida: alcanzar VC usuarios concurrentes.
    { duration: DURATION, target: VC },     // 2. Meseta estable: mantener VC usuarios bajo tráfico sostenido.
    { duration: '5s', target: 0 },          // 3. Rampa de bajada: desconexión ordenada de los usuarios virtuales.
  ],
  // Umbrales formales (thresholds) que dictan si la prueba pasa o reprueba en CI/CD:
  thresholds: {
    // SLO 1: El percentil 95 del tiempo de aceptación en POST /jobs debe ser menor a 1000 milisegundos.
    post_jobs_duration: ['p(95)<1000'],
    // SLO 3: La tasa de errores inesperados (HTTP 5xx o jobs en failed/dlq no provocados) debe ser < 1%.
    slo_unexpected_errors: ['rate<0.01'],
    // Criterio de consistencia: Al menos el 95% de los POST deben completarse dentro del límite aceptable.
    slo_post_ok: ['rate>=0.95'],
  },
  // Metadatos y etiquetas para catalogar los resultados de la prueba en Prometheus / InfluxDB:
  tags: {
    dimension_va: `${VA}`,
    dimension_vb: `${VB}`,
    dimension_vc: `${VC}`,
    dimension_v1: `${V1}`,
    dimension_v3: `${V3}`,
  },
};

// ==============================================================================
// Precarga de Datos de Prueba (Tokens e Imágenes Sintéticas)
// ==============================================================================

// Carga del archivo de tokens JWT pre-generados por generar_datos.py
let tokens;
try {
  // Lee de forma síncrona el JSON con tokens emitidos mediante HMAC-SHA256
  tokens = JSON.parse(open('./datos/tokens.json'));
} catch (_) {
  // Mecanismo de resguardo si no se ejecutó previamente generar_datos.py
  tokens = { usuarios: { user_01: 'dummy' }, admin: 'dummy', expirado: 'dummy' };
}

// Carga en memoria binaria del archivo de imagen correspondiente al tamaño V1 seleccionado
let imgBytes;
try {
  imgBytes = open(`./datos/img_${V1}.png`, 'b');
} catch (_) {
  // Advertencia informativa en consola si la imagen solicitada aún no fue sintetizada
  console.warn(`[ADVERTENCIA] No se encontró ./datos/img_${V1}.png. Reintentando con img_1kb.png.`);
  try {
    imgBytes = open('./datos/img_1kb.png', 'b');
  } catch (err) {
    imgBytes = null; // En caso extremo sin imágenes, se controlará en la iteración
  }
}

// Carga de la imagen deliberadamente corrupta para probar el rechazo estricto del backend
let imgCorrupta;
try {
  imgCorrupta = open('./datos/corrupta.png', 'b');
} catch (_) {
  imgCorrupta = null;
}

// Extrae el listado de identificadores de usuarios ('user_01', 'user_02', ...) para rotar VUs
const userKeys = Object.keys(tokens.usuarios || {});

// ==============================================================================
// Función Auxiliar: Construcción del Pipeline de Operaciones (Dimensión VB)
// ==============================================================================
// Serializa en JSON la lista de operaciones según lo estipulado en la Sección 4 del Contrato:
// - VB = 1: Un solo paso ('sobel').
// - VB = 2: Dos pasos encadenados ('grises' -> 'sobel').
// - VB = 3: Tres pasos encadenados ('grises' -> 'blur' -> 'sobel').
// - invalidOp = true: Inyecta una operación inexistente ('rotar_invalida') para comprobar validación.
function construirOperaciones(vb, invalidOp = false) {
  if (invalidOp) {
    // Caso de error deliberado: debe provocar 422 OPERACION_NO_SOPORTADA
    return JSON.stringify([{ operacion: 'rotar_invalida', imagen: DEFAULT_IMAGE_REF }]);
  }
  const ops = [];
  if (vb === 1) {
    // 1 sola operación: detección de bordes directa con Sobel
    ops.push({ operacion: 'sobel', imagen: DEFAULT_IMAGE_REF });
  } else if (vb === 2) {
    // 2 operaciones: conversión a escala de grises previa al filtro de Sobel
    ops.push({ operacion: 'grises', imagen: DEFAULT_IMAGE_REF });
    ops.push({ operacion: 'sobel', imagen: DEFAULT_IMAGE_REF });
  } else {
    // 3 operaciones: pre-procesamiento con grises, suavizado gaussiano blur, y Sobel final
    ops.push({ operacion: 'grises', imagen: DEFAULT_IMAGE_REF });
    ops.push({ operacion: 'blur', imagen: DEFAULT_IMAGE_REF });
    ops.push({ operacion: 'sobel', imagen: DEFAULT_IMAGE_REF });
  }
  return JSON.stringify(ops);
}

// ==============================================================================
// Función Principal de Iteración k6 (Ejecutada por cada Virtual User)
// ==============================================================================
export default function () {
  // Asignar determinísticamente un usuario diferente a cada VU según su ID (__VU es 1-indexed)
  const userIdx = (__VU - 1) % (userKeys.length || 1);
  const userKey = userKeys[userIdx] || 'user_01';
  // Extraer el token JWT válido correspondiente al usuario asignado
  const token = (tokens.usuarios && tokens.usuarios[userKey]) ? tokens.usuarios[userKey] : 'token_invalido';

  // Generación de un número aleatorio uniforme [0.0, 1.0) para inyectar tráfico anómalo realista:
  // El Contrato y el Requerimiento R5 exigen verificar que el sistema maneje bots y pedidos malformados
  // sin degradar su servicio ni registrar falsos positivos de indisponibilidad.
  const rand = Math.random();

  // ----------------------------------------------------------------------------
  // Caso de Prueba 1: Bot sin autenticación (5% de probabilidad)
  // ----------------------------------------------------------------------------
  // Objetivo: Verificar que el middleware de autenticación rechace inmediatamente con HTTP 401.
  if (rand < 0.05) {
    // Envía petición POST /jobs sin cabecera 'Authorization'
    const res = http.post(`${TARGET_URL}/jobs`, {});
    const is401 = res.status === 401;
    // Aserción k6: validar que el código de respuesta sea efectivamente 401
    check(res, {
      'Bot sin token recibe 401': () => is401,
    });
    if (is401) {
      // Éxito en la validación: el backend protegió el recurso correctamente
      botRequestsHandled.add(1);
    } else {
      // Si respondió 200, 500 u otro, es una brecha de seguridad o fallo no deseado
      sloErrors.add(1);
    }
    sleep(1); // Pausa de respiración antes de la siguiente iteración
    return;
  }

  // ----------------------------------------------------------------------------
  // Caso de Prueba 2: Operación no soportada con archivo válido (4% de probabilidad)
  // ----------------------------------------------------------------------------
  // Objetivo: Comprobar que el validador semántico devuelva HTTP 422 OPERACION_NO_SOPORTADA.
  if (rand >= 0.05 && rand < 0.09 && imgBytes) {
    const payload = {
      operaciones: JSON.stringify([{ operacion: 'rotar_invalida', imagen: DEFAULT_IMAGE_REF }]),
      archivos: http.file(imgBytes, 'test.png', 'image/png'),
    };
    const params = {
      headers: {
        Authorization: `Bearer ${token}`, // Autenticación válida
      },
      tags: { tipo: 'caso_op_invalida' },
    };
    const res = http.post(`${TARGET_URL}/jobs`, payload, params);
    const is422 = res.status === 422;
    check(res, {
      'Operación inválida recibe 422': () => is422,
    });
    if (is422) {
      invalidRequestsHandled.add(1);
    } else {
      sloErrors.add(1);
    }
    sleep(1);
    return;
  }

  // ----------------------------------------------------------------------------
  // Caso de Prueba 3: Archivo corrupto con operaciones válidas (4% de probabilidad)
  // ----------------------------------------------------------------------------
  // Objetivo: Comprobar que la validación de cabeceras de imagen devuelva HTTP 422 ARCHIVO_INVALIDO.
  if (rand >= 0.09 && rand < 0.13 && imgCorrupta) {
    const payload = {
      operaciones: construirOperaciones(VB, false),
      archivos: http.file(imgCorrupta, 'corrupta.png', 'image/png'),
    };
    const params = {
      headers: {
        Authorization: `Bearer ${token}`,
      },
      tags: { tipo: 'caso_archivo_corrupto' },
    };
    const res = http.post(`${TARGET_URL}/jobs`, payload, params);
    const is422 = res.status === 422;
    check(res, {
      'Archivo corrupto recibe 422': () => is422,
    });
    if (is422) {
      invalidRequestsHandled.add(1);
    } else {
      sloErrors.add(1);
    }
    sleep(1);
    return;
  }

  // ============================================================================
  // Petición Válida según parámetros VA x VB x VC x V1 (Tráfico Nominal del SLO)
  // ============================================================================

  // Verificación de resguardo: confirmar que el buffer de imagen esté disponible
  if (!imgBytes) {
    console.error(`[ERROR] No se dispone de la imagen para la dimensión V1=${V1}`);
    sleep(1);
    return;
  }

  // Construcción del arreglo de archivos multipart según la dimensión VA (1, 10 o 50 imágenes)
  const fileArray = [];
  for (let i = 0; i < VA; i++) {
    // Empaqueta cada imagen con un nombre distintivo y tipo MIME 'image/png'
    fileArray.push(http.file(imgBytes, `imagen_${i}.png`, 'image/png'));
  }

  // Estructura del cuerpo multipart/form-data:
  // - 'operaciones': cadena JSON con el arreglo de transformaciones.
  // - 'archivos': un único archivo si VA=1, o un arreglo de archivos si VA > 1.
  const payload = {
    operaciones: construirOperaciones(VB, false),
    archivos: (VA === 1) ? fileArray[0] : fileArray,
  };

  // Cabeceras HTTP: Portador de credencial JWT y etiqueta para métricas de k6
  const params = {
    headers: {
      Authorization: `Bearer ${token}`,
    },
    tags: { name: 'POST_jobs' },
  };

  // Medición de alta precisión de la latencia de aceptación del backend
  const startTime = Date.now();
  // Envío de la petición HTTP POST asíncrona hacia el endpoint de ingreso
  const res = http.post(`${TARGET_URL}/jobs`, payload, params);
  const postDuration = Date.now() - startTime;

  // Registro del tiempo de respuesta en el histograma de latencia de k6
  postJobsDuration.add(postDuration);
  // Evaluación del SLO 1: ¿La respuesta tardó 1000 milisegundos o menos? (true = 1.0, false = 0.0)
  sloPostOk.add(postDuration <= 1000);

  // Aserciones de contrato sobre la respuesta inmediata del backend:
  // 1. Debe retornar código HTTP 202 Accepted.
  // 2. Debe contener en su cuerpo JSON la clave 'job_id'.
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

  // Si la petición POST no fue exitosa (falló validación, timeout o 5xx):
  if (!postSuccess) {
    // Contabiliza como fallo del SLO 3 (error imprevisto en petición válida)
    sloErrors.add(1);
    sleep(1);
    return; // Aborta el sondeo de este job para no procesar un ID inexistente
  }

  // Si el POST fue aceptado correctamente, registramos un 0 en errores
  sloErrors.add(0);

  // Si el usuario configuró POLL_JOB=false (modo solo ingesta), finaliza aquí
  if (!POLL_JOB) {
    sleep(1);
    return;
  }

  // ============================================================================
  // Sondeo Reactivo del Estado del Job (Medición de SLO 2: Procesamiento Total)
  // ============================================================================

  // Extrae el identificador único del trabajo distribuido recién creado
  const jobId = res.json().contenido.job_id;
  let jobFinished = false;
  let pollAttempts = 0;

  // Bucle de consulta de estado periódica (polling) cada 1 segundo:
  while (!jobFinished && pollAttempts < MAX_POLL_TIMEOUT_S) {
    // Pausa de 1 segundo entre consultas sucesivas para no saturar la red ni la base de datos
    sleep(1);
    pollAttempts++;

    // Consulta GET /jobs/{jobId} transmitiendo el token del usuario creador
    const pollRes = http.get(`${TARGET_URL}/jobs/${jobId}`, params);

    if (pollRes.status === 200) {
      try {
        const body = pollRes.json();
        const estado = body.contenido.estado;

        // Caso A: El procesamiento distribuido completó con éxito
        if (estado === 'done') {
          jobFinished = true;
          // Si el backend calcula y retorna 'tiempos_s.total', usamos esa métrica oficial del servidor;
          // de lo contrario, calculamos el delta transcurrido desde el inicio del POST en el cliente.
          const e2eSeconds = body.contenido.tiempos_s && body.contenido.tiempos_s.total
            ? body.contenido.tiempos_s.total
            : (Date.now() - startTime) / 1000;

          const e2eMs = e2eSeconds * 1000;
          // Registra la duración en el histograma de procesamiento total
          jobTotalDuration.add(e2eMs);
          // Evalúa si el tiempo total cumple el umbral estipulado por el SLO 2 (30s o 120s)
          sloTotalOk.add(e2eMs <= SLO_TOTAL_THRESHOLD_MS);
          // Confirma que no hubo error no provocado
          sloErrors.add(0);
          break;
        } else if (estado === 'failed' || estado === 'dlq') {
          // Caso B: El trabajo falló internamente o fue enviado a Dead Letter Queue inesperadamente
          jobFinished = true;
          sloErrors.add(1); // Computa como infracción del SLO 3
          console.warn(`[WARN] Job ${jobId} finalizó inesperadamente en estado: ${estado}`);
          break;
        }
        // Si el estado es 'pending' o 'processing', el bucle continúa en la siguiente iteración
      } catch (_) {
        // En caso de parseo JSON erróneo del cuerpo de sondeo
      }
    } else if (pollRes.status >= 500) {
      // Si el endpoint de consulta devuelve un error 5xx de servidor, cuenta como fallo
      sloErrors.add(1);
    }
  }

  // Si el ciclo agotó todos los intentos sin que el job alcanzara estado terminal
  if (!jobFinished) {
    console.warn(`[TIMEOUT] Job ${jobId} no completó su ejecución tras ${MAX_POLL_TIMEOUT_S}s de sondeo.`);
    // Infracción del SLO 2 por vencimiento de plazo
    sloTotalOk.add(false);
    // Infracción del SLO 3 por procesamiento no completado
    sloErrors.add(1);
  }
}
