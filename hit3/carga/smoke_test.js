// ==============================================================================
// smoke_test.js — Test de Humo y Verificación Rápida de Contrato de API (Hit #3)
// Sistemas Distribuidos y Paralelos — Grupo Cerberus (Salva / Salvador Baez)
// ==============================================================================
// Propósito del archivo:
// Este script k6 ejecuta una pasada rápida y determinística (1 VU, 1 iteración)
// sobre los endpoints principales del sistema para validar el cumplimiento del
// Contrato de la API antes de lanzar pruebas de carga masivas o despliegues:
//   1. Verificación del endpoint público GET /health (debe responder 200 OK).
//   2. Verificación de rechazo por falta de credenciales (POST /jobs sin auth -> 401).
//   3. Verificación de rechazo por credenciales vencidas (POST /jobs expirado -> 401).
//   4. Verificación de validación semántica de operaciones (operación inválida -> 422).
//   5. Verificación de validación de formato de archivos (imagen corrupta -> 422).
//   6. Flujo completo feliz (Happy Path): creación de trabajo (202 Accepted) y
//      sondeo periódico de estado en GET /jobs/{id} hasta alcanzar estado 'done'.
// ==============================================================================

// 1. Módulo HTTP de k6 para emisión de peticiones HTTP/1.1
import http from 'k6/http';

// 2. Funciones de aserción ('check') y espera de tiempo ('sleep')
import { check, sleep } from 'k6';

// 3. Métricas de k6 para contabilizar tasa de éxito de contrato y fallos inesperados
import { Counter, Rate } from 'k6/metrics';

// ==============================================================================
// Definición de Métricas y Umbrales
// ==============================================================================

// Tasa de aserciones de contrato exitosas (debe ser exactamente 1.0 = 100%)
const contractSuccess = new Rate('contract_checks_success');

// Contador de errores inesperados (debe permanecer estrictamente en 0)
const unexpectedErrors = new Counter('unexpected_errors');

// Configuración de ejecución del smoke test: 1 solo usuario virtual, 1 sola pasada
export const options = {
  vus: 1,
  iterations: 1,
  thresholds: {
    // Si alguna verificación del contrato falla, k6 marca la prueba como fallida
    contract_checks_success: ['rate==1.0'],
    unexpected_errors: ['count==0'],
  },
};

// ==============================================================================
// Variables de Entorno y Rutas Base
// ==============================================================================

// URL de destino del sistema. Por defecto apunta al ingress local en el puerto 80
const BASE_URL = __ENV.TARGET_URL || 'http://localhost';

// Referencia de imagen OCI para workers (estipulada en la Sección 4 del Contrato)
const DEFAULT_IMAGE_REF = __ENV.WORKER_IMAGE || 'ghcr.io/chimera-sdypp/sobel-worker:1.0.0';

// ==============================================================================
// Precarga de Datos de Prueba en Memoria
// ==============================================================================

// Carga del archivo con tokens sintéticos JWT
// Sin tokens válidos el smoke daría 401 en todo: si falta el archivo, open() corta acá (lo crea generar_datos.py).
const tokens = JSON.parse(open('./datos/tokens.json'));

// Carga de la imagen de prueba pequeña (1 KB)
let img1kb;
try {
  img1kb = open('./datos/img_1kb.png', 'b');
} catch (e) {
  img1kb = null;
}

// Carga del archivo deliberadamente corrupto
let imgCorrupta;
try {
  imgCorrupta = open('./datos/corrupta.png', 'b');
} catch (e) {
  imgCorrupta = null;
}

// ==============================================================================
// Función Principal de Prueba (Ejecución del Smoke Test)
// ==============================================================================
export default function () {
  // Selección del token de usuario estándar ('user_01')
  const token = tokens.usuarios && tokens.usuarios.user_01 ? tokens.usuarios.user_01 : 'dummy';
  // Selección del token expirado para pruebas de caducidad
  const tokenExpirado = tokens.expirado || 'expirado';

  console.log(`[SMOKE] Iniciando verificación de contrato contra: ${BASE_URL}`);

  // ----------------------------------------------------------------------------
  // Paso 1: Verificación de Disponibilidad (GET /health)
  // ----------------------------------------------------------------------------
  // Objetivo: El endpoint de salud debe ser público (sin token) y responder 200 OK.
  {
    const res = http.get(`${BASE_URL}/health`);
    const ok = check(res, {
      'GET /health responde 200': (r) => r.status === 200,
      'GET /health contiene estructura ok': (r) => {
        try {
          const body = r.json();
          // Valida formato estándar { "codigo": 200, "contenido": { ... } }
          return body.codigo === 200 && body.contenido !== undefined;
        } catch (_) {
          return false;
        }
      },
    });
    contractSuccess.add(ok);
    if (!ok) unexpectedErrors.add(1);
  }

  // ----------------------------------------------------------------------------
  // Paso 2: Rechazo de Solicitud sin Credenciales (POST /jobs)
  // ----------------------------------------------------------------------------
  // Objetivo: Una petición anónima debe responder HTTP 401 y error tipo NO_AUTORIZADO.
  {
    const res = http.post(`${BASE_URL}/jobs`, {});
    const ok = check(res, {
      'POST /jobs sin token responde 401': (r) => r.status === 401,
      'POST /jobs sin token tiene tipo NO_AUTORIZADO': (r) => {
        try {
          const body = r.json();
          return body.codigo === 401 && body.contenido.error.tipo === 'NO_AUTORIZADO';
        } catch (_) {
          return false;
        }
      },
    });
    contractSuccess.add(ok);
    if (!ok) unexpectedErrors.add(1);
  }

  // ----------------------------------------------------------------------------
  // Paso 3: Rechazo de Solicitud con Token Vencido (POST /jobs)
  // ----------------------------------------------------------------------------
  // Objetivo: Un token con claim 'exp' en el pasado debe ser rechazado con HTTP 401.
  {
    const params = {
      headers: {
        Authorization: `Bearer ${tokenExpirado}`,
      },
    };
    const res = http.post(`${BASE_URL}/jobs`, {}, params);
    const ok = check(res, {
      'POST /jobs con token expirado responde 401': (r) => r.status === 401,
    });
    contractSuccess.add(ok);
    if (!ok) unexpectedErrors.add(1);
  }

  // ----------------------------------------------------------------------------
  // Paso 4: Validación de Operaciones No Soportadas (POST /jobs)
  // ----------------------------------------------------------------------------
  // Objetivo: Enviar un filtro inexistente ('rotar_inexistente') debe responder
  // con HTTP 422 y error tipo OPERACION_NO_SOPORTADA según la Sección 6 del Contrato.
  if (img1kb) {
    const operaciones = JSON.stringify([
      { operacion: 'rotar_inexistente', imagen: DEFAULT_IMAGE_REF },
    ]);
    const payload = {
      operaciones: operaciones,
      archivos: http.file(img1kb, 'test.png', 'image/png'),
    };
    const params = {
      headers: {
        Authorization: `Bearer ${token}`,
      },
    };
    const res = http.post(`${BASE_URL}/jobs`, payload, params);
    const ok = check(res, {
      'Operación inexistente responde 422': (r) => r.status === 422,
      'Operación inexistente tiene tipo OPERACION_NO_SOPORTADA': (r) => {
        try {
          const body = r.json();
          return body.contenido.error.tipo === 'OPERACION_NO_SOPORTADA';
        } catch (_) {
          return false;
        }
      },
    });
    contractSuccess.add(ok);
    if (!ok) unexpectedErrors.add(1);
  }

  // ----------------------------------------------------------------------------
  // Paso 5: Validación de Archivos Corruptos (POST /jobs)
  // ----------------------------------------------------------------------------
  // Objetivo: Un archivo sin encabezado PNG válido debe responder HTTP 422 ARCHIVO_INVALIDO.
  if (imgCorrupta) {
    const operaciones = JSON.stringify([
      { operacion: 'sobel', imagen: DEFAULT_IMAGE_REF },
    ]);
    const payload = {
      operaciones: operaciones,
      archivos: http.file(imgCorrupta, 'corrupta.png', 'image/png'),
    };
    const params = {
      headers: {
        Authorization: `Bearer ${token}`,
      },
    };
    const res = http.post(`${BASE_URL}/jobs`, payload, params);
    const ok = check(res, {
      'Archivo corrupto responde 422': (r) => r.status === 422,
      'Archivo corrupto tiene tipo ARCHIVO_INVALIDO': (r) => {
        try {
          const body = r.json();
          return body.contenido.error.tipo === 'ARCHIVO_INVALIDO';
        } catch (_) {
          return false;
        }
      },
    });
    contractSuccess.add(ok);
    if (!ok) unexpectedErrors.add(1);
  }

  // ----------------------------------------------------------------------------
  // Paso 6: Camino Feliz y Sondeo E2E (POST /jobs -> GET /jobs/{id})
  // ----------------------------------------------------------------------------
  // Objetivo: Subir 1 imagen válida con operación 'sobel', obtener job_id (202 Accepted)
  // y consultar su estado hasta que alcance el estado terminal 'done'.
  if (img1kb) {
    const operaciones = JSON.stringify([
      { operacion: 'sobel', imagen: DEFAULT_IMAGE_REF },
    ]);
    const payload = {
      operaciones: operaciones,
      archivos: http.file(img1kb, 'imagen_valida.png', 'image/png'),
    };
    const params = {
      headers: {
        Authorization: `Bearer ${token}`,
      },
    };
    
    // Envío del trabajo válido
    const res = http.post(`${BASE_URL}/jobs`, payload, params);
    const ok = check(res, {
      'POST /jobs válido responde 202': (r) => r.status === 202,
      'POST /jobs devuelve job_id': (r) => {
        try {
          const body = r.json();
          return body.codigo === 202 && body.contenido.job_id !== undefined;
        } catch (_) {
          return false;
        }
      },
    });
    contractSuccess.add(ok);
    
    // Si no se pudo crear el trabajo, interrumpimos el flujo
    if (!ok) {
      unexpectedErrors.add(1);
      return;
    }

    // Obtención del identificador del trabajo
    const jobId = res.json().contenido.job_id;
    console.log(`[SMOKE] Job creado: ${jobId}. Sondeando estado...`);

    // Bucle de sondeo reactivo durante un máximo de 20 segundos
    let attempts = 0;
    let finalStatus = null;
    while (attempts < 20) {
      sleep(1); // Espera de 1 segundo entre encuestas
      const pollRes = http.get(`${BASE_URL}/jobs/${jobId}`, params);
      
      if (pollRes.status === 200) {
        try {
          const pollBody = pollRes.json();
          const estado = pollBody.contenido.estado;
          // Si alcanzó un estado definitivo, salimos del bucle
          if (estado === 'done' || estado === 'failed' || estado === 'dlq') {
            finalStatus = estado;
            break;
          }
        } catch (_) {}
      }
      attempts++;
    }

    console.log(`[SMOKE] Estado final del job tras ${attempts}s: ${finalStatus}`);
    
    // Aserción final: El trabajo debe haber culminado en estado 'done'
    const jobTerminoBien = check(null, {
      'El job procesó exitosamente a estado done': () => finalStatus === 'done',
    });
    contractSuccess.add(jobTerminoBien);
    if (!jobTerminoBien) {
      unexpectedErrors.add(1);
    }
  }
}
