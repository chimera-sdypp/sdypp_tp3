import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate } from 'k6/metrics';

// Métricas de validación de contrato
const contractSuccess = new Rate('contract_checks_success');
const unexpectedErrors = new Counter('unexpected_errors');

export const options = {
  vus: 1,
  iterations: 1,
  thresholds: {
    contract_checks_success: ['rate==1.0'],
    unexpected_errors: ['count==0'],
  },
};

const BASE_URL = __ENV.TARGET_URL || 'http://localhost:8080';
const DEFAULT_IMAGE_REF = __ENV.WORKER_IMAGE || 'ghcr.io/chimera-sdypp/sobel-worker:1.0.0';

// Carga de archivo de tokens e imágenes
let tokens;
try {
  tokens = JSON.parse(open('./datos/tokens.json'));
} catch (e) {
  // Si no está el archivo generado, usaremos un token placeholder o se avisará
  tokens = { usuarios: { user_01: 'token_dummy' }, admin: 'token_admin', expirado: 'token_expirado' };
}

let img1kb;
try {
  img1kb = open('./datos/img_1kb.png', 'b');
} catch (e) {
  img1kb = null;
}

let imgCorrupta;
try {
  imgCorrupta = open('./datos/corrupta.png', 'b');
} catch (e) {
  imgCorrupta = null;
}

export default function () {
  const token = tokens.usuarios && tokens.usuarios.user_01 ? tokens.usuarios.user_01 : 'dummy';
  const tokenExpirado = tokens.expirado || 'expirado';

  console.log(`[SMOKE] Iniciando verificación de contrato contra: ${BASE_URL}`);

  // 1. GET /health (público)
  {
    const res = http.get(`${BASE_URL}/health`);
    const ok = check(res, {
      'GET /health responde 200': (r) => r.status === 200,
      'GET /health contiene estructura ok': (r) => {
        try {
          const body = r.json();
          return body.codigo === 200 && body.contenido !== undefined;
        } catch (_) {
          return false;
        }
      },
    });
    contractSuccess.add(ok);
    if (!ok) unexpectedErrors.add(1);
  }

  // 2. POST /jobs sin token (debe responder 401 NO_AUTORIZADO)
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

  // 3. POST /jobs con token expirado (debe responder 401)
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

  // 4. POST /jobs con operación inexistente (debe responder 422 OPERACION_NO_SOPORTADA)
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

  // 5. POST /jobs con archivo corrupto (debe responder 422 ARCHIVO_INVALIDO)
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

  // 6. POST /jobs válido con 1 archivo y sondeo hasta done
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
    if (!ok) {
      unexpectedErrors.add(1);
      return;
    }

    const jobId = res.json().contenido.job_id;
    console.log(`[SMOKE] Job creado: ${jobId}. Sondeando estado...`);

    // Sondeo de estado
    let attempts = 0;
    let finalStatus = null;
    while (attempts < 20) {
      sleep(1);
      const pollRes = http.get(`${BASE_URL}/jobs/${jobId}`, params);
      if (pollRes.status === 200) {
        try {
          const pollBody = pollRes.json();
          const estado = pollBody.contenido.estado;
          if (estado === 'done' || estado === 'failed' || estado === 'dlq') {
            finalStatus = estado;
            break;
          }
        } catch (_) {}
      }
      attempts++;
    }

    console.log(`[SMOKE] Estado final del job tras ${attempts}s: ${finalStatus}`);
  }
}
