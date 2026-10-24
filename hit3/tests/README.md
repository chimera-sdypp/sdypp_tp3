# Tests de Integración End-to-End — Hit #3 (Tarea S5)

Este directorio contiene la suite de pruebas automatizadas de integración punta a punta (*End-to-End*) para la arquitectura distribuida del **Hit #3**, conforme a lo especificado en la **Tarea S5** del reparto de responsabilidades del Grupo Cerberus (`TP3-P2-reparto-tareas.docx`).

---

## 🎯 Objetivo de la Prueba

El script [`test_e2e.sh`](test_e2e.sh) valida de manera totalmente desatendida y reproducible:
1. **Despliegue y salud:** Comprueba o aplica los 21 manifiestos de Kubernetes en k3s (`hit3/k8s/`) y asegura la disponibilidad de la API Gateway en `/health`.
2. **Generación sintética y Ground-Truth:** Genera una imagen representativa con alto contraste y calcula la salida canónica con el algoritmo centralizado del **Hit #1** (`hit1/sobel.py`).
3. **Seguridad y contratos:** Emite dinámicamente un token JWT HS256 (`SOBEL_SECRETO`) y envía el trabajo vía `POST /jobs` con `multipart/form-data`.
4. **Ciclo de vida asincrónico:** Realiza sondeo periódico con timeout al endpoint `GET /jobs/{id}` observando la transición de estados (`queued` -> `processing` -> `done`).
5. **Fidelidad matemática de procesamiento:** Descarga la imagen reconstruida desde `GET /jobs/{id}/resultado/0` y valida con `numpy.array_equal` que la salida distribuida sea **100% idéntica píxel por píxel** al procesamiento centralizado.
6. **Robustez y validación temprana (R2):** Envía un pedido con operaciones inexistentes y comprueba el rechazo preventivo antes de encolar en RabbitMQ.

---

## 🚀 Instrucciones de Ejecución

### Prerrequisitos
* `curl` y `python3` con librerías `numpy` y `Pillow`.
* Clúster local k3s levantado con Traefik como Ingress Controller en el puerto local (default: `http://localhost`).
* Workers activos para la imagen configurada (ej. `ghcr.io/chimera-sdypp/sobel-worker:1.0.0`).

### Ejecución estándar contra clúster ya levantado
```bash
bash hit3/tests/test_e2e.sh
```

### Ejecución aplicando manifiestos automáticamente
```bash
bash hit3/tests/test_e2e.sh --deploy
```

### Ejecución con parámetros personalizados
```bash
bash hit3/tests/test_e2e.sh \
  --url "http://localhost:8080" \
  --namespace "sobel" \
  --imagen "ghcr.io/chimera-sdypp/sobel-worker:1.0.0" \
  --timeout 240
```

---

## 📊 Códigos de Salida

| Código | Significado |
|---|---|
| `0` | **Éxito total:** Pipeline distribuido funcional y píxeles idénticos al centralizado. |
| `1` | Error de argumentos o dependencias faltantes en el sistema. |
| `2` | La API Gateway no responde 200 en `/health` dentro del tiempo límite. |
| `3` | No se pudo emitir o recuperar un token JWT válido. |
| `4` | `POST /jobs` rechazado o código HTTP inesperado (distinto de 202). |
| `5` | Timeout esperando que el job alcance el estado `done`. |
| `6` | El job finalizó con estado `failed` en el backend. |
| `7` | El job terminó en la cola de mensajes muertos (`dlq`). |
| `8` | Error al descargar la imagen final desde `/resultado/0`. |
| `9` | El archivo descargado está vacío. |
| `10` / `11` | Discrepancia dimensional o de valores de píxeles contra la salida del Hit #1. |
