# 10 imágenes de 100 MB a la vez, con el bucket (07/11/2026, v1.1.2)

La prueba que tiraba Redis: 10 usuarios virtuales mandan cada uno un job con una imagen de 100 MB (`sobel`, 2 workers),
con k6 dentro del cluster contra la IP pública del ingress (`--celda 1 1 10 100mb`). Desde la 1.1.2 los píxeles van al
bucket `chimera-sobel-imagenes` y Redis guarda sólo el estado de cada job y un puntero por imagen
([contrato §10](../../../contrato.md#10-redis-y-el-bucket-de-los-píxeles)).

| | Antes (1.1.1, prueba del profe, 07/11 20:38–20:54) | Ahora (1.1.2, 07/11 21:58) |
|---|---|---|
| Memoria de Redis | Llegó a su límite de 2 GiB: 3 reinicios por OOM | **1,91–2,07 MiB** durante toda la carga ([`redis-used-memory.txt`](redis-used-memory.txt)) |
| Respuestas | 767 `502` y 220 `503` en 12 min | 0 % de errores (`http_req_failed` 0 de 511) |
| Jobs | Jobs perdidos al reiniciarse Redis | **10 de 10 en `done`**, 0 `failed`, 0 en la DLQ |

Estado final según `/stats`: los 10 jobs en `done`, latencia del job p50 96,7 s y p95 163,5 s; en el bucket quedaron los
10 resultados (257 MB), que vencen al día. Un pedido de 10 × 100 MB en un solo `POST` da `413 ARCHIVO_DEMASIADO_GRANDE`
(el contrato acepta hasta 200 MiB por pedido).

`informe.md` y el JSON son el resumen de k6: la meseta es de 30 s y cada job tarda más de un minuto, así que k6 sólo
cuenta las 2 iteraciones que terminaron dentro de la corrida (las otras 8 las corta al final, pero los jobs siguen y
terminan en el cluster: por eso los tiempos de arriba salen de `/stats`).

**Qué limita ahora.** Redis ya no: lo que queda es cómputo y transferencia.

- El `POST /jobs` tarda 12,7 s de p95: el backend recibe los 100 MB, los valida y los sube al bucket antes de
  responder `202`. La mejora siguiente sería que el cliente suba directo al bucket con una URL firmada.
- El job (p95 163 s, más que los 120 s del SLO) lo limitan los 2 workers con 1 GB de imágenes para procesar: se
  escala con más workers, no con más memoria.

`redis-used-memory.txt`: una línea por segundo con `<hora unix> <used_memory en bytes> <claves en Redis>`.
