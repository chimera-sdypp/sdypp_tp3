# Matriz R5 — 2026-11-07 16:05

Destino: `http://8.232.111.42` · ramp-up 5s · meseta 30s (salvo que la celda diga otra cosa).

Una celda **cumple** si: p95 de `POST /jobs` ≤ 1 s; p95 de `tiempos_s.total` ≤ 30 s para pedidos de
hasta 10 MiB en total (≤ 120 s si son más grandes); y menos de 1 % de errores no provocados (contrato §8).

| VA | VB | VC | V1 | V3 | Falla | p95 POST (ms) | p95 total (s) | Errores (%) | Req/s | Resultado |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1 | 1 | 100mb | 2 |  | 9665 | 25.44 (≤ 120) | 0.00 | 0.63 | **NO CUMPLE** |
| 1 | 1 | 1 | 10mb | 2 |  | 2411 | 3.14 (≤ 30) | 0.00 | 0.69 | **NO CUMPLE** |
| 1 | 1 | 1 | 1kb | 2 |  | 230 | 0.08 (≤ 30) | 0.00 | 1.38 | **CUMPLE** |
| 1 | 1 | 1 | 1mb | 2 |  | 1391 | 0.38 (≤ 30) | 0.00 | 0.87 | **NO CUMPLE** |
| 1 | 1 | 1 | 50mb | 2 |  | 5952 | 13.95 (≤ 120) | 0.00 | 0.64 | **NO CUMPLE** |
