# Matriz R5 — 2026-11-07 14:42

Destino: `http://localhost` · ramp-up 5s · meseta 30s (salvo que la celda diga otra cosa).

Una celda **cumple** si: p95 de `POST /jobs` ≤ 1 s; p95 de `tiempos_s.total` ≤ 30 s para pedidos de
hasta 10 MiB en total (≤ 120 s si son más grandes); y menos de 1 % de errores no provocados (contrato §8).

| VA | VB | VC | V1 | V3 | Falla | p95 POST (ms) | p95 total (s) | Errores (%) | Req/s | Resultado |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1 | 1 | 100mb | 2 |  | 4921 | 24.50 (≤ 120) | 0.00 | 0.83 | **NO CUMPLE** |
| 1 | 1 | 1 | 10mb | 2 |  | 1244 | 2.88 (≤ 30) | 0.00 | 1.21 | **NO CUMPLE** |
| 1 | 1 | 1 | 1kb | 2 |  | 98 | 0.27 (≤ 30) | 0.00 | 1.55 | **CUMPLE** |
| 1 | 1 | 1 | 1mb | 2 |  | 73 | 0.22 (≤ 30) | 0.00 | 1.73 | **CUMPLE** |
| 1 | 1 | 1 | 50mb | 2 |  | 2964 | 15.74 (≤ 120) | 0.00 | 0.85 | **NO CUMPLE** |
