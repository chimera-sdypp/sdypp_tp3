# Matriz R5 — 2026-11-07 20:38

Destino: `http://8.232.111.42` · ramp-up 5s · meseta 30s (salvo que la celda diga otra cosa).

Una celda **cumple** si: p95 de `POST /jobs` ≤ 1 s; p95 de `tiempos_s.total` ≤ 30 s para pedidos de
hasta 10 MiB en total (≤ 120 s si son más grandes); y menos de 1 % de errores no provocados (contrato §8).

| VA | VB | VC | V1 | V3 | Falla | p95 POST (ms) | p95 total (s) | Errores (%) | Req/s | Resultado |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1 | 1 | 100mb | 2 |  | 1985 | 26.94 (≤ 120) | 0.00 | 0.95 | **NO CUMPLE** |
| 1 | 1 | 1 | 10mb | 2 |  | 344 | 3.06 (≤ 30) | 0.00 | 1.18 | **CUMPLE** |
| 1 | 1 | 1 | 1kb | 2 |  | 186 | 0.18 (≤ 30) | 0.00 | 1.68 | **CUMPLE** |
| 1 | 1 | 1 | 1mb | 2 |  | 175 | 0.50 (≤ 30) | 0.00 | 1.60 | **CUMPLE** |
| 1 | 1 | 1 | 50mb | 2 |  | 834 | 12.68 (≤ 120) | 0.00 | 1.00 | **CUMPLE** |
