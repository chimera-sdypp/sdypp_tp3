# Matriz R5 — 2026-11-07 11:24

Destino: `http://localhost` · ramp-up 5s · meseta 30s (salvo que la celda diga otra cosa).

Una celda **cumple** si: p95 de `POST /jobs` ≤ 1 s; p95 de `tiempos_s.total` ≤ 30 s para pedidos de
hasta 10 MiB en total (≤ 120 s si son más grandes); y menos de 1 % de errores no provocados (contrato §8).

| VA | VB | VC | V1 | V3 | Falla | p95 POST (ms) | p95 total (s) | Errores (%) | Req/s | Resultado |
|---|---|---|---|---|---|---|---|---|---|---|
| 10 | 2 | 10 | 1mb | 2 |  | 1194 | 20.65 (≤ 120) | 0.00 | 8.82 | **NO CUMPLE** |
| 10 | 2 | 10 | 1mb | 2 | sí | 590 | 36.85 (≤ 120) | 0.00 | 9.51 | **CUMPLE** |
| 10 | 3 | 10 | 1mb | 2 |  | 1108 | 24.76 (≤ 120) | 0.00 | 8.94 | **NO CUMPLE** |
| 1 | 1 | 1 | 1mb | 2 |  | 34 | 0.17 (≤ 30) | 0.00 | 1.79 | **CUMPLE** |
| 1 | 1 | 50 | 1mb | 2 |  | 2401 | 10.42 (≤ 30) | 0.00 | 46.69 | **NO CUMPLE** |
| 1 | 2 | 10 | 10mb | 2 |  | 984 | 28.27 (≤ 30) | 0.00 | 9.35 | **CUMPLE** |
| 1 | 2 | 10 | 1mb | 2 |  | 410 | 1.80 (≤ 30) | 0.00 | 13.07 | **CUMPLE** |
| 50 | 1 | 10 | 1mb | 2 |  | 19598 | 52.08 (≤ 120) | 0.00 | 6.55 | **NO CUMPLE** |
