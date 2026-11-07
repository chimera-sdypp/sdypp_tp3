# Matriz R5 — 2026-11-07 20:06

Destino: `http://8.232.111.42` · ramp-up 5s · meseta 30s (salvo que la celda diga otra cosa).

Una celda **cumple** si: p95 de `POST /jobs` ≤ 1 s; p95 de `tiempos_s.total` ≤ 30 s para pedidos de
hasta 10 MiB en total (≤ 120 s si son más grandes); y menos de 1 % de errores no provocados (contrato §8).

| VA | VB | VC | V1 | V3 | Falla | p95 POST (ms) | p95 total (s) | Errores (%) | Req/s | Resultado |
|---|---|---|---|---|---|---|---|---|---|---|
| 10 | 2 | 10 | 1mb | 2 |  | 524 | 23.84 (≤ 120) | 0.00 | 7.94 | **CUMPLE** |
| 10 | 2 | 10 | 1mb | 2 | sí | 508 | 52.33 (≤ 120) | 0.00 | 8.49 | **CUMPLE** |
| 10 | 3 | 10 | 1mb | 2 |  | 725 | 26.49 (≤ 120) | 0.00 | 7.97 | **CUMPLE** |
| 1 | 1 | 1 | 1mb | 2 |  | 78 | 0.44 (≤ 30) | 0.00 | 1.76 | **CUMPLE** |
| 1 | 1 | 50 | 1mb | 2 |  | 393 | 15.47 (≤ 30) | 0.00 | 43.31 | **CUMPLE** |
| 1 | 2 | 10 | 10mb | 2 |  | 514 | 60.92 (≤ 30) | 100.00 | 9.12 | **NO CUMPLE** |
| 1 | 2 | 10 | 1mb | 2 |  | 209 | 2.35 (≤ 30) | 0.00 | 11.67 | **CUMPLE** |
| 50 | 1 | 10 | 1mb | 2 |  | 6513 | 65.42 (≤ 120) | 0.00 | 8.36 | **NO CUMPLE** |
