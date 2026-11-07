# Matriz R5 — 2026-11-07 20:14

Destino: `http://8.232.111.42` · ramp-up 5s · meseta 30s (salvo que la celda diga otra cosa).

Una celda **cumple** si: p95 de `POST /jobs` ≤ 1 s; p95 de `tiempos_s.total` ≤ 30 s para pedidos de
hasta 10 MiB en total (≤ 120 s si son más grandes); y menos de 1 % de errores no provocados (contrato §8).

| VA | VB | VC | V1 | V3 | Falla | p95 POST (ms) | p95 total (s) | Errores (%) | Req/s | Resultado |
|---|---|---|---|---|---|---|---|---|---|---|
| 10 | 2 | 10 | 1mb | 4 |  | 483 | 29.25 (≤ 120) | 0.00 | 7.61 | **CUMPLE** |
