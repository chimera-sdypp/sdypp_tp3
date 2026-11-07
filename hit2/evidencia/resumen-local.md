# Hit 2 — línea de base local (27/10/2026)

Ryzen 5 3600 (6 núcleos / 12 hilos), 31 GiB, Docker 29.9.2, commit `ab73028`. Master y workers del Hit 1 en la
misma PC (Docker Compose). Mediana de 3 corridas; `total` es lo que mide el master, entre paréntesis el tiempo de
pared con el arranque del contenedor. Datos crudos en `mediciones-local.csv` y `corridas-local.log`.

| Imagen | Píxeles | Centralizado | 1 worker | 2 workers | 4 workers | 8 workers |
|---|---|---|---|---|---|---|
| 1 MB | 600×600 | 0,09 s (0,84) | 0,20 s (1,43) | 0,13 s (1,31) | 0,10 s (1,05) | 0,11 s (1,13) |
| 10 MB | 1850×1850 | 0,61 s (1,38) | 1,31 s (2,71) | 0,86 s (1,96) | 0,70 s (1,70) | 0,71 s (1,93) |
| 50 MB | 4150×4150 | 3,27 s (4,15) | 6,12 s (7,30) | 4,55 s (5,77) | 3,62 s (4,67) | 3,29 s (4,43) |
| 100 MB | 5750×5750 | 7,08 s (8,19) | 11,31 s (12,60) | 9,16 s (10,66) | 6,90 s (8,01) | 6,31 s (7,40) |

Desglose de la imagen de 100 MB (primera corrida):

| | Lectura | Sobel (o reparto + Sobel) | Escritura PNG | Total |
|---|---|---|---|---|
| Centralizado | 0,64 s | 1,72 s | 4,72 s | 7,08 s |
| 1 worker | 0,64 s | 6,56 s | 4,11 s | 11,31 s |
| 2 workers | 0,64 s | 3,59 s | 4,87 s | 9,10 s |
| 4 workers | 0,62 s | 2,11 s | 4,11 s | 6,84 s |
| 8 workers | 0,63 s | 1,56 s | 4,15 s | 6,35 s |
