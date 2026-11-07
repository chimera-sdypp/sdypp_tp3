# Hit 2 — corridas híbridas en GCP (28/10/2026, versión del tag v1.0.0)

`hit2/desbordar.sh N imagen salida` en el proyecto `chimera-tp3`: 1 worker local (la PC de la línea de base, ver
`resumen-local.md`) más N VMs `e2-standard-2` en `southamerica-east1-a`, creadas y borradas por Terraform en cada
corrida. Una corrida por configuración. Las 4 salidas son idénticas píxel a píxel a la del centralizado y al final
de cada corrida quedan 0 VMs en el proyecto. Log completo en `corridas-gcp-28-10.log` (incluye 3 intentos de las 18:3x que
fallaron antes de crear nada por un error de tipeo en el comando: `cantidad` recibió "1 10mb").

| Imagen | VMs | Franja local | Franjas en la nube | Master total | VMs listas | Corrida completa |
|---|---|---|---|---|---|---|
| 10 MB (1850²) | 1 | 0,35 s | 1,28 s | 1,75 s | 100 s | 105 s |
| 10 MB (1850²) | 2 | 0,26 s | 1,04 / 1,07 s | 1,54 s | 140 s | 150 s |
| 50 MB (4150²) | 1 | 1,74 s | 3,88 s | 6,40 s | 108 s | 118 s |
| 50 MB (4150²) | 2 | 1,18 s | 2,23 / 2,79 s | 5,26 s | 93 s | 101 s |

Comparación con la línea de base local (mediana de 3, `resumen-local.md`):

| Imagen | 1 worker local | 2 workers locales | 1 local + 1 VM | 1 local + 2 VMs |
|---|---|---|---|---|
| 10 MB | 1,31 s | 0,86 s | 1,75 s | 1,54 s |
| 50 MB | 6,12 s | 4,55 s | 6,40 s | 5,26 s |

- Una franja en la nube tarda 2 a 4 veces más que la misma en la PC: es el viaje de ida y vuelta por Internet; el
  cálculo es el mismo.
- Con 10 MB la nube no ayuda nunca: el viaje pesa más que el cálculo. Con 50 MB, sumar 2 VMs a un solo worker local
  baja de 6,12 a 5,26 s (−14 %), pero 2 workers locales siguen siendo más rápidos (4,55 s).
- Arrancar las VMs (Terraform + Docker + imagen) tarda 93 a 140 s: sólo se amortiza si se procesan muchas imágenes
  con las mismas VMs, no una sola.
