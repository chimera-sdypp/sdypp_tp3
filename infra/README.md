# Infraestructura en GCP

Una sola vez, a mano (crea la identidad con la que después el CI aplica el resto):

```bash
gcloud auth application-default login
gcloud services enable cloudresourcemanager.googleapis.com --project=<proyecto>   # la usa Terraform para los permisos
gcloud storage buckets create gs://<proyecto>-tfstate --location=southamerica-east1 --uniform-bucket-level-access
gcloud storage buckets update gs://<proyecto>-tfstate --versioning
terraform -chdir=infra/base init -backend-config="bucket=<proyecto>-tfstate"
terraform -chdir=infra/base apply -var proyecto=<proyecto>
```

Con las salidas se cargan las **variables** del repositorio en GitHub (no son secretos): `GCP_PROYECTO`,
`GCP_PROVEEDOR_WIF` (`proveedor_wif`), `GCP_CUENTA_CI` (`cuenta_ci`), `TF_BUCKET`, `HIT2_IMAGEN`,
`HIT2_IP_PERMITIDA` y `HIT3_IMAGEN_WORKER` (las imágenes van en `registry`). `hit3/infra` se aplica sólo si además
`HIT3_ACTIVO=true`: GKE gasta crédito mientras exista.

Desde ahí, [`terraform.yml`](../.github/workflows/terraform.yml) hace `plan` de [`hit2/infra`](../hit2/infra) y
[`hit3/infra`](../hit3/infra) en cada PR y `apply` al mergear a `main`, autenticado por OIDC: no hay claves de GCP
en GitHub ni en las VMs. El estado vive en el bucket (con lock y versionado), nunca en el repo.
