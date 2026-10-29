# El master del Hit #2: el reparto del Hit #1 como servicio, con RabbitMQ, Redis y Terraform para las VMs.
# El contexto lo arma construir.sh (los .py del Hit #1 y de hit2/, y hit2/infra sin el tfvars).
FROM hashicorp/terraform:1.15.9 AS terraform

FROM python:3.14-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY --from=terraform /bin/terraform /usr/local/bin/terraform
RUN useradd --uid 10001 --create-home --shell /usr/sbin/nologin sobel && mkdir logs infra /datos && chown sobel logs infra /datos
COPY --chown=sobel infra/ infra/
COPY *.py ./
USER sobel
# El provider de Google queda en la imagen: al arrancar sólo se configura el backend (el bucket del estado).
RUN terraform -chdir=infra init -backend=false -input=false -no-color > /dev/null
EXPOSE 8080
CMD ["python", "servicio.py"]
