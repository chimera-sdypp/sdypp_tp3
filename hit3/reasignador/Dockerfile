# Se construye desde hit3/ para incluir el código común (contrato §12):
#   docker build -f reasignador/Dockerfile -t ghcr.io/chimera-sdypp/sobel-reasignador:<tag> hit3
FROM python:3.14-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY comun/requirements.txt comun/
COPY reasignador/requirements.txt reasignador/
RUN pip install --no-cache-dir -r reasignador/requirements.txt
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin sobel \
    && mkdir logs && chown sobel logs
COPY comun/ comun/
COPY reasignador/ reasignador/
USER sobel
EXPOSE 8080
CMD ["python", "-m", "reasignador.reasignador"]
