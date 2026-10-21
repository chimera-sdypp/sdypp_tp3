# Se construye desde hit3/ para incluir comun/: docker build -f frontend/Dockerfile .
FROM python:3.14-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 SOBEL_LOGS=/app/logs
WORKDIR /app
COPY comun/requirements.txt comun/
COPY frontend/requirements.txt frontend/
RUN pip install --no-cache-dir -r frontend/requirements.txt
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin sobel \
    && mkdir logs && chown sobel logs
COPY comun/ comun/
COPY frontend/ frontend/
USER sobel
EXPOSE 8080
CMD ["sh", "-c", "exec uvicorn --factory frontend.app:crear_app --host 0.0.0.0 --port ${SOBEL_PUERTO:-8080} --no-access-log"]
