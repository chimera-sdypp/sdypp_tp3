# Una sola imagen para el worker (comando por defecto) y el master (compose le cambia el comando).
FROM python:3.14-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin sobel \
    && mkdir logs && chown sobel logs
COPY *.py ./
USER sobel
EXPOSE 8080
CMD ["python", "worker.py"]
