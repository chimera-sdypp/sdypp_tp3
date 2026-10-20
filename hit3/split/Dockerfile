# Se construye desde hit3/ para incluir el código común: docker build -f split/Dockerfile hit3
FROM python:3.14-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY comun/requirements.txt comun/
COPY split/requirements.txt split/
RUN pip install --no-cache-dir -r split/requirements.txt
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin sobel \
    && mkdir logs && chown sobel logs
COPY comun/*.py comun/
COPY split/*.py split/
USER sobel
EXPOSE 8080
CMD ["python", "-m", "split.split"]
