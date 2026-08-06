FROM python:3.12-slim
WORKDIR /app
RUN addgroup --system appgroup && adduser --system --ingroup appgroup appuser
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY downloadv5.py .
RUN mkdir -p channels logs data && chown -R appuser:appgroup /app
USER appuser
VOLUME ["/app/channels", "/app/logs", "/app/data"]
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health', timeout=3)" || exit 1
CMD ["python", "downloadv5.py"]
