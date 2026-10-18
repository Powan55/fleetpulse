FROM python:3.14.8@sha256:12d187a59ecdeb95df0a78cd6107a84398455365a66cc2183db2be42b6067d33
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --require-hashes -r requirements.txt
COPY run.py lib.py s01.py s02.py ./
RUN useradd --system --uid 10001 runner && mkdir /app/out && chown runner /app/out
USER runner
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "/app/run.py"]
