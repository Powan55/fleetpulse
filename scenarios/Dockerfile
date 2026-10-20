FROM docker:29.8.2-cli@sha256:35cfcf893c20f2931f4909151b5407aaaeab711277de7ce8b8f521f095d54c32 AS docker-cli

FROM python:3.14.8@sha256:12d187a59ecdeb95df0a78cd6107a84398455365a66cc2183db2be42b6067d33
WORKDIR /app
COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker
COPY requirements.txt .
RUN pip install --no-cache-dir --require-hashes -r requirements.txt
COPY *.py ./
RUN useradd --system --uid 10001 runner && mkdir /app/out && chown runner /app/out
USER runner
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "/app/run.py"]
