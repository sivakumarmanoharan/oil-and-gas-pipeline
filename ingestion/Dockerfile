FROM cgr.dev/chainguard/python:latest-dev AS builder

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

FROM cgr.dev/chainguard/python:latest

WORKDIR /app
COPY --from=builder /home/nonroot/.local /home/nonroot/.local
COPY ingestion.py .

ENV PATH="/home/nonroot/.local/bin:$PATH"
ENTRYPOINT ["python", "ingestion.py"]