FROM python:3.12-slim AS builder
WORKDIR /build
COPY requirements.txt .
RUN python -m venv /venv && /venv/bin/pip install --no-cache-dir -r requirements.txt

FROM python:3.12-slim
RUN useradd --create-home --uid 10001 app
COPY --from=builder /venv /venv
WORKDIR /app
COPY api api
USER app
ENV PATH="/venv/bin:$PATH" PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]

