from __future__ import annotations

import os
import sqlite3
import time
from collections import defaultdict, deque
from contextlib import contextmanager
from datetime import date

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, status
from prometheus_client import Counter, Histogram, make_asgi_app
from pydantic import BaseModel, Field

DB = os.getenv("DATABASE_URL", os.getenv("SERVING_DB", "data/serving.db"))
API_KEY = os.getenv("API_KEY", "local-dev-key")
REQUESTS = Counter("rentcars_api_requests_total", "API requests", ["method", "path", "status"])
LATENCY = Histogram("rentcars_api_request_duration_seconds", "API request latency", ["path"])
app = FastAPI(title="Rentcars Data API", version="1.0.0")
app.mount("/metrics", make_asgi_app())
RATE_WINDOWS: dict[str, deque[float]] = defaultdict(deque)


@contextmanager
def connection():
    if str(DB).startswith(("postgresql://", "postgres://")):
        import psycopg
        from psycopg.rows import dict_row
        con = psycopg.connect(DB, row_factory=dict_row)
    else:
        con = sqlite3.connect(DB)
        con.row_factory = sqlite3.Row
    try:
        yield con
    finally:
        con.close()


def sql(statement: str) -> str:
    return statement.replace("?", "%s") if str(DB).startswith(("postgresql://", "postgres://")) else statement


async def authenticate(x_api_key: str = Header(..., alias="X-API-Key")) -> str:
    if x_api_key != API_KEY:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid API key")
    return x_api_key


async def rate_limit(request: Request) -> None:
    key = request.headers.get("X-API-Key") or (request.client.host if request.client else "unknown")
    now, window = time.monotonic(), RATE_WINDOWS[key]
    while window and window[0] <= now - 60:
        window.popleft()
    if len(window) >= 120:
        raise HTTPException(429, "rate limit exceeded")
    window.append(now)


@app.middleware("http")
async def observe(request: Request, call_next):
    started = time.perf_counter()
    response = await call_next(request)
    route = request.scope.get("route")
    path = getattr(route, "path", request.url.path)
    LATENCY.labels(path).observe(time.perf_counter() - started)
    REQUESTS.labels(request.method, path, response.status_code).inc()
    return response


class EventIn(BaseModel):
    event_id: str = Field(min_length=8)
    event_type: str
    session_id: str
    event_ts: str
    ingest_date: date
    user_id: str | None = None
    partner_id: str | None = None
    device: str = "unknown"
    country: str = "unknown"
    channel: str | None = None
    page: str | None = None
    price_usd: float | None = None
    metadata_json: str | None = None


@app.get("/v1/health")
async def health():
    try:
        with connection() as con:
            con.execute("SELECT 1").fetchone()
        return {"status": "healthy", "database": "up"}
    except Exception as exc:
        raise HTTPException(503, detail={"status": "unhealthy", "database": str(exc)}) from exc


@app.get("/v1/metrics/funnel", dependencies=[Depends(authenticate), Depends(rate_limit)])
async def funnel(request: Request, start_date: date, end_date: date, channel: str | None = None):
    if end_date < start_date:
        raise HTTPException(422, "end_date must be on or after start_date")
    where, params = "date(event_ts) BETWEEN ? AND ?", [str(start_date), str(end_date)]
    if channel:
        where += " AND channel = ?"
        params.append(channel)
    with connection() as con:
        rows = con.execute(sql(f"""SELECT COALESCE(channel,'unknown') channel, event_type, COUNT(*) events,
            COUNT(DISTINCT session_id) sessions FROM events WHERE {where}
            GROUP BY COALESCE(channel,'unknown'), event_type ORDER BY channel,event_type"""), params).fetchall()
    return {"start_date": start_date, "end_date": end_date, "data": [dict(r) for r in rows]}


@app.get("/v1/partners/{partner_id}", dependencies=[Depends(authenticate), Depends(rate_limit)])
async def partner(request: Request, partner_id: str):
    with connection() as con:
        profile = con.execute(sql("SELECT * FROM partners WHERE partner_id=? ORDER BY COALESCE(updated_at,created_at) DESC LIMIT 1"), (partner_id,)).fetchone()
        if not profile:
            raise HTTPException(404, "partner not found")
        perf = con.execute(sql("""SELECT COUNT(*) transaction_count,
            SUM(CASE WHEN status IN ('confirmed','completed') THEN amount ELSE 0 END) revenue,
            SUM(CASE WHEN status IN ('confirmed','completed') THEN 1 ELSE 0 END) approved_count
            FROM transactions WHERE partner_id=?"""), (partner_id,)).fetchone()
    return {"partner": dict(profile), "performance": dict(perf)}


@app.get("/v1/transactions/summary", dependencies=[Depends(authenticate), Depends(rate_limit)])
async def transaction_summary(request: Request, start_date: date, end_date: date,
                        partner_id: str | None = None, currency: str | None = Query(None, min_length=3, max_length=3)):
    where, params = "date(created_at) BETWEEN ? AND ?", [str(start_date), str(end_date)]
    if partner_id:
        where += " AND partner_id=?"; params.append(partner_id)
    if currency:
        where += " AND currency=?"; params.append(currency.upper())
    with connection() as con:
        rows = con.execute(sql(f"""SELECT partner_id,currency,status,COUNT(*) transaction_count,
            ROUND(SUM(amount),2) total_amount,ROUND(AVG(amount),2) average_amount
            FROM transactions WHERE {where} GROUP BY partner_id,currency,status ORDER BY partner_id,currency,status"""), params).fetchall()
    return {"data": [dict(r) for r in rows]}


@app.post("/v1/events/ingest", status_code=202, dependencies=[Depends(authenticate), Depends(rate_limit)])
async def ingest_event(request: Request, event: EventIn):
    values = event.model_dump(mode="json") | {"is_bot_flag": 0}
    with connection() as con:
        columns = "event_id,event_type,session_id,user_id,event_ts,page,partner_id,device,country,channel,price_usd,metadata_json,is_bot_flag,ingest_date"
        if str(DB).startswith(("postgresql://", "postgres://")):
            placeholders = ",".join(f"%({name})s" for name in columns.split(","))
            statement = f"INSERT INTO events ({columns}) VALUES ({placeholders}) ON CONFLICT (event_id) DO NOTHING"
        else:
            placeholders = ",".join(f":{name}" for name in columns.split(","))
            statement = f"INSERT INTO events ({columns}) VALUES ({placeholders}) ON CONFLICT (event_id) DO NOTHING"
        cursor = con.execute(statement, values)
        con.commit()
    return {"event_id": event.event_id, "accepted": cursor.rowcount == 1, "idempotent": cursor.rowcount == 0}
