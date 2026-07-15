from httpx import ASGITransport, AsyncClient
import pytest


@pytest.fixture(scope="session")
def app(tmp_path_factory):
    from pipeline.run import load_serving
    tmp_path = tmp_path_factory.mktemp("api")
    db = tmp_path / "serving.db"
    events = [{"event_id":"seed-event","event_type":"page_view","session_id":"seed-session","user_id":None,
               "event_ts":"2025-03-31 10:00:00","page":"/","partner_id":"PRT0001","device":"mobile",
               "country":"BR","channel":"direct","price_usd":None,"metadata_json":None,
               "is_bot_flag":False,"ingest_date":"2025-03-31"}]
    transactions = [{"transaction_id":"TXN-SEED","booking_ref":"BKG-SEED","partner_id":"PRT0001","user_id":None,
                     "created_at":"2025-03-31 10:01:00","ingest_ts":"2025-03-31 10:02:00","amount":100.0,
                     "currency":"BRL","status":"confirmed","payment_method":"pix","gateway":"seed",
                     "retry_count":0,"error_code":None,"notes":None,"processing_ms":100,
                     "ingest_date":"2025-03-31","is_late":False}]
    partners = [{"partner_id":"PRT0001","schema_version":"v3","name":"Seed Partner","country_code":"BR",
                 "status":"active","tier":"gold","commission_rate":0.15,"created_at":"2024-01-01 00:00:00",
                 "updated_at":"2025-01-01 00:00:00","sla_hours":24,"avg_rating":4.8,
                 "api_endpoint":None,"webhook_enabled":False}]
    load_serving(db, events, transactions, partners)
    import api.main
    api.main.DB = str(db)
    return api.main.app


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_health(app):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/v1/health")
    assert response.status_code == 200


@pytest.mark.anyio
async def test_auth_is_required(app):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/v1/partners/PRT0001")
    assert response.status_code == 422


@pytest.mark.anyio
async def test_partner_and_idempotent_ingest(app):
    headers = {"X-API-Key": "local-dev-key"}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        assert (await client.get("/v1/partners/PRT0001", headers=headers)).status_code == 200
        payload = {"event_id":"event-test-0001","event_type":"page_view","session_id":"session-1",
                   "event_ts":"2025-03-31 12:00:00","ingest_date":"2025-03-31"}
        assert (await client.post("/v1/events/ingest", headers=headers, json=payload)).json()["accepted"]
        assert (await client.post("/v1/events/ingest", headers=headers, json=payload)).json()["idempotent"]


def test_openapi_contract_contains_versioned_endpoints_and_api_key(app):
    contract = app.openapi()
    required = {
        "/v1/health", "/v1/metrics/funnel", "/v1/partners/{partner_id}",
        "/v1/transactions/summary", "/v1/events/ingest",
    }
    assert required <= contract["paths"].keys()
    secured = contract["paths"]["/v1/partners/{partner_id}"]["get"]
    assert {item["name"] for item in secured["parameters"]} >= {"partner_id", "X-API-Key"}
