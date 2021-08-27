from fastapi.testclient import TestClient
from web.main import app


client = TestClient(app)

def test_get():
    response = client.get("/api/test")
    assert response.status_code == 200
    
    resp_j = response.json()[0]
    assert (resp_j['id'] == 1) and (resp_j['b'] == "a") and (resp_j['q'] == "b")
