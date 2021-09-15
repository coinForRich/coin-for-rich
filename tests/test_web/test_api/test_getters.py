import pytest
from fastapi.testclient import TestClient
from web.main import app


client = TestClient(app)

@pytest.mark.afterpop
def test_get():
    '''
    Tests getting from Test table
    '''
    
    response = client.get("/api/test")
    assert response.status_code == 200
    
    resp_j = response.json()[0]
    assert (resp_j['id'] == 1) and (resp_j['b'] == "a") and (resp_j['q'] == "b")

@pytest.mark.afterpop
def test_analytics():
    '''
    Tests getting from analytics APIs
    '''

    # Geometric daily return
    response = client.get("/api/analytics/geodr")
    assert response.status_code == 200

    response = client.get("/api/analytics/geodr?cutoff_upper_pct=20")
    assert response.status_code == 200

    response = client.get("/api/analytics/geodr?cutoff_lower_pct=10")
    assert response.status_code == 200

    response = client.get(
        "/api/analytics/geodr?cutoff_upper_pct=20&cutoff_lower_pct=10&limit=10")
    assert response.status_code == 200

    # Weekly return
    response = client.get("/api/analytics/wr")
    assert response.status_code == 200

    response = client.get("/api/analytics/wr?cutoff_upper_pct=20")
    assert response.status_code == 200

    response = client.get("/api/analytics/wr?cutoff_lower_pct=10")
    assert response.status_code == 200

    response = client.get(
        "/api/analytics/wr?cutoff_upper_pct=20&cutoff_lower_pct=10&limit=10")
    assert response.status_code == 200

    # Top 20 Volume
    response = client.get("/api/analytics/top20qvlm")
    assert response.status_code == 200

@pytest.mark.afterpop
def test_other_api():
    '''
    Tests getting from other API endpoints
    '''

    response = client.get("/api/symbol-exchange")
    assert response.status_code == 200
