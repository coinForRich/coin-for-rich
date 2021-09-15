from fastapi.testclient import TestClient
from web.main import app


client = TestClient(app)

def test_websocket():
    with client.websocket_connect("/api/test") as websocket:
        data = websocket.receive_json()
        assert data == {"detail": "Hello WebSocket"}
