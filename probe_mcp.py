import httpx

URL = "http://127.0.0.1:8000/mcp"
client = httpx.Client(timeout=10.0)

r = client.get(URL)
print('status', r.status_code)
print('headers', dict(r.headers))
print('text:', r.text[:2000])
