import json
import httpx

URL = "http://127.0.0.1:8000/mcp"

HEADERS = {
    "Accept": "application/json, text/event-stream",
    "Content-Type": "application/json",
}

_request_id = 1


def _next_id():
    global _request_id
    val = _request_id
    _request_id += 1
    return val


def _parse_sse_for_jsonrpc(text: str, wanted_id: int | None = None):
    """
    Parse SSE payload and return JSON-RPC message(s).

    SSE format is typically lines like:
      event: message
      data: {...json...}

    We extract all data: lines and json-decode them.
    If wanted_id is provided, we return the first JSON object whose "id" matches.
    """
    messages = []
    for line in text.splitlines():
        line = line.strip()
        if not line.startswith("data:"):
            continue
        data = line[len("data:"):].strip()
        if not data:
            continue
        try:
            obj = json.loads(data)
            messages.append(obj)
        except json.JSONDecodeError:
            # Some servers may send non-JSON data lines, ignore them
            continue

    if wanted_id is None:
        return messages

    for obj in messages:
        if isinstance(obj, dict) and obj.get("id") == wanted_id:
            return obj

    return None


def mcp_post(client: httpx.Client, session_id: str | None, payload: dict):
    headers = dict(HEADERS)
    if session_id:
        headers["mcp-session-id"] = session_id

    r = client.post(URL, json=payload, headers=headers)

    # Helpful diagnostics on failures
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code}\n{r.headers}\n{r.text}")

    ctype = (r.headers.get("content-type") or "").lower()

    # JSON response
    if "application/json" in ctype:
        if not r.content:
            raise RuntimeError(f"Empty JSON response body.\nHeaders: {r.headers}")
        return r.json()

    # SSE response
    if "text/event-stream" in ctype:
        parsed = _parse_sse_for_jsonrpc(r.text, wanted_id=payload.get("id"))
        if parsed is None:
            raise RuntimeError(f"SSE response did not contain JSON-RPC for id={payload.get('id')}.\nBody:\n{r.text[:2000]}")
        return parsed

    # Unknown response type
    raise RuntimeError(f"Unexpected Content-Type: {ctype}\nHeaders: {r.headers}\nBody:\n{r.text[:2000]}")


def mcp_initialize(client: httpx.Client) -> str:
    init_id = _next_id()
    payload = {
        "jsonrpc": "2.0",
        "id": init_id,
        "method": "initialize",
        "params": {
            # If your server is older and expects 2024-11-05, switch this value.
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": {"name": "cli", "version": "0.1.0"},
        },
    }

    r = client.post(URL, json=payload, headers=HEADERS)
    r.raise_for_status()

    session_id = r.headers.get("mcp-session-id")
    if not session_id:
        # Some servers may be stateless, but yours returned a session earlier
        raise RuntimeError(f"No mcp-session-id header returned.\nHeaders: {r.headers}\nBody: {r.text[:1000]}")

    # Notify initialized (some servers expect this step)
    notif = {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}}
    client.post(URL, json=notif, headers={**HEADERS, "mcp-session-id": session_id}).raise_for_status()

    return session_id


def list_tools(client: httpx.Client, session_id: str):
    payload = {"jsonrpc": "2.0", "id": _next_id(), "method": "tools/list", "params": {}}
    return mcp_post(client, session_id, payload)


def call_tool(client: httpx.Client, session_id: str, name: str, arguments: dict):
    payload = {
        "jsonrpc": "2.0",
        "id": _next_id(),
        "method": "tools/call",
        "params": {"name": name, "arguments": arguments},
    }
    return mcp_post(client, session_id, payload)


if __name__ == "__main__":
    with httpx.Client(timeout=30.0) as client:
        sid = mcp_initialize(client)
        print("MCP session:", sid)

        tools = list_tools(client, sid)
        print("tools/list ->")
        print(json.dumps(tools, indent=2)[:3000])

        resp = call_tool(
            client,
            sid,
            "load_csv",
            {"path": r"C:\Users\pchitnbh\Documents\Workday Data Table.csv", "delimiter": ",", "sample_rows": 5},
        )
        print("load_csv ->")
        print(json.dumps(resp, indent=2)[:3000])

        schema = call_tool(client, sid, "get_schema", {})
        print("get_schema ->")
        print(json.dumps(schema, indent=2)[:3000])