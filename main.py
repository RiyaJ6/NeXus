"""
NeXus — Intelligent Network Orchestration for MENA Smart Cities
"""
import os, json, time, random, re, hashlib, asyncio, httpx
from contextvars import ContextVar
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Dict, List, Optional
from collections import OrderedDict, deque
from functools import lru_cache
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Header, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage, ToolMessage
from langchain_groq import ChatGroq
from langchain_core.tools import tool

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

load_dotenv()

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "") or os.getenv("GROQ_KEY", "")
NOKIA_API_KEY = os.getenv("NOKIA_API_KEY", "")
NOKIA_ENTRY = (os.getenv("NOKIA_BASE", "") or os.getenv("NOKIA_API_BASE", "") or "https://network-as-code.nokia.rapidapi.com").rstrip("/")
TEST_MSISDN = os.getenv("NOKIA_TEST_MSISDN", "").strip()
STATE_FILE = os.getenv("STATE_FILE", "nexus_state.json")
WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN", "")
TWIN_MODE = os.getenv("TWIN_MODE", "auto").lower()
MAX_LLM_CALLS = int(os.getenv("MAX_LLM_CALLS", "2000"))
MAX_API_CALLS = int(os.getenv("MAX_API_CALLS", "5000"))
MAX_AGENT_STEPS = int(os.getenv("MAX_AGENT_STEPS", "16"))
CACHE_TTL = int(os.getenv("CACHE_TTL", "120"))
BASE_SWEEP = int(os.getenv("DETECTION_SWEEP_SEC", "45"))
USER_AGENT = os.getenv("NEXUS_USER_AGENT", "NeXus-OS/1.0 (hackathon demo)")
B2G_LICENSE = os.getenv("B2G_LICENSE", "City licensing (per-region SaaS)")
B2B_MODEL = os.getenv("B2B_MODEL", "Per-QoD B2B micro-transaction")
BREAKEVEN = os.getenv("BREAKEVEN", "~40 protected-asset subscriptions")
SAAS_MARGIN = os.getenv("SAAS_MARGIN", "70-85% (LLM+API COGS)")
YEAR3_TARGET = os.getenv("YEAR3_TARGET", "10 cities / 5k protected assets")

active_websockets: set = set()
timeline_events = deque(maxlen=3000)
timeline_lock = asyncio.Lock()
_cache: OrderedDict = OrderedDict()
previous_block_hash = "0" * 64
ledger_lock = asyncio.Lock()
spawn_depth: ContextVar = ContextVar("spawn_depth", default=0)
network_state = {"mode": "CONNECTING", "reason": "booting"}
economy = {"initialized": False, "credits": 0.0, "total_earned": 0.0, "total_spent": 0.0, "minted": 0.0,
           "transactions": 0, "currency": "USD", "currency_symbol": "$", "ctx": None}
economy_lock = asyncio.Lock()
guardian = {"initialized": False, "global_health": None, "active_agents": 0, "current_region": None}
guardian_lock = asyncio.Lock()
tool_stats: Dict[str, dict] = {}
tool_stats_lock = asyncio.Lock()
metrics = {k: 0 for k in ["agents_spawned", "tasks_completed", "qod_provisioned", "grounding_violations",
                          "consent_checks", "detections_found", "investigations_run", "camara_calls",
                          "camara_errors", "sweeps", "demos_run", "rules_triggered"]}
metrics_lock = asyncio.Lock()
TOOL_REGISTRY: Dict[str, any] = {}
TOOL_PROVENANCE: Dict[str, str] = {}
active_detections: Dict[str, dict] = {}
detection_lock = asyncio.Lock()
device_state_memory: Dict[str, dict] = {}
known_assets: Dict[str, dict] = {}
device_state_lock = asyncio.Lock()
grounded_facts: Dict[str, dict] = {}
grounding_lock = asyncio.Lock()
consents: Dict[str, dict] = {}
consents_lock = asyncio.Lock()
tracked_assets: Dict[str, dict] = {}
tracked_assets_lock = asyncio.Lock()
active_scenarios: Dict[str, dict] = {}
runtime_blueprints: Dict[str, dict] = {}
adaptive = {"sweep_sec": BASE_SWEEP, "cong_threshold": 70}
adaptive_lock = asyncio.Lock()
START_TIME = time.time()
_twin_announced = {"done": False}

class ConstitutionalFailure(RuntimeError): pass
class GroundingViolation(RuntimeError): pass

def now_utc(): return datetime.now(timezone.utc)
def now_iso(): return now_utc().isoformat()
def now_human(): return now_utc().strftime("%A, %d %B %Y — %H:%M:%S UTC")

def minutes_ago(iso):
    try:
        ts = datetime.fromisoformat(str(iso).replace('Z', '+00:00'))
        if ts.tzinfo is None: ts = ts.replace(tzinfo=timezone.utc)
        return max(0.0, (now_utc() - ts).total_seconds() / 60.0)
    except Exception:
        return 9999.0

def recency_weight(m):
    return 5.0 if m <= 2 else 4.0 if m <= 5 else 3.0 if m <= 15 else 2.0 if m <= 30 else 1.5 if m <= 60 else 1.0

def severity_weight(s):
    return {"CRITICAL": 5, "HIGH": 4, "MEDIUM": 3, "LOW": 2, "INFO": 1}.get(str(s).upper(), 2)

def compute_priority(sev, det, ong):
    return round(severity_weight(sev) * recency_weight(minutes_ago(det)) * (2.0 if ong else 1.0), 2)

def safe_json_dumps(o):
    try:
        return json.dumps(o, default=str)
    except Exception:
        try:
            return json.dumps({"value": str(o)})
        except Exception:
            return json.dumps({"value": "unserializable"})

def safe_dict(x):
    if isinstance(x, dict): return x
    if isinstance(x, str):
        try:
            v = json.loads(x)
            return v if isinstance(v, dict) else {"value": v}
        except Exception:
            return {"raw": x}
    return {}

def safe_json(x):
    if isinstance(x, (dict, list)): return x
    if isinstance(x, str):
        try:
            return json.loads(x)
        except Exception:
            return x
    return x

def _mask(m):
    s = str(m)
    return s[:4] + "•" * (max(0, len(s) - 7)) + s[-3:] if len(s) > 7 else "•••"

def _valid_msisdn(m):
    return bool(re.fullmatch(r"\+?[1-9]\d{6,14}", str(m or "").strip()))

def _cong_level(v):
    if isinstance(v, dict):
        for k in ("congestion_level", "cell_load_pct", "level", "load", "value", "score"):
            if isinstance(v.get(k), (int, float)): return float(v[k])
    return float(v) if isinstance(v, (int, float)) else None

class ErrorClassifier:
    @staticmethod
    def classify(err):
        e = str(err or "").lower()
        if any(k in e for k in ["getaddrinfo", "resolve", "unreachable", "refused", "timed out", "timeout", "connect"]):
            return {"category": "CONNECTIVITY", "severity": "HIGH", "cause": "Cannot reach Nokia endpoint",
                    "action": "Labeled Digital Twin engaged."}
        if any(k in e for k in ["unauthorized", "401", "invalid_token", "invalid api key", "authentication", "missing"]):
            return {"category": "AUTHENTICATION", "severity": "CRITICAL", "cause": "Key rejected/missing",
                    "action": "Verify NOKIA_API_KEY."}
        if any(k in e for k in ["forbidden", "403"]):
            return {"category": "PERMISSION", "severity": "HIGH", "cause": "Key lacks permission", "action": "Check scopes."}
        if any(k in e for k in ["not found", "404"]):
            return {"category": "NOT_FOUND", "severity": "MEDIUM", "cause": "Resource not found",
                    "action": "Interpret semantically."}
        if any(k in e for k in ["rate", "429"]):
            return {"category": "RATE_LIMIT", "severity": "LOW", "cause": "Rate limit", "action": "Auto backoff."}
        return {"category": "UNKNOWN", "severity": "MEDIUM", "cause": str(err)[:120], "action": "Inspect feed."}

error_classifier = ErrorClassifier()

async def broadcast(d):
    d["timestamp"] = time.time()
    async with timeline_lock: timeline_events.append(d)
    dead = []
    for ws in list(active_websockets):
        try:
            await ws.send_json(d)
        except Exception:
            dead.append(ws)
    for ws in dead: active_websockets.discard(ws)

async def transparency(tn, tg, detail=""):
    await broadcast({"type": "transparency", "tool": tn, "target": str(tg)[:300], "detail": str(detail)[:300], "ts": now_iso()})

async def track(k, n=1):
    async with metrics_lock:
        if k in metrics: metrics[k] += n

async def think(a, m):
    await broadcast({"type": "thinking", "agent": a, "thought": m})

async def set_mode(mode, reason):
    network_state["mode"] = mode
    network_state["reason"] = reason
    await broadcast({"type": "network_mode", "mode": mode, "reason": reason})

class TokenBucket:
    def __init__(s, r, b):
        s.rate = r
        s.burst = b
        s.tokens = float(b)
        s.last = time.time()
        s.lock = asyncio.Lock()

    async def acquire(s, mw=10.0):
        w = 0.0
        while w < mw:
            async with s.lock:
                n = time.time()
                s.tokens = min(s.burst, s.tokens + (n - s.last) * s.rate)
                s.last = n
                if s.tokens >= 1.0:
                    s.tokens -= 1.0
                    return True
                wt = min((1.0 - s.tokens) / s.rate, 0.5)
            await asyncio.sleep(wt)
            w += wt
        return False

api_bucket = TokenBucket(2.0, 5)

class BudgetManager:
    def __init__(s):
        s.llm = 0
        s.api = 0
        s.lock = asyncio.Lock()

    async def can_llm(s):
        async with s.lock: return s.llm < MAX_LLM_CALLS

    async def can_api(s):
        async with s.lock: return s.api < MAX_API_CALLS

    async def hit_llm(s):
        async with s.lock: s.llm += 1

    async def hit_api(s):
        async with s.lock: s.api += 1

    def stats(s):
        return {"llm_remaining": max(0, MAX_LLM_CALLS - s.llm), "api_remaining": max(0, MAX_API_CALLS - s.api),
                "llm_used": s.llm, "api_used": s.api}

budget = BudgetManager()

async def spend_credits(a, r):
    async with economy_lock:
        if not economy["initialized"] or economy["credits"] < a: return False
        economy["credits"] -= a
        economy["total_spent"] += a
        economy["transactions"] += 1
        bal, sym = economy["credits"], economy["currency_symbol"]
    await broadcast({"type": "credits", "delta": -a, "reason": r, "balance": round(bal, 2), "currency": sym})
    return True

async def earn_credits(a, r, mint=False):
    async with economy_lock:
        economy["credits"] += a
        economy["total_earned"] += a
        economy["transactions"] += 1
        if mint: economy["minted"] += a
        bal, sym = economy["credits"], economy["currency_symbol"]
    await broadcast({"type": "credits", "delta": a, "reason": r, "balance": round(bal, 2), "currency": sym, "minted": mint})

async def register_fact(src, api, subject, value, raw):
    fid = "F-" + hashlib.sha1(f"{src}:{subject}:{time.time()}:{random.random()}".encode()).hexdigest()[:14]
    async with grounding_lock:
        grounded_facts[fid] = {"source_tool": src, "api": api, "subject": subject, "value": value, "ts": now_iso()}
        if len(grounded_facts) > 3000:
            for k in list(grounded_facts.keys())[:500]: del grounded_facts[k]
    return fid

async def verify_grounding(claims):
    ok, viol = [], []
    async with grounding_lock:
        for c in claims:
            fid = c.get("fact_id")
            if fid and fid in grounded_facts:
                ok.append({**c, "verified": True})
            else:
                viol.append(c)
    if viol:
        await track("grounding_violations", len(viol))
        await broadcast({"type": "grounding_violation", "violations": viol})
        raise GroundingViolation(f"{len(viol)} ungrounded claim(s) rejected")
    return ok

async def check_consent(msisdn, purpose):
    async with consents_lock:
        rec = consents.setdefault(msisdn, {"msisdn": msisdn, "purposes": [], "granted_at": now_iso()})
        if purpose not in rec["purposes"]: rec["purposes"].append(purpose)
        await track("consent_checks")

async def ledger_append(action, payload_str):
    global previous_block_hash
    async with ledger_lock:
        previous_block_hash = hashlib.sha256(f"{previous_block_hash}|{action}|{payload_str}|{now_iso()}".encode()).hexdigest()
        return previous_block_hash

@lru_cache(maxsize=16)
def get_llm(model, temp):
    return ChatGroq(temperature=temp, model_name=model, api_key=GROQ_API_KEY)

_JF = re.compile(r"```(?:json)?\s*(.*?)\s*```", re.DOTALL)

async def _llm_once(llm, msgs):
    try:
        return await llm.ainvoke(msgs, response_format={"type": "json_object"})
    except Exception as e:
        err = str(e).lower()
        if "response_format" in err or "json_object" in err or "json mode" in err:
            return await llm.ainvoke(msgs)
        raise

def _xj(raw):
    raw = (raw or "").strip()
    if raw.startswith("{"): return raw
    s = raw.find("{")
    e = raw.rfind("}")
    return raw[s:e + 1] if s != -1 and e != -1 and e > s else raw

async def llm_json(role, system, prompt, model=None, temp=0.1, retries=3):
    model = model or GROQ_MODEL_FAST
    llm = get_llm(model, temp)
    msg = prompt
    last = ""
    for attempt in range(retries + 1):
        if not await budget.can_llm(): raise ConstitutionalFailure("llm_budget_exceeded")
        await budget.hit_llm()
        try:
            resp = await _llm_once(llm, [SystemMessage(content=system), HumanMessage(content=msg)])
        except Exception as e:
            err = str(e).lower()
            last = f"transport:{str(e)[:150]}"
            wt = (2 ** attempt) * 3 if ("rate_limit" in err or "429" in err or "quota" in err) else 1.5 * (attempt + 1)
            await asyncio.sleep(min(wt, 20))
            continue
        raw = _xj(_JF.sub(r"\1", getattr(resp, "content", "") or "").strip())
        try:
            out = json.loads(raw)
            if not isinstance(out, dict): raise ValueError("not object")
            return out
        except Exception as e:
            last = f"parse:{e}"
            msg = f"Previous reply invalid JSON ({e}). Reply ONLY one valid JSON object:\n\n{raw[:1800]}"
    return {"_error": f"{role} could not produce valid JSON", "detail": last}

def load_persisted():
    global previous_block_hash
    try:
        if not os.path.exists(STATE_FILE): return False
        with open(STATE_FILE, "r", encoding="utf-8") as f: d = json.load(f)
        economy.update(d.get("economy", {}))
        metrics.update(d.get("metrics", {}))
        previous_block_hash = d.get("block_hash", previous_block_hash)
        return True
    except Exception:
        return False

free_http = httpx.AsyncClient(limits=httpx.Limits(max_connections=50, max_keepalive_connections=15),
                              timeout=httpx.Timeout(10.0, connect=5.0))

async def api_get(url, params=None, headers=None, timeout=10.0):
    if not await budget.can_api(): return {"error": "api_budget_exceeded"}
    await budget.hit_api()
    try:
        r = await free_http.get(url, params=params, headers=headers or {"User-Agent": USER_AGENT}, timeout=timeout)
        try:
            body = r.json()
        except Exception:
            body = r.text[:2000]
        return {"status": r.status_code, "data": body}
    except Exception as e:
        return {"error": str(e)[:200]}

class NokiaClient:
    PATHS = {"device-status": "device-status/v0/status", "sim-swap": "sim-swap/v0/check",
             "number-verification": "number-verification/v0/verify", "location-retrieval": "location/v0/retrieve",
             "location-verification": "location-verification/v0/verify", "congestion-insights": "congestion/v0/insights",
             "qod": "qod/v0/sessions", "geofencing": "geofencing/v0/subscriptions"}

    def __init__(s):
        s.entry = NOKIA_ENTRY
        s.resolved = None
        s.http = httpx.AsyncClient(follow_redirects=True,
                                   limits=httpx.Limits(max_connections=100, max_keepalive_connections=30),
                                   timeout=httpx.Timeout(15.0, connect=8.0))

    def _base(s):
        return (s.resolved or s.entry or "").rstrip("/")

    def _h(s):
        host = s._base().replace("https://", "").replace("http://", "").split("/")[0]
        if "rapidapi.com" in host:
            return {"X-RapidAPI-Key": NOKIA_API_KEY, "X-RapidAPI-Host": host, "Content-Type": "application/json",
                    "Accept": "application/json"}
        return {"Authorization": f"Bearer {NOKIA_API_KEY}", "x-api-key": NOKIA_API_KEY,
                "Content-Type": "application/json", "Accept": "application/json"}

    async def _learn(s, r):
        f = f"{r.url.scheme}://{r.url.host}".rstrip("/")
        if f and f != s._base():
            s.resolved = f
            await broadcast({"type": "network_discovery", "entry": s.entry, "resolved": f})

    async def discover(s):
        if not s.entry: return False
        try:
            r = await s.http.get(s.entry + "/", headers=s._h())
            await s._learn(r)
            return True
        except Exception:
            return False

    async def call(s, api, method="GET", params=None, payload=None, raw_path=False):
        path = api if raw_path else s.PATHS.get(api)
        if not path: return {"error": f"unknown api: {api}"}
        if not NOKIA_API_KEY: return {"error": "NOKIA_API_KEY missing"}
        await api_bucket.acquire()
        await budget.hit_api()
        url = f"{s._base()}/{path}"
        last = None
        for a in range(3):
            try:
                r = await s.http.request(method, url, params=params, json=payload, headers=s._h())
                await s._learn(r)
                if r.status_code == 429:
                    last = "rate limited"
                    await asyncio.sleep(2 * (a + 1))
                    continue
                if r.status_code >= 400:
                    return {"error": f"http {r.status_code}: {r.text[:200]}", "status": r.status_code}
                try:
                    data = r.json()
                except Exception:
                    data = {"raw": r.text[:1000]}
                return {"value": data, "status": r.status_code, "final_url": str(r.url)}
            except Exception as e:
                last = str(e)
                await asyncio.sleep(1.5 * (a + 1))
        return {"error": last or "unknown error"}

    async def close(s):
        await s.http.aclose()

nokia = NokiaClient()

def _twin_value(ep, params, payload, subject):
    seed = hashlib.sha1(f"{ep}:{subject}:{(params or {}).get('latitude', '')}".encode()).hexdigest()
    h = int(seed[:8], 16)
    if ep == "congestion-insights":
        lvl = 55 + (h % 40)
        return {"congestion_level": lvl, "cell_load_pct": lvl, "trend": "rising", "source": "TWIN"}
    if ep == "device-status":
        return {"status": "CONNECTED", "reachability": "REACHABLE", "signal_quality": 60 + (h % 35), "source": "TWIN"}
    if ep == "location-retrieval":
        return {"latitude": float((params or {}).get("latitude", 26.0)),
                "longitude": float((params or {}).get("longitude", 45.0)), "accuracy": 25, "source": "TWIN"}
    if ep == "sim-swap":
        return {"swapped": False, "last_swap_hours_ago": 500 + (h % 2000), "source": "TWIN"}
    if ep == "qod":
        return {"sessionId": f"TWIN-QOD-{seed[:8]}", "status": "ACTIVE",
                "qosProfile": (payload or {}).get("qosProfile", "QOS_E"), "source": "TWIN"}
    if ep == "geofencing":
        return {"subscriptionId": f"TWIN-GEO-{seed[:8]}", "status": "ACTIVE", "source": "TWIN"}
    return {"source": "TWIN"}

async def camara_free_call(ep, method="GET", params=None, payload=None, subject=None):
    pth = NokiaClient.PATHS.get(ep)
    if not pth: return {"error": f"unknown endpoint '{ep}'"}
    method = (method or "GET").upper()
    await track("camara_calls")
    await transparency("nok:" + ep, f"{nokia._base()}/{pth}", f"params={params} subject={subject}")
    res = await nokia.call(pth, method, params, payload, raw_path=True)
    if "error" in res:
        diag = error_classifier.classify(res["error"])
        if diag["category"] == "CONNECTIVITY" and TWIN_MODE in ("auto", "on"):
            value = _twin_value(ep, params, payload, subject)
            fid = await register_fact(ep, pth, subject or "network", value, value)
            if not _twin_announced["done"]:
                _twin_announced["done"] = True
                await set_mode("TWIN",
                               "Live Nokia endpoint unreachable — labeled Network Digital Twin engaged; live auto-resumes when reachable.")
            await broadcast({"type": "camara_twin", "api": ep, "fact_id": fid})
            return {"api": ep, "subject": subject, "value": value, "fact_id": fid, "mode": "TWIN",
                    "provenance": "CAMARA(TWIN)", "retrieved_at": now_iso()}
        await track("camara_errors")
        await broadcast(
            {"type": "camara_error", "api": ep, "subject": subject, "error": str(res["error"])[:200], "diagnostic": diag})
        return {"api": ep, "subject": subject, "error": res["error"], "diagnostic": diag, "value": None}
    value = res.get("value")
    fid = await register_fact(ep, pth, subject or "network", value, value)
    if res.get("final_url"): await transparency("resolved_endpoint", res["final_url"], "redirect-followed base")
    if _twin_announced["done"] and network_state["mode"] != "LIVE":
        await set_mode("LIVE", "Live Nokia endpoint reachable again — auto-resumed from twin.")
    return {"api": ep, "subject": subject, "value": value, "fact_id": fid, "mode": "LIVE", "provenance": "CAMARA",
            "retrieved_at": now_iso()}

def _looks_not_found(v):
    if v is None: return True
    s = json.dumps(v, default=str).lower() if not isinstance(v, str) else v.lower()
    return any(k in s for k in
               ["not found", "notfound", "no resource", "absent", "unknown subscriber", "no record", "404",
                "does not exist", "invalid", "unregistered"])

async def record_device_state(sub, reach, meta=None):
    async with device_state_lock:
        prev = device_state_memory.get(sub, {})
        device_state_memory[sub] = {"reachable": reach, "last_seen": now_iso() if reach else prev.get("last_seen"),
                                    "last_checked": now_iso(),
                                    "state_change": (prev.get("reachable") != reach) if prev else False,
                                    "meta": meta or prev.get("meta")}
        return device_state_memory[sub]

async def _interpret_not_found(api, sub):
    async with device_state_lock:
        prev = device_state_memory.get(sub)
        known = sub in known_assets
    if known:
        was = prev.get("reachable", True) if prev else True
        if was:
            return {"semantic": "KNOWN_ASSET_UNREACHABLE", "significance": "CRITICAL", "state_change": True,
                    "meaning": "Registered critical asset now unreachable — tunnel disconnect/outage/SIM swap.",
                    "subject": sub}
        return {"semantic": "KNOWN_ASSET_STILL_DOWN", "significance": "HIGH", "state_change": False,
                "meaning": "Registered asset remains unreachable.", "subject": sub}
    return {"semantic": "UNKNOWN_PROBE_NOT_FOUND", "significance": "LOW", "state_change": False,
            "meaning": "Unknown identifier not found.", "subject": sub}

async def classify_camara_response(api, sub, result):
    value = result.get("value") if isinstance(result, dict) else None
    error = result.get("error") if isinstance(result, dict) else None
    if error:
        diag = error_classifier.classify(error)
        cat = diag["category"]
        if cat in ("AUTHENTICATION", "PERMISSION"):
            return {"semantic": "INFRA_ERROR", "significance": "HIGH", "meaning": "API/permission problem.",
                    "diagnostic": diag}
        if cat == "RATE_LIMIT":
            return {"semantic": "TRANSIENT", "significance": "LOW", "meaning": "Rate limited.", "diagnostic": diag}
        if cat == "CONNECTIVITY":
            return {"semantic": "INFRA_ERROR", "significance": "MEDIUM", "meaning": "Cannot reach network tier.",
                    "diagnostic": diag}
        if cat == "NOT_FOUND": return await _interpret_not_found(api, sub)
        return {"semantic": "ERROR", "significance": "MEDIUM", "meaning": diag.get("cause"), "diagnostic": diag}
    if _looks_not_found(value): return await _interpret_not_found(api, sub)
    return {"semantic": "DATA", "significance": "INFO", "meaning": "Valid response.", "value": value}

async def _web_search_raw(q):
    try:
        try:
            from ddgs import DDGS
        except ImportError:
            try:
                from duckduckgo_search import DDGS
            except ImportError:
                return {"error": "web_search_unavailable", "blocked": False, "results": []}

        def _s():
            with DDGS() as d:
                return [{"title": r.get("title", ""), "snippet": r.get("body", ""), "url": r.get("href", "")} for r in
                        d.text(q, max_results=8)]

        res = await asyncio.to_thread(_s)
        return {"blocked": False, "results": res} if res else {"blocked": False, "error": "no_results", "results": []}
    except Exception as e:
        err = str(e).lower()
        if any(k in err for k in ["ratelimit", "blocked", "forbidden", "403", "banned", "captcha", "restricted"]):
            return {"blocked": True, "error": f"restricted:{str(e)[:100]}", "results": []}
        return {"blocked": False, "error": str(e)[:150], "results": []}

def register_tool(t):
    TOOL_REGISTRY[t.name] = t
    return t

@tool
async def deep_discover(intent: str, location_hint: str = "", strategies: int = 4) -> str:
    """ADAPTIVE DISCOVERY ENGINE — find events/assets even when wording varies or sources are restricted."""
    await transparency("deep_discover", intent[:200], f"{strategies} strategies")
    sn = max(1, min(6, int(strategies)))
    schema = '{"queries":["q1","q2"],"restricted_likely":false,"alt_terms":["t1"],"reasoning":"string"}'
    exp = await llm_json("DiscoveryPlanner", "Generate DIVERSE search queries (synonyms, acronyms, alt phrasings).",
                         f"INTENT: {intent}\nLOCATION: {location_hint or 'anywhere'}\nReturn ONLY JSON: {schema}")
    if "_error" in exp:
        exp = {"queries": [intent], "restricted_likely": False, "alt_terms": [], "reasoning": exp["_error"]}
    queries = [q for q in exp.get("queries", []) if isinstance(q, str)][:sn] or [intent]
    findings, log, blocked = [], [], []
    for q in queries:
        res = await _web_search_raw(q)
        if res.get("blocked") or res.get("error"):
            blocked.append({"query": q, "reason": res.get("error", "blocked")})
            log.append({"query": q, "status": "blocked", "findings": 0})
            continue
        for r in res.get("results", []):
            findings.append(
                {"title": r.get("title", ""), "snippet": r.get("snippet", ""), "url": r.get("url", ""), "matched_query": q})
        log.append({"query": q, "status": "ok", "findings": len(res.get("results", []))})
    seen, uniq = set(), []
    for f in findings:
        u = f.get("url", "")
        if u and u not in seen:
            seen.add(u)
            uniq.append(f)
    return safe_json_dumps(
        {"intent": intent, "queries_tried": log, "blocked": blocked, "findings": uniq[:15], "total": len(uniq),
         "discovered_at": now_iso()})

@tool
async def evaluate_protection_rule(context_json: str) -> str:
    """LLM evaluates the NeXus protection rule from LIVE context. No string-matching."""
    ctx = safe_json(context_json)
    schema = '{"trigger":true,"confidence":0.9,"reasoning":"string"}'
    r = await llm_json("RuleEngine",
                       "Evaluate the protection rule from the provided LIVE context. Judge trigger and confidence.",
                       f"CONTEXT: {json.dumps(ctx, default=str)[:1500]}\nRULE: IF vehicle is autonomous AND location is tunnel/limited-coverage AND congestion high predicted within 60s THEN reserve 5G slice.\nReturn ONLY JSON: {schema}")
    if r.get("trigger"): await track("rules_triggered")
    return safe_json_dumps(r)

@tool
async def camara_congestion(lat: float, lon: float) -> str:
    """CAMARA Congestion Insights — PREDICT pillar: real-time network load at a location."""
    return safe_json_dumps(await camara_free_call("congestion-insights", "GET", params={"latitude": lat, "longitude": lon}, subject=f"{lat},{lon}"))

@tool
async def camara_device_status(msisdn: str) -> str:
    """CAMARA Device Status — connectivity health of an asset."""
    await check_consent(msisdn, "device_status")
    return safe_json_dumps(await camara_free_call("device-status", "GET", params={"phoneNumber": msisdn}, subject=msisdn))

@tool
async def camara_location(msisdn: str) -> str:
    """CAMARA Location Retrieval — current location of an asset."""
    await check_consent(msisdn, "location_retrieval")
    return safe_json_dumps(await camara_free_call("location-retrieval", "GET", params={"phoneNumber": msisdn}, subject=msisdn))

@tool
async def camara_qos(msisdn: str, profile: str, duration: int) -> str:
    """CAMARA Quality on Demand — ACT pillar: reserve a dedicated 5G slice; bills a B2B micro-transaction."""
    await check_consent(msisdn, "qod_provisioning")
    out = await camara_free_call("qod", "POST", payload={"device": {"phoneNumber": msisdn}, "qosProfile": profile, "duration": duration}, subject=msisdn)
    if not out.get("error"):
        await track("qod_provisioned")
        async with economy_lock: economy["transactions"] += 1
        fee = float((economy.get("ctx") or {}).get("qod_fee") or 0)
        if fee: await spend_credits(fee, f"B2B micro-transaction QoD {profile}")
        await broadcast({"type": "network_actuation", "act": "qod_session", "msisdn": msisdn, "profile": profile, "duration": duration, "fact_id": out.get("fact_id"), "provenance": "CAMARA"})
    return safe_json_dumps(out)

@tool
async def camara_sim_swap(msisdn: str) -> str:
    """CAMARA SIM Swap — anti-hijack security check (zero-trust)."""
    await check_consent(msisdn, "sim_swap_check")
    return safe_json_dumps(await camara_free_call("sim-swap", "POST", payload={"phoneNumber": msisdn, "maxAge": 240}, subject=msisdn))

@tool
async def camara_geofence(msisdn: str, lat: float, lon: float, radius: int, event_type: str) -> str:
    """CAMARA Geofencing — corridor safety-net alert for an asset. radius in METERS."""
    await check_consent(msisdn, "geofencing")
    out = await camara_free_call("geofencing", "POST", payload={"device": {"phoneNumber": msisdn}, "area": {"areaType": "Circle", "center": {"latitude": lat, "longitude": lon}, "radius": radius}, "subscriptionDetail": {"eventType": event_type}}, subject=msisdn)
    if not out.get("error"):
        await broadcast({"type": "network_actuation", "act": "geofence", "msisdn": msisdn, "lat": lat, "lon": lon, "radius": radius, "event": event_type, "fact_id": out.get("fact_id"), "provenance": "CAMARA"})
    return safe_json_dumps(out)

@tool
async def register_detection(title: str, severity: str, detected_at: str, source: str, lat: float = None, lon: float = None, summary: str = "") -> str:
    """Register a detection on the fusion board with severity, timestamp, location."""
    det_id = f"DET-{int(time.time() * 1000)}-{random.randint(100, 999)}"
    det = {"id": det_id, "title": title, "severity": severity.upper(), "detected_at": detected_at, "source": source, "lat": lat, "lon": lon, "summary": summary, "is_ongoing": True, "priority": compute_priority(severity, detected_at, True), "minutes_ago": round(minutes_ago(detected_at), 1), "registered_at": now_iso(), "investigations": [], "status": "ACTIVE"}
    async with detection_lock: active_detections[det_id] = det
    await track("detections_found")
    await broadcast({"type": "detection_registered", "detection": det})
    return safe_json_dumps({"id": det_id, "priority": det["priority"], "minutes_ago": det["minutes_ago"]})

@tool
async def update_detection(det_id: str, is_ongoing: bool = None, status: str = None, new_info: str = "") -> str:
    """Update a detection's ongoing/resolution status or add investigation notes."""
    async with detection_lock:
        det = active_detections.get(det_id)
        if not det: return safe_json_dumps({"error": "detection_not_found"})
        if is_ongoing is not None:
            det["is_ongoing"] = is_ongoing
            det["priority"] = compute_priority(det["severity"], det["detected_at"], is_ongoing)
        if status: det["status"] = status
        if new_info: det.setdefault("investigations", []).append({"note": new_info, "ts": now_iso()})
        snap = dict(det)
    await broadcast({"type": "detection_updated", "detection": snap})
    return safe_json_dumps(snap)

@tool
async def get_priority_board() -> str:
    """Return the fusion priority board ranked by severity x recency x ongoing."""
    async with detection_lock: dets = [dict(d) for d in active_detections.values()]
    for d in dets:
        d["minutes_ago"] = round(minutes_ago(d["detected_at"]), 1)
        d["priority"] = compute_priority(d["severity"], d["detected_at"], d.get("is_ongoing", True))
    dets.sort(key=lambda x: x["priority"], reverse=True)
    await broadcast({"type": "priority_board", "detections": dets[:20]})
    return safe_json_dumps({"detections": dets[:20], "total_active": len(dets), "generated_at": now_iso()})

@tool
async def investigate_detection(det_id: str, depth: int = 2) -> str:
    """Autonomously investigate a detection — research, check ongoing, build a case file."""
    depth = depth or 2
    async with detection_lock: det = dict(active_detections.get(det_id) or {})
    if not det: return safe_json_dumps({"error": "detection_not_found"})
    await track("investigations_run")
    findings = []
    und = det.get("summary") or det.get("title", "")
    async with guardian_lock: home = guardian.get("current_region") or ""
    for i in range(depth):
        loc = f"near {det['lat']},{det['lon']}" if det.get("lat") and det.get("lon") else home
        r = safe_json(await deep_discover.ainvoke({"intent": f"Latest updates: {det.get('title', '')}. {und}", "location_hint": loc, "strategies": 3}))
        nf = r.get("findings", [])
        if nf:
            findings.extend(nf[:5])
            und += f" Also: {nf[0].get('title', '')}"
        await asyncio.sleep(1)
    schema = '{"is_ongoing":true,"confidence":0.8,"latest_update":"string","assessment":"string","recommended_action":"string"}'
    assess = await llm_json("InvestigationAnalyst", "Determine if this event is STILL ONGOING or resolved.",
                            f"EVENT: {det.get('title')}\nDETECTED: {det.get('detected_at')}\nFINDINGS:\n{json.dumps(findings[:10], default=str)[:2000]}\nReturn ONLY JSON: {schema}")
    if "_error" in assess:
        assess = {"is_ongoing": True, "confidence": 0.5, "assessment": "Unable to determine", "recommended_action": "Continue monitoring", "latest_update": ""}
    async with detection_lock:
        d = active_detections.get(det_id)
        if d:
            d["is_ongoing"] = bool(assess.get("is_ongoing", True))
            d["priority"] = compute_priority(d["severity"], d["detected_at"], d["is_ongoing"])
            d.setdefault("investigations", []).append({"findings": findings[:10], "assessment": assess, "ts": now_iso()})
            snap = dict(d)
    await broadcast({"type": "investigation_complete", "detection": snap, "assessment": assess})
    return safe_json_dumps({"detection": snap, "assessment": assess, "total_findings": len(findings)})

@tool
async def register_known_asset(msisdn: str, asset_type: str, criticality: str) -> str:
    """Register a critical asset MSISDN so its unreachability is treated as significant."""
    if not _valid_msisdn(msisdn): return safe_json_dumps({"error": "invalid_msisdn"})
    async with device_state_lock: known_assets[msisdn] = {"asset_type": asset_type, "criticality": criticality, "registered_at": now_iso()}
    return safe_json_dumps({"registered": _mask(msisdn), "asset_type": asset_type, "criticality": criticality})

@tool
async def probe_device(msisdn: str) -> str:
    """Probe a device with semantic interpretation (known asset unreachable becomes CRITICAL)."""
    await check_consent(msisdn, "device_probe")
    result = await camara_free_call("device-status", "GET", params={"phoneNumber": msisdn}, subject=msisdn)
    sem = await classify_camara_response("device-status", msisdn, result)
    await record_device_state(msisdn, sem.get("semantic") == "DATA", meta={"api": "device-status"})
    sem["device_state"] = device_state_memory.get(msisdn, {})
    if sem.get("significance") in ("CRITICAL", "HIGH") and sem.get("semantic") in ("KNOWN_ASSET_UNREACHABLE", "KNOWN_ASSET_STILL_DOWN"):
        async with device_state_lock: asset = known_assets.get(msisdn, {})
        sev = "CRITICAL" if sem.get("semantic") == "KNOWN_ASSET_UNREACHABLE" else "HIGH"
        det_id = f"DET-{int(time.time() * 1000)}-{random.randint(100, 999)}"
        det = {"id": det_id, "title": f"Critical asset unreachable: {asset.get('asset_type', 'device')} ({_mask(msisdn)})", "severity": sev, "detected_at": now_iso(), "source": "camara_device_probe", "lat": None, "lon": None, "summary": sem.get("meaning"), "is_ongoing": True, "priority": compute_priority(sev, now_iso(), True), "minutes_ago": 0.0, "investigations": [], "status": "ACTIVE", "msisdn": msisdn}
        async with detection_lock: active_detections[det_id] = det
        await track("detections_found")
        await broadcast({"type": "detection_registered", "detection": det})
        sem["auto_detection"] = det
    await broadcast({"type": "device_probe", "msisdn": _mask(msisdn), "semantic": sem})
    return safe_json_dumps(sem)

@tool
async def geocode_location(location_name: str) -> str:
    """Free geocoding via OSM Nominatim — place name to coordinates (runtime, no key)."""
    await transparency("geocode_location", location_name, "geocoding")
    k = f"geo:{location_name.lower().strip()}"
    if (c := _cache.get(k)):
        ts, v = c
        if time.time() - ts <= CACHE_TTL: return v
    r = await api_get("https://nominatim.openstreetmap.org/search", params={"q": location_name, "format": "json", "limit": 1})
    if r.get("status", 500) >= 400 or not r.get("data"): return safe_json_dumps({"error": "not_found"})
    d = (r.get("data") or [{}])[0]
    out = safe_json_dumps({"lat": float(d.get("lat", 0)), "lon": float(d.get("lon", 0)), "name": str(d.get("display_name", ""))[:120]})
    _cache[k] = (time.time(), out)
    return out

@tool
async def fetch_sovereign_context(lat: float, lon: float) -> str:
    """Country/currency/languages for coordinates (runtime, no key)."""
    k = f"sov:{round(lat, 2)}:{round(lon, 2)}"
    if (c := _cache.get(k)):
        ts, v = c
        if time.time() - ts <= CACHE_TTL: return v
    rev = await api_get("https://nominatim.openstreetmap.org/reverse", params={"lat": lat, "lon": lon, "format": "json"})
    rd = rev.get("data") if isinstance(rev.get("data"), dict) else {}
    addr = rd.get("address", {}) if isinstance(rd.get("address"), dict) else {}
    cc = str(addr.get("country_code", "")).lower()
    country = {}
    if cc:
        cr = await api_get(f"https://restcountries.com/v3.1/alpha/{cc}")
        cd = cr.get("data")
        c0 = cd[0] if isinstance(cd, list) and cd else (cd if isinstance(cd, dict) else {})
        if isinstance(c0, dict) and c0:
            nf = c0.get("name")
            curs = c0.get("currencies") or {}
            code = list(curs.keys())[0] if isinstance(curs, dict) and curs else ""
            sym = ""
            if code:
                first = curs.get(code, {}) if isinstance(curs, dict) else {}
                sym = first.get("symbol", "") if isinstance(first, dict) else ""
            country = {"name": nf.get("common") if isinstance(nf, dict) else str(nf or ""), "currency": code, "currency_symbol": sym, "region": c0.get("region", "")}
    out = safe_json_dumps({"lat": lat, "lon": lon, "address": addr, "country": country})
    _cache[k] = (time.time(), out)
    return out

@tool
async def initialize_system(lat: float, lon: float, region_name: str, currency: str = "", currency_symbol: str = "") -> str:
    """Cold-boot: Economy Guardian (LLM) sets credits, health, and per-QoD micro-transaction fee."""
    if not currency:
        sov = safe_dict(await fetch_sovereign_context.ainvoke({"lat": lat, "lon": lon}))
        c = safe_dict(sov.get("country"))
        currency = c.get("currency", "")
        currency_symbol = c.get("currency_symbol", "")
    if not currency:
        cur_schema = '{"currency":"USD","currency_symbol":"$"}'
        cur = await llm_json("EconomyGuardian", f"Infer ISO currency + symbol for '{region_name}'.",
                             f"REGION: {region_name}\nReturn ONLY JSON: {cur_schema}")
        currency = cur.get("currency", "")
        currency_symbol = cur.get("currency_symbol", "")
    vals_schema = '{"starting_credits":500.0,"initial_health":92,"qod_fee":0.03,"reasoning":"string"}'
    vals = await llm_json("EconomyGuardian", f"Initialize NeXus economy for {region_name} ({currency}).",
                          f"REGION: {region_name} ({lat},{lon}) CURRENCY: {currency}\nReturn ONLY JSON: {vals_schema}")
    if "_error" in vals: vals = {"starting_credits": 0, "initial_health": 0, "qod_fee": 0, "reasoning": "uninitialized (degraded)"}
    async with economy_lock:
        economy.update({"initialized": True, "credits": float(vals.get("starting_credits", 0)), "currency": currency or "USD", "currency_symbol": currency_symbol or "$", "ctx": {"qod_fee": float(vals.get("qod_fee", 0)), "health": int(vals.get("initial_health", 0))}})
    async with guardian_lock: guardian.update({"initialized": True, "global_health": int(vals.get("initial_health", 0)), "current_region": region_name})
    await broadcast({"type": "system_initialized", "region": region_name, "currency": currency, "currency_symbol": currency_symbol, "credits": vals.get("starting_credits"), "health": vals.get("initial_health"), "at": now_iso()})
    return safe_json_dumps({"status": "INITIALIZED", "currency": currency})

@tool
async def generate_blueprint(region: str) -> str:
    """Research a region and author its NeXus blueprint (theme, metric, description) entirely at runtime."""
    geo = safe_json(await geocode_location.ainvoke({"location_name": region}))
    if not geo or geo.get("error") or "lat" not in geo: return safe_json_dumps({"error": f"cannot geocode {region}"})
    lat, lon = float(geo["lat"]), float(geo["lon"])
    sov = safe_dict(await fetch_sovereign_context.ainvoke({"lat": lat, "lon": lon}))
    country = safe_dict(sov.get("country"))
    bp_schema = '{"theme":"EFFICIENCY","metric":"string","description":"string"}'
    bp = await llm_json("BlueprintAuthor",
                        "Author a regional smart-city blueprint: theme, headline metric, one-line description, grounded in the region only.",
                        f"REGION: {region}\nCOUNTRY: {country.get('name')}\nCURRENCY: {country.get('currency')}\nReturn ONLY JSON: {bp_schema}")
    if "_error" in bp: bp = {"theme": "EFFICIENCY", "metric": "Regional optimization", "description": f"NeXus orchestration for {region}."}
    entry = {"region": region, "lat": lat, "lon": lon, "country": country.get("name"), "currency": country.get("currency"), **bp, "generated_at": now_iso()}
    runtime_blueprints[region] = entry
    await broadcast({"type": "blueprint_generated", "blueprint": entry})
    return safe_json_dumps(entry)

@tool
async def verify_claims(claims_json: str) -> str:
    """GROUNDING GATE — verify numeric claims cite real CAMARA fact_ids; reject hallucinations."""
    claims = safe_json(claims_json)
    if not isinstance(claims, list): claims = [claims]
    try:
        v = await verify_grounding(claims)
        return safe_json_dumps({"verified": True, "count": len(v), "claims": v})
    except GroundingViolation as e:
        return safe_json_dumps({"verified": False, "error": str(e), "note": "Ungrounded numbers rejected."})

@tool
async def emit_trust_receipt(action: str, fact_ids: List[str], decision: str, outcome: str) -> str:
    """Emit an immutable hash-chained Trust Receipt proving a decision used only grounded facts."""
    h = await ledger_append(action, json.dumps({"fact_ids": fact_ids, "decision": decision, "outcome": outcome}))
    async with grounding_lock: cited = {f: grounded_facts.get(f) for f in fact_ids if f in grounded_facts}
    missing = [f for f in fact_ids if f not in cited]
    verified = len(missing) == 0
    receipt = {"receipt_id": f"TR-{int(time.time() * 1000)}", "action": action, "cited_facts": cited, "missing_facts": missing, "verified": verified, "decision": decision, "outcome": outcome, "ledger_hash": h, "issued_at": now_iso()}
    await broadcast({"type": "trust_receipt", "receipt": receipt})
    return safe_json_dumps(receipt)

@tool
async def task_complete(reason: str, final_summary: str, confidence: float, satisfied: bool, sources_used: List[str]) -> str:
    """End the mission with honest confidence and cited sources."""
    conf = max(0.0, min(1.0, float(confidence)))
    await track("tasks_completed")
    await broadcast({"type": "task_completed", "reason": reason, "summary": final_summary[:300], "confidence": conf, "satisfied": bool(satisfied), "sources": sources_used})
    return safe_json_dumps({"status": "TASK_COMPLETE"})

@tool
async def list_available_tools() -> str:
    """List all registered tools with usage stats and provenance."""
    async with tool_stats_lock: st = dict(tool_stats)
    return safe_json_dumps(
        {"tools": {n: {"desc": (t.description or "")[:140], "uses": st.get(n, {}).get("uses", 0), "provenance": TOOL_PROVENANCE.get(n, "UNKNOWN")} for n, t in TOOL_REGISTRY.items()}})

@tool
async def read_budget() -> str:
    """Read current LLM and API budget usage and remaining capacity."""
    return safe_json_dumps(budget.stats())

for _t in [deep_discover, evaluate_protection_rule, camara_congestion, camara_device_status, camara_location, camara_qos, camara_sim_swap, camara_geofence, register_detection, update_detection, get_priority_board, investigate_detection, register_known_asset, probe_device, geocode_location, fetch_sovereign_context, initialize_system, generate_blueprint, verify_claims, emit_trust_receipt, task_complete, list_available_tools, read_budget]:
    register_tool(_t)

CAMARA_TOOLS = {"camara_congestion", "camara_device_status", "camara_location", "camara_qos", "camara_sim_swap", "camara_geofence", "probe_device"}
FREE_TOOLS = {"deep_discover", "geocode_location", "fetch_sovereign_context"}
for t in TOOL_REGISTRY:
    TOOL_PROVENANCE[t] = "CAMARA" if t in CAMARA_TOOLS else ("FREE" if t in FREE_TOOLS else "INTERNAL")

async def _exec(name, tc):
    t = TOOL_REGISTRY.get(tc["name"])
    if not t: return safe_json_dumps({"error": "unknown_tool"})
    async with tool_stats_lock:
        tool_stats.setdefault(tc["name"], {"uses": 0, "fails": 0})
        tool_stats[tc["name"]]["uses"] += 1
    await broadcast({"type": "tool_exec", "agent": name, "tool": tc["name"], "provenance": TOOL_PROVENANCE.get(tc["name"], "INTERNAL")})
    try:
        out = await t.ainvoke(tc["args"] or {})
        if '"error"' in str(out)[:60]:
            async with tool_stats_lock: tool_stats[tc["name"]]["fails"] += 1
        return str(out)
    except Exception as e:
        async with tool_stats_lock: tool_stats[tc["name"]]["fails"] += 1
        return safe_json_dumps({"error": str(e)[:200]})

async def run_agent_loop(name, purpose, mission, model=None, max_steps=None, depth=0):
    model = model or GROQ_MODEL_FAST
    if spawn_depth.get() >= 2: return safe_json_dumps({"error": "max_spawn_depth"})
    token = spawn_depth.set(depth + 1)
    await track("agents_spawned")
    async with guardian_lock: guardian["active_agents"] += 1
    await broadcast({"type": "agent_deployed", "agent": name, "task": mission[:140]})
    llm = get_llm(model, 0.1).bind_tools(list(TOOL_REGISTRY.values()))
    law = (f"You are {name}. {purpose}\nOPERATING LAW: every judgment is yours; no thresholds exist in code. "
           "CAMARA tools hit the Nokia network (single NOKIA_API_KEY). Free tools need no key. "
           "Cite fact_ids for any number you report. Finish ONLY via task_complete.")
    msgs = [SystemMessage(content=law), HumanMessage(content=f"MISSION: {mission}")]
    final = ""
    ok = False
    try:
        for step in range(1, (max_steps or MAX_AGENT_STEPS) + 1):
            if not await budget.can_llm(): msgs.append(
                HumanMessage(content="LLM budget low — finalize NOW with evidence gathered."))
            await budget.hit_llm()
            try:
                resp = await llm.ainvoke([msgs[0]] + msgs[-20:])
                if getattr(resp, "content", None):
                    await broadcast({"type": "token", "chunk": resp.content, "agent": name})
            except Exception as e:
                await broadcast({"type": "structured_error", "component": f"llm:{name}", "error": str(e)[:150]})
                break
            tcs = list(getattr(resp, "tool_calls", None) or [])
            ai = AIMessage(content=getattr(resp, "content", "") or "", tool_calls=tcs)
            msgs.append(ai)
            if not ai.tool_calls:
                final = ai.content
                break
            if any(tc["name"] == "task_complete" for tc in ai.tool_calls):
                for tc in ai.tool_calls:
                    out = await _exec(name, tc)
                    msgs.append(ToolMessage(content=out[:2500], tool_call_id=tc["id"]))
                    if tc["name"] == "task_complete": final = out
                ok = "TASK_COMPLETE" in final
                break
            res = await asyncio.gather(*[_exec(name, tc) for tc in ai.tool_calls])
            for tc, out in zip(ai.tool_calls, res): msgs.append(ToolMessage(content=str(out)[:2500], tool_call_id=tc["id"]))
        else:
            final = safe_json_dumps({"error": "step_limit"})
        await broadcast({"type": "agent_report", "agent": name, "ok": ok or "TASK_COMPLETE" in final, "summary": (final or "")[:180]})
        return final or safe_json_dumps({"error": "no_final"})
    except Exception as e:
        await broadcast({"type": "structured_error", "component": f"agent:{name}", "error": str(e)[:180]})
        return safe_json_dumps({"error": "agent_crashed", "detail": str(e)[:180]})
    finally:
        async with guardian_lock: guardian["active_agents"] = max(0, guardian["active_agents"] - 1)
        spawn_depth.reset(token)

async def run_runtime_scenario(region: str, asset_type: str, fault: str, msisdn: str):
    region = (region or "").strip()
    asset_type = (asset_type or "Critical Connected Asset").strip()
    fault = (fault or "none").strip().lower()
    geo = safe_json(await geocode_location.ainvoke({"location_name": region}))
    geo_note = ""
    if not geo or geo.get("error") or "lat" not in geo:
        guess_schema = '{"lat":25.0,"lon":55.0}'
        guess = await llm_json("GeoFallback", "Estimate approximate city-center coordinates from geographic knowledge.",
                               f"PLACE: {region}\nReturn ONLY JSON: {guess_schema}")
        try:
            lat, lon = float(guess["lat"]), float(guess["lon"])
            if not (-90 <= lat <= 90 and -180 <= lon <= 180): raise ValueError("out of range")
            geo_note = "approximate coords (geocoder unreachable)"
            await transparency("geo_fallback", region, geo_note)
        except Exception:
            await broadcast({"type": "structured_error", "component": "scenario", "error": f"Could not locate '{region}'."})
            return
    else:
        lat, lon = float(geo["lat"]), float(geo["lon"])
    sov = safe_dict(await fetch_sovereign_context.ainvoke({"lat": lat, "lon": lon}))
    country = safe_dict(sov.get("country"))
    source = "operator-provided"
    synthetic = False
    if not _valid_msisdn(msisdn or ""):
        if TEST_MSISDN:
            msisdn = TEST_MSISDN
            source = "sandbox test line (NOKIA_TEST_MSISDN)"
        else:
            msisdn = "+" + "".join(random.choice("123456789") for _ in range(11))
            synthetic = True
            source = "synthetic (twin-safe)"
    async with economy_lock: init = economy["initialized"]
    if not init:
        await initialize_system.ainvoke({"lat": lat, "lon": lon, "region_name": region, "currency": country.get("currency", ""), "currency_symbol": country.get("currency_symbol", "")})
    brief_schema = '{"asset_label":"string","criticality":"CRITICAL","narrative":"string","narrative_ar":"Arabic translation","location_context":"tunnel"}'
    brief = await llm_json("ScenarioAuthor",
                           "Author a realistic smart-city protection scenario in English AND Arabic. Do NOT fabricate network readings.",
                           f"REGION: {region} ({lat},{lon})\nCOUNTRY: {country.get('name')}\nASSET: {asset_type}\nINJECTED FAULT: {fault}\nReturn ONLY JSON: {brief_schema}")
    if "_error" in brief: brief = {"asset_label": asset_type, "criticality": "HIGH", "narrative": f"Protect a {asset_type} in {region}.", "narrative_ar": "", "location_context": "urban"}
    sid = f"SCN-{int(time.time() * 1000)}"
    active_scenarios[sid] = {"status": "running"}
    await track("demos_run")
    dossier = {"asset_id": brief.get("asset_label", asset_type), "msisdn": _mask(msisdn), "msisdn_source": source, "asset_type": asset_type, "criticality": brief.get("criticality"), "region": region, "location": {"lat": lat, "lon": lon}, "status": "TRACKING", "connectivity": "unknown", "sim_integrity": "unknown", "last_location": "unknown", "injected_fault": {"field": fault, "state": "injected"}, "events": [], "qod_sessions": [], "congestion": None}
    async with device_state_lock: known_assets[msisdn] = {"asset_type": asset_type, "criticality": brief.get("criticality"), "asset_id": dossier["asset_id"], "registered_at": now_iso()}
    async with tracked_assets_lock: tracked_assets[dossier["asset_id"]] = dossier
    await broadcast({"type": "scenario_start", "scenario_id": sid, "situation": {"title": f"{region} — {asset_type}", "subtitle": f"Fault: {fault} · MSISDN: {source}" + (f" · {geo_note}" if geo_note else ""), "narrative": brief.get("narrative"), "narrative_ar": brief.get("narrative_ar", ""), "asset_id": dossier["asset_id"], "injected_fault": dossier["injected_fault"], "region": region, "lat": lat, "lon": lon}})
    await broadcast({"type": "asset_update", "asset": dossier, "scenario_id": sid})
    await asyncio.sleep(1)
    fact_ids = []
    items = []
    step = [0]

    async def emit(phase, api, status, summary, fact_id=None, is_fault=False):
        step[0] += 1
        await broadcast({"type": "scenario_step", "scenario_id": sid, "step": step[0], "phase": phase, "api": api, "status": status, "summary": summary, "fact_id": fact_id, "is_fault": is_fault, "ts": now_iso()})
        if status != "calling": items.append({"phase": phase, "api": api, "status": status, "fact_id": fact_id, "summary": summary, "is_fault": is_fault})

    await emit("PREDICT", "congestion-insights", "calling", "Sensing congestion…")
    cong = await camara_free_call("congestion-insights", "GET", params={"latitude": lat, "longitude": lon}, subject=f"{lat},{lon}")
    if cong.get("fact_id"): fact_ids.append(cong["fact_id"])
    dossier["congestion"] = cong.get("value") if not cong.get("error") else None
    await emit("PREDICT", "congestion-insights", "complete" if not cong.get("error") else "degraded", "Congestion telemetry captured" if not cong.get("error") else str(cong.get("error"))[:120], cong.get("fact_id"))
    await emit("SENSE", "device-status", "calling", "Probing connectivity…")
    ds = await camara_free_call("device-status", "GET", params={"phoneNumber": msisdn}, subject=msisdn)
    ds_sem = await classify_camara_response("device-status", msisdn, ds)
    if fault == "connectivity":
        await record_device_state(msisdn, False, meta={"demo": sid})
        dossier["connectivity"] = "LOST (fault injected)"
        dossier["events"].append({"ts": now_iso(), "event": "connectivity_lost"})
        await emit("SENSE", "device-status", "degraded", f"FAULT INJECTED: connectivity lost. Semantic: {ds_sem.get('semantic')}", ds.get("fact_id"), True)
    else:
        await record_device_state(msisdn, ds_sem.get("semantic") == "DATA", meta={"demo": sid})
        dossier["connectivity"] = "REACHABLE" if ds_sem.get("semantic") == "DATA" else ds_sem.get("semantic", "unknown")
        await emit("SENSE", "device-status", "complete" if not ds.get("error") else "degraded", f"Connectivity: {dossier['connectivity']}", ds.get("fact_id"))
    await emit("LOCATE", "location-retrieval", "calling", "Locating asset…")
    loc = await camara_free_call("location-retrieval", "GET", params={"phoneNumber": msisdn}, subject=msisdn)
    if loc.get("fact_id"): fact_ids.append(loc["fact_id"])
    if fault == "location":
        dossier["last_location"] = "UNAVAILABLE (fault injected)"
        dossier["events"].append({"ts": now_iso(), "event": "location_unavailable"})
        await emit("LOCATE", "location-retrieval", "degraded", "FAULT INJECTED: location unavailable", loc.get("fact_id"), True)
    else:
        okl = not loc.get("error") and not _looks_not_found(loc.get("value"))
        dossier["last_location"] = "acquired" if okl else "unavailable"
        await emit("LOCATE", "location-retrieval", "complete" if okl else "degraded", "Location acquired" if okl else "Location unavailable", loc.get("fact_id"))
    await emit("SECURITY", "sim-swap", "calling", "Zero-trust SIM-integrity check…")
    sw = await camara_free_call("sim-swap", "POST", payload={"phoneNumber": msisdn, "maxAge": 240}, subject=msisdn)
    if sw.get("fact_id"): fact_ids.append(sw["fact_id"])
    swapped = bool(safe_dict(sw.get("value")).get("swapped"))
    dossier["sim_integrity"] = "COMPROMISED (recent swap)" if swapped else "INTACT"
    await emit("SECURITY", "sim-swap", "complete" if not sw.get("error") else "degraded", f"SIM integrity: {dossier['sim_integrity']}", sw.get("fact_id"), swapped)
    lvl = _cong_level(dossier["congestion"])
    async with adaptive_lock: thr = adaptive["cong_threshold"]
    rule = safe_json(await evaluate_protection_rule.ainvoke({"context_json": json.dumps({"asset_type": asset_type, "location_context": brief.get("location_context"), "congestion_level": lvl, "threshold": thr, "sim_integrity": dossier["sim_integrity"], "fault": fault, "region": region})}))
    dec_schema = '{"action_needed":true,"qos_profile":"QOS_E","duration":3600,"use_geofence":false,"missing_data_handling":"string","reasoning":"string"}'
    decision = await llm_json("OrchestrationBrain", "Decide the optimal protective action using ONLY available data; compensate for any injected fault or missing data.",
                              f"REGION: {region}\nASSET: {asset_type} ({brief.get('asset_label')})\nFAULT: {fault}\nSIM: {dossier['sim_integrity']}\nRULE: {json.dumps(rule, default=str)}\nDOSSIER: {json.dumps(dossier, default=str)[:1200]}\nReturn ONLY JSON: {dec_schema}")
    if "_error" in decision: decision = {"action_needed": True, "qos_profile": "QOS_E", "duration": 3600, "use_geofence": fault == "location", "missing_data_handling": "compensate (degraded decision)", "reasoning": "degraded mode"}
    await broadcast({"type": "scenario_decision", "scenario_id": sid, "decision": decision, "rule": rule})
    if decision.get("action_needed"):
        await emit("ACT", "qod", "calling", f"Reserving 5G slice ({decision.get('qos_profile')})…")
        await check_consent(msisdn, "demo_qod")
        qod = await camara_free_call("qod", "POST", payload={"device": {"phoneNumber": msisdn}, "qosProfile": decision.get("qos_profile", "QOS_E"), "duration": int(decision.get("duration", 3600))}, subject=msisdn)
        if qod.get("fact_id"): fact_ids.append(qod["fact_id"])
        if not qod.get("error"):
            await track("qod_provisioned")
            async with economy_lock: economy["transactions"] += 1
            fee = float((economy.get("ctx") or {}).get("qod_fee") or 0)
            if fee: await spend_credits(fee, "B2B micro-transaction QoD")
            dossier["qod_sessions"].append({"profile": decision.get("qos_profile"), "fact_id": qod.get("fact_id"), "ts": now_iso()})
            await broadcast({"type": "network_actuation", "act": "qod_session", "msisdn": msisdn, "profile": decision.get("qos_profile"), "fact_id": qod.get("fact_id"), "provenance": "CAMARA"})
        await emit("ACT", "qod", "complete" if not qod.get("error") else "degraded", "5G slice reserved" if not qod.get("error") else str(qod.get("error"))[:120], qod.get("fact_id"))
        if decision.get("use_geofence"):
            await emit("ACT", "geofencing", "calling", "Arming corridor geofence…")
            g2 = await camara_free_call("geofencing", "POST", payload={"device": {"phoneNumber": msisdn}, "area": {"areaType": "Circle", "center": {"latitude": lat, "longitude": lon}, "radius": 1500}, "subscriptionDetail": {"eventType": "LEAVING"}}, subject=msisdn)
            if g2.get("fact_id"): fact_ids.append(g2.get("fact_id"))
            await emit("ACT", "geofencing", "complete" if not g2.get("error") else "degraded", "Geofence armed" if not g2.get("error") else str(g2.get("error"))[:120], g2.get("fact_id"))
    dossier["status"] = "PROTECTED"
    async with tracked_assets_lock: tracked_assets[dossier["asset_id"]] = dossier
    await broadcast({"type": "asset_update", "asset": dossier, "scenario_id": sid})
    out_schema = '{"summary":"string","reliability":"HIGH","resilience_note":"string","decision_points":["point1","point2"]}'
    outcome = await llm_json("GuaranteeEngine", "Summarize outcome, reliability, and how missing data was handled.",
                             f"REGION: {region}\nDECISION: {json.dumps(decision, default=str)}\nFAULT: {fault}\nFACT_IDS: {len(fact_ids)} grounded\nSIM: {dossier['sim_integrity']}\nReturn ONLY JSON: {out_schema}")
    if "_error" in outcome: outcome = {"summary": "Asset protected; telemetry partially degraded.", "reliability": "MEDIUM", "resilience_note": "Fallback paths engaged and labeled.", "decision_points": ["rule trigger", "QoS profile choice"]}
    rcpt = safe_json(await emit_trust_receipt.ainvoke({"action": f"protect:{dossier['asset_id']}@{region}", "fact_ids": fact_ids, "decision": json.dumps(decision, default=str), "outcome": str(outcome.get("summary", ""))[:300]}))
    active_scenarios[sid] = {"status": "complete"}
    await broadcast({"type": "scenario_complete", "scenario_id": sid, "situation_title": f"{region} — {asset_type}", "receipt": rcpt, "resilience_note": outcome.get("resilience_note", ""), "items": items, "decision_points": outcome.get("decision_points", [])})

async def operator_mission(command: str):
    if not command.strip(): return
    await broadcast({"type": "operator_command", "command": command[:200]})
    async with guardian_lock: home = guardian.get("current_region")
    mission = command + (f" (operations home region: {home})" if home else "")
    await run_agent_loop("Operator", "You execute operator intents against the live network using the registered tools.", mission)

async def detection_sweep_loop():
    beat = 0
    while True:
        async with adaptive_lock: sweep = adaptive["sweep_sec"]
        await asyncio.sleep(max(15, sweep))
        beat += 1
        await track("sweeps")
        await broadcast({"type": "detection_sweep", "beat": beat})
        async with detection_lock: dets = [dict(d) for d in active_detections.values()]
        for d in dets:
            d["minutes_ago"] = round(minutes_ago(d["detected_at"]), 1)
            d["priority"] = compute_priority(d["severity"], d["detected_at"], d.get("is_ongoing", True))
        dets.sort(key=lambda x: x["priority"], reverse=True)
        await broadcast({"type": "priority_board", "detections": dets[:20]})
        uninv = [d for d in dets if d.get("status") == "ACTIVE" and not d.get("investigations")]
        if uninv and await budget.can_llm():
            async def _inv(det_id=uninv[0]["id"]):
                try:
                    await investigate_detection.ainvoke({"det_id": det_id, "depth": 1})
                except Exception:
                    pass
            asyncio.create_task(_inv())
        if beat % 10 == 0 and await budget.can_llm():
            adj_schema = '{"sweep_sec":45,"cong_threshold":70,"reasoning":"one line"}'
            adj = await llm_json("AdaptiveController", "Adjust sweep cadence and congestion threshold within caps from live vitals.",
                                 f"SWEEP={adaptive['sweep_sec']} THRESH={adaptive['cong_threshold']} OPEN={len(dets)} BUDGET={budget.stats()}\nReturn ONLY JSON: {adj_schema}")
            if "_error" not in adj:
                async with adaptive_lock:
                    adaptive["sweep_sec"] = max(20, min(120, int(adj.get("sweep_sec", adaptive["sweep_sec"]))))
                    adaptive["cong_threshold"] = max(40, min(90, int(adj.get("cong_threshold", adaptive["cong_threshold"]))))
                    snap = dict(adaptive)
                await broadcast({"type": "adaptive_update", "adaptive": snap, "reasoning": adj.get("reasoning", "")})

async def metrics_loop():
    while True:
        async with detection_lock: nd = len([d for d in active_detections.values() if d.get("is_ongoing")])
        calls = metrics["camara_calls"]
        errs = metrics["camara_errors"]
        avail = round(100 * (calls - errs) / calls) if calls else None
        async with economy_lock: eco = {"credits": round(economy["credits"], 1), "currency": economy["currency"] or "USD", "transactions": economy["transactions"]}
        async with guardian_lock: ag = guardian["active_agents"]
        hp = guardian["global_health"]
        await broadcast({"type": "metrics", "data": {**metrics, **budget.stats(), "active_detections": nd, "agents": ag, "health": hp, "availability": avail, **eco}})
        await asyncio.sleep(6)

async def auto_save_loop():
    while True:
        await asyncio.sleep(60)
        try:
            data = {"economy": economy, "block_hash": previous_block_hash, "metrics": metrics, "saved_at": now_iso()}
            tmp = STATE_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f: json.dump(data, f, default=str)
            os.replace(tmp, STATE_FILE)
        except Exception as e:
            print(f"[Persistence] {e}")

@asynccontextmanager
async def lifespan(_app: FastAPI):
    recovered = load_persisted()
    print(f"[NeXus] state={'recovered (ledger chain continues)' if recovered else 'fresh'}")
    if NOKIA_API_KEY:
        await set_mode("CONNECTING", "probing Nokia endpoint")
        ok = await nokia.discover()
        if ok:
            await set_mode("LIVE", "LIVE: Nokia endpoint reachable — every tool call flows through NOKIA_API_KEY")
        elif TWIN_MODE in ("auto", "on"):
            await set_mode("TWIN", "TWIN: endpoint unreachable at boot — labeled twin; live auto-resumes on first success")
        else:
            await set_mode("OFFLINE", "endpoint unreachable and twin disabled")
    else:
        await set_mode("TWIN" if TWIN_MODE in ("auto", "on") else "OFFLINE", "NOKIA_API_KEY not set — network tools run on labeled twin")
    for coro in [detection_sweep_loop(), metrics_loop(), auto_save_loop()]:
        asyncio.create_task(coro)
    print(f"NeXus ready — mode={network_state['mode']} tools={len(TOOL_REGISTRY)}")
    yield
    await nokia.close()
    await free_http.aclose()
    print("[NeXus] shutdown clean")

app = FastAPI(title="NeXus | Intelligent Network Orchestration", lifespan=lifespan)

class CommandRequest(BaseModel):
    command: str

class DemoRequest(BaseModel):
    region: str = ""
    asset_type: str = ""
    fault: str = "none"
    msisdn: str = ""

class BlueprintRequest(BaseModel):
    region: str

def _auth(token: str):
    if WEBHOOK_TOKEN and token != WEBHOOK_TOKEN: raise HTTPException(401, "bad token")

@app.get("/", response_class=HTMLResponse)
async def index():
    try:
        with open("index.html", "r", encoding="utf-8") as f: return f.read()
    except FileNotFoundError:
        return HTMLResponse("<h1>NeXus UI missing</h1>")

@app.post("/api/demo")
async def api_demo(event: DemoRequest, x_nexus_token: str = Header(default="")):
    _auth(x_nexus_token)
    if not event.region.strip():
        await broadcast({"type": "structured_error", "component": "demo", "error": "Region is required — type any city."})
        return {"status": "REJECTED", "reason": "region required"}
    asyncio.create_task(run_runtime_scenario(event.region, event.asset_type, event.fault, event.msisdn))
    return {"status": "ACCEPTED"}

@app.post("/webhook/command")
async def ingest_command(event: CommandRequest, x_nexus_token: str = Header(default="")):
    _auth(x_nexus_token)
    asyncio.create_task(operator_mission(event.command))
    return {"status": "ACCEPTED"}

@app.get("/api/priority")
async def api_priority():
    async with detection_lock: dets = [dict(d) for d in active_detections.values()]
    for d in dets:
        d["minutes_ago"] = round(minutes_ago(d["detected_at"]), 1)
        d["priority"] = compute_priority(d["severity"], d["detected_at"], d.get("is_ongoing", True))
    dets.sort(key=lambda x: x["priority"], reverse=True)
    return {"board": dets[:20]}

@app.get("/api/blueprints")
async def api_blueprints():
    return {"blueprints": list(runtime_blueprints.values())}

@app.post("/api/blueprint")
async def api_blueprint(event: BlueprintRequest, x_nexus_token: str = Header(default="")):
    _auth(x_nexus_token)
    asyncio.create_task(generate_blueprint.ainvoke({"region": event.region}))
    return {"status": "ACCEPTED"}

@app.get("/api/security/posture")
async def api_security():
    async with grounding_lock: nf = len(grounded_facts)
    async with consents_lock: nc = len(consents)
    pillars = {"Grounding": f"{nf} facts minted · {metrics['grounding_violations']} ungrounded claims rejected", "Zero-Trust": "SIM-swap + device-state semantics on every asset touch", "Consent": f"{metrics['consent_checks']} purpose-scoped consent checks across {nc} subjects", "Ledger": f"SHA-256 hash chain · head {previous_block_hash[:16]}…", "Provenance": "Every fact labeled LIVE (CAMARA) or TWIN — never unlabeled"}
    compliance = ["CAMARA consent-based APIs", "GDPR-style purpose logging", "SHA-256 audit chain", "MSISDN masking at UI"]
    return {"pillars": pillars, "compliance": compliance, "consents": metrics["consent_checks"], "ledger_head": previous_block_hash}

@app.get("/api/viability")
async def api_viability():
    return {"b2g": {"city_licensing": B2G_LICENSE}, "b2b": {"micro_transaction": B2B_MODEL}, "breakeven": BREAKEVEN, "saas_margin": SAAS_MARGIN, "year3_target": YEAR3_TARGET, "transactions_to_date": economy["transactions"], "revenue_to_date": round(economy["total_spent"], 2)}

@app.websocket("/ws")
async def ws_handler(ws: WebSocket):
    await ws.accept()
    await ws.send_json({"type": "network_mode", "mode": network_state["mode"], "reason": network_state["reason"]})
    async with timeline_lock:
        replay = [e for e in timeline_events if e.get("type") not in ("token", "transparency", "detection_sweep")][-40:]
    for e in replay:
        try:
            await ws.send_json(e)
        except Exception:
            break
    active_websockets.add(ws)
    try:
        while True:
            raw = await ws.receive_text()
            try:
                d = json.loads(raw)
                if d.get("type") == "ping": await ws.send_json({"type": "pong"})
            except json.JSONDecodeError:
                continue
    except WebSocketDisconnect:
        active_websockets.discard(ws)

if __name__ == "__main__":
    import uvicorn
    if not GROQ_API_KEY: raise ValueError("GROQ_API_KEY required (brain).")
    if not NOKIA_API_KEY: print("WARNING: NOKIA_API_KEY missing — all network tools degrade to labeled TWIN.")
    uvicorn.run(app, host="0.0.0.0", port=8000)
