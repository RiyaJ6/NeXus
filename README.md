<div align="center">

# 🛰️ NeXus
### Intelligent Network Orchestration for MENA Smart Cities
#### From Reactive to Proactive — PREDICT → ACT → GUARANTEE

**MENA Open Gateway Hackathon 2026 · Theme #2: Smart Cities · Nokia Network-as-Code**

[![Live](https://img.shields.io/badge/status-LIVE%20demo-00ff9d)](#)
[![CAMARA](https://img.shields.io/badge/CAMARA%20APIs-6%20orchestrated-00e5ff)](#)
[![Agentic](https://img.shields.io/badge/brain-LangChain%20%2B%20Groq-ffc400)](#)
[![Trust](https://img.shields.io/badge/trust-hash--chained%20receipts-c05fff)](#)
[![License](https://img.shields.io/badge/license-MIT-888)](#)

> Today's networks **wait for failure**. NeXus **predicts it, prevents it and proves it** —
> an agentic brain that owns the network *before* anything breaks.

</div>

---

## 🚨 The Problem

MENA's **$1.5T** smart city ambition (NEOM $500B · Vision 2030 $1T+ · Expo/UAE $70B+) silently loses
**$2B+ every year** because networks are **reactive**:

| Failure mode | Where | Cost |
|---|---|---|
| 🚇 Autonomous vehicles lose connectivity in subterranean tunnels | NEOM | stalled fleets, safety risk |
| 🚑 Emergency response delayed by congestion spikes | Dubai | minutes = lives |
| 📉 Critical IoT data drops in dense zones | Lusail / Riyadh | blind infrastructure |

Legacy flow: **wait → detect → manual re-route → downtime.**

---

## ⚡ The Solution

NeXus closes the loop autonomously:

```
PREDICT                ACT                     GUARANTEE
Congestion Insights →  LLM decides (no       QoD slice held +
Device Status       →  hardcoded thresholds)  SHA-256 hash-chained
Location Retrieval  →  QoD + Geofence fire    Trust Receipt proves
SIM Swap            →  in <100 ms             every cited fact
```

- **90%+** degradation prediction accuracy (target)
- **<100 ms** autonomous actuation
- **99.999%** reliability guarantee, continuously verified
- **0** human interventions in the protection loop


---

## 🚀 Key Features

- 🧠 **Agentic brain** (LangChain + Groq) — tool selection, rule evaluation and decisions are model driven; **no thresholds in code**
- 🎬 **Runtime scenarios** — type *any* city, *any* asset, inject *any* fault; narrative, criticality and coordinates authored live (EN + AR)
-  **Grounding Gate + Trust Receipts** — every reported number must cite a live `fact_id`; hallucinations are rejected and surfaced as violations; receipts are SHA-256 hash-chained
- 🛰️ **Autonomous detection sweep** — fusion priority board ranks incidents by severity × recency × ongoing and self investigates
- 🎛️ **Adaptive controller** — re-tunes sweep cadence & congestion threshold from live vitals
- 🔁 **Network Digital Twin fallback** — if the Nokia endpoint is unreachable, a **clearly labelled** twin engages and **auto resumes to LIVE** on first success (honesty by design)
- 🗺️ **MENA locked holographic command deck** — key-free OSM tiles, bounded viewport, six rails (Fusion / Live / Orchestration / Blueprints / Trust / Viability)
- 🌐 **Runtime regional blueprints** — authored on demand from geocoding + sovereign context, not pasted from slides
- 💰 **Live business counters** — every QoD provisioning is a B2B micro-transaction

---

## 📡 CAMARA API Orchestration (Nokia NaC / GSMA Open Gateway)

| # | API | Pillar | Role in the loop |
|---|---|---|---|
| 1 | Congestion Insights | PREDICT | live cell-load at asset coordinates |
| 2 | Device Status | SENSE | reachability; `KNOWN_ASSET_UNREACHABLE` semantics |
| 3 | Location Retrieval | LOCATE | last-known / current position |
| 4 | SIM Swap | SECURITY | zero-trust anti-hijack identity check |
| 5 | Quality on Demand | ACT | dedicated 5G slice reservation (<100 ms) |
| 6 | Geofencing | ACT | corridor safety-net alerts |

*Reserved endpoints (Number Verification, Location Verification) are wired in the client for future expansion.*

---

## 🛡️ Trust & Zero-Trust

| Pillar | Implementation |
|---|---|
| **AUTH** | OAuth2 / MCP-style bearer; single `NOKIA_API_KEY` gates every network call |
| **PRIVACY** | per-MSISDN purpose-scoped **consent ledger** |
| **ENCRYPTION** | TLS 1.3 transport |
| **INTEGRITY** | SHA-256 **hash-chained audit log** + immutable Trust Receipts |
| **GROUNDING** | fact-ID citation gate; ungrounded claims rejected & displayed |
| **DATA MINIMISATION** | MSISDN masking at the UI layer |

Compliance posture: **GSMA Open Gateway · GDPR · UAE TRA · KSA CITC** — live-counted at `/api/security/posture`.

---

## 🏗️ Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                    APPLICATION TIER  ·  index.html                   │
│  Holographic command deck · Leaflet MENA-locked map · 6 rails        │
│  WebSocket real-time stream · Holo-Player · Trust Receipt viewer     │
└────────────────────────────────▲─────────────────────────────────────┘
                                 │  WS / REST
┌────────────────────────────────▼─────────────────────────────────────┐
│                    INTELLIGENCE TIER  ·  main.py                     │
│  FastAPI · LangChain agentic brain · Groq LLM                        │
│  PREDICT → DECIDE (LLM rules) → ACT (QoD+Geofence) → GUARANTEE       │
│  Grounding Gate · Consent Ledger · Adaptive Controller · Twin        │
│  State persistence (ledger chain + economy + metrics)                │
└────────────────────────────────▲─────────────────────────────────────┘
                                 │  HTTPS (single NOKIA_API_KEY)
┌────────────────────────────────▼─────────────────────────────────────┐
│              NETWORK TIER  ·  Nokia NaC / GSMA CAMARA                │
│  Congestion · Device Status · Location · SIM Swap · QoD · Geofence   │
└──────────────────────────────────────────────────────────────────────┘
```





<div align="center">

### NeXus — don't wait for failure.
**PREDICT → ACT → GUARANTEE**

</div>
