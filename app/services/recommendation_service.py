import json
import os
from datetime import datetime, timedelta
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import httpx
from supabase import Client

from app.services.product_service import search_and_save_products


def _extract_json_object(text: str) -> Dict[str, Any]:
    try:
        return json.loads(text)
    except Exception:
        pass
    match = re.search(r"\{[\s\S]*\}", text)
    if not match:
        raise RuntimeError("Invalid JSON from Gemma API: no JSON object found")
    try:
        return json.loads(match.group(0))
    except Exception as e:
        raise RuntimeError(f"Invalid JSON from Gemma API: {e}")

def _normalize_text(text: str) -> str:
    if not text:
        return ""
    text = text.lower()
    replacements = {
        "á": "a",
        "é": "e",
        "í": "i",
        "ó": "o",
        "ú": "u",
        "ü": "u",
        "ñ": "n",
        "ý": "y",
    }
    for k, v in replacements.items():
        text = text.replace(k, v)
    return re.sub(r"[^\w\s]", " ", text).strip()

def _query_intent(query: str) -> Dict[str, Any]:
    qn = _normalize_text(query)
    words = [w for w in qn.split() if len(w) > 1]
    wants_phone = any(w in words for w in ["iphone", "celular", "celulares", "telefono", "telefonos", "smartphone", "movil", "moviles"])
    wants_accessory = any(w in words for w in ["estuche", "case", "funda", "protector", "cargador", "cable", "vidrio", "mica"])
    return {"normalized": qn, "words": words, "wants_phone": wants_phone, "wants_accessory": wants_accessory}

def _is_accessory(name_norm: str) -> bool:
    accessory_terms = [
        "estuche",
        "case",
        "funda",
        "protector",
        "cargador",
        "cable",
        "vidrio",
        "mica",
        "forro",
        "cover",
        "soporte",
        "audifono",
        "audifonos",
        "buds",
        "airpods",
        "powerbank",
        "bateria externa",
    ]
    return any(t in name_norm for t in accessory_terms)

def _relevance_score(product: dict, intent: Dict[str, Any]) -> float:
    name_norm = _normalize_text(str(product.get("name", "")))
    brand_norm = _normalize_text(str(product.get("brand", "")))
    category_norm = _normalize_text(str(product.get("category", "")))
    words = intent["words"]

    score = 0.0
    for w in words:
        if w in name_norm:
            score += 3.0
        if w in brand_norm:
            score += 1.5
        if w in category_norm:
            score += 1.0

    if intent["wants_phone"] and not intent["wants_accessory"] and _is_accessory(name_norm):
        score -= 6.0

    digits = re.findall(r"\d+", intent["normalized"])
    if digits:
        for d in digits:
            if d and d in name_norm:
                score += 2.0
            else:
                score -= 0.5

    return score

def _best_candidates_by_store(products: List[dict], query: str, per_store_limit: int = 5) -> Tuple[List[dict], List[dict]]:
    intent = _query_intent(query)
    buckets: Dict[str, List[dict]] = {}
    for p in products:
        store = p.get("source") or _infer_store_from_url(p.get("url")) or "Unknown"
        buckets.setdefault(store, []).append(p)

    offers: List[dict] = []
    candidates: List[dict] = []
    for store, items in buckets.items():
        ranked = []
        for p in items:
            try:
                price = float(p.get("price") or 0)
            except Exception:
                continue
            if price <= 0:
                continue
            score = _relevance_score(p, intent)
            ranked.append((score, price, p))
        ranked.sort(key=lambda x: (-x[0], x[1]))
        top = [x[2] for x in ranked[: max(per_store_limit, 1)]]
        candidates.extend(top)
        if top:
            offers.append(
                {
                    "store": store,
                    "name": top[0].get("name"),
                    "price": float(top[0].get("price") or 0),
                    "url": top[0].get("url"),
                    "relevance": float(_relevance_score(top[0], intent)),
                }
            )

    offers.sort(key=lambda x: x["price"])
    return offers, candidates

def _infer_store_from_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    try:
        host = (urlparse(url).netloc or "").lower()
    except Exception:
        return None
    if "alkosto" in host:
        return "Alkosto"
    if "exito" in host:
        return "Exito"
    if "jumbo" in host:
        return "Jumbo"
    return None


def _group_best_by_store(products: List[dict]) -> Dict[str, dict]:
    best: Dict[str, dict] = {}
    for p in products:
        store = p.get("source") or _infer_store_from_url(p.get("url")) or "Unknown"
        try:
            price = float(p.get("price") or 0)
        except Exception:
            price = 0
        if price <= 0:
            continue
        if store not in best or price < float(best[store].get("price") or 0):
            best[store] = p
    return best


def _pick_best_offer(best_by_store: Dict[str, dict]) -> Optional[dict]:
    offers = []
    for store, p in best_by_store.items():
        try:
            price = float(p.get("price") or 0)
        except Exception:
            continue
        if price > 0:
            offers.append((price, store, p))
    if not offers:
        return None
    offers.sort(key=lambda x: x[0])
    return offers[0][2]


def _estimate_monthly_budget_from_history(rows: List[dict]) -> Optional[float]:
    prices = []
    for r in rows:
        try:
            price = float(r.get("price") or 0)
        except Exception:
            continue
        if price > 0:
            prices.append(price)
    if not prices:
        return None
    return float(sum(prices))


def _consumption_summary(rows: List[dict]) -> Dict[str, Any]:
    categories: Dict[str, int] = {}
    stores: Dict[str, int] = {}
    total_events = 0
    for r in rows:
        total_events += 1
        category = (r.get("category") or "Unknown").strip() or "Unknown"
        categories[category] = categories.get(category, 0) + 1
        store = _infer_store_from_url(r.get("url")) or "Unknown"
        stores[store] = stores.get(store, 0) + 1
    top_categories = sorted(categories.items(), key=lambda x: x[1], reverse=True)[:5]
    top_stores = sorted(stores.items(), key=lambda x: x[1], reverse=True)[:3]
    return {
        "events": total_events,
        "top_categories": [{"category": k, "count": v} for k, v in top_categories],
        "top_stores": [{"store": k, "count": v} for k, v in top_stores],
    }


async def _call_gemma_api(context: Dict[str, Any]) -> Dict[str, Any]:
    base_url = os.getenv("GEMMA_API_BASE_URL", "").rstrip("/")
    api_key = os.getenv("GEMMA_API_KEY", "")
    model = os.getenv("GEMMA_MODEL", "gemma-4")
    timeout_s = float(os.getenv("GEMMA_TIMEOUT", "30"))

    if not base_url:
        raise RuntimeError("GEMMA_API_BASE_URL is required")

    parsed = urlparse(base_url)
    is_google_genai = "generativelanguage.googleapis.com" in (parsed.netloc or "").lower() or base_url.endswith("/v1beta")
    headers = {"Content-Type": "application/json"}

    schema = {
        "type": "object",
        "properties": {
            "summary": {"type": "string"},
            "decision": {
                "type": "object",
                "properties": {
                    "recommended_store": {"type": "string"},
                    "recommended_product_name": {"type": "string"},
                    "recommended_price": {"type": "number"},
                    "recommended_url": {"type": ["string", "null"]},
                    "why": {"type": "string"},
                },
                "required": ["recommended_store", "recommended_product_name", "recommended_price", "recommended_url", "why"],
            },
            "best_store": {"type": "string"},
            "projected_savings": {"type": "number"},
            "budget_impact_percent": {"type": ["number", "null"]},
            "budget_impact_explanation": {"type": "string"},
            "comparison": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "store": {"type": "string"},
                        "product_name": {"type": "string"},
                        "price": {"type": "number"},
                        "savings_vs_best": {"type": "number"},
                    },
                    "required": ["store", "product_name", "price", "savings_vs_best"],
                },
            },
            "consumption_analysis": {
                "type": "object",
                "properties": {
                    "events": {"type": "number"},
                    "top_categories": {"type": "array"},
                    "top_stores": {"type": "array"},
                    "insights": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["events", "top_categories", "top_stores", "insights"],
            },
            "recommendations": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "title": {"type": "string"},
                        "action": {"type": "string"},
                        "store": {"type": "string"},
                        "product_name": {"type": "string"},
                        "price": {"type": "number"},
                        "reason": {"type": "string"},
                        "expected_savings": {"type": "number"},
                        "confidence": {"type": "number"},
                    },
                    "required": ["title", "action", "store", "product_name", "price", "reason", "expected_savings", "confidence"],
                },
            },
            "notes": {"type": "array", "items": {"type": "string"}},
        },
        "required": [
            "summary",
            "decision",
            "best_store",
            "projected_savings",
            "budget_impact_percent",
            "budget_impact_explanation",
            "comparison",
            "consumption_analysis",
            "recommendations",
            "notes",
        ],
    }

    user_text = json.dumps({"schema": schema, "context": context}, ensure_ascii=False)
    system_text = (
        "Eres un asistente de recomendaciones de compra para comparar tiendas. Responde en español. "
        "Usa solo los datos del CONTEXT_JSON (precios, tiendas, presupuesto, historial). "
        "No inventes precios, productos ni tiendas. Devuelve SOLO JSON válido y estricto que cumpla el schema."
    )

    async with httpx.AsyncClient(timeout=timeout_s) as client:
        if is_google_genai:
            if not api_key:
                raise RuntimeError("GEMMA_API_KEY is required for Google Generative Language API")
            model_path = model if model.startswith("models/") else f"models/{model}"
            url = f"{base_url}/{model_path}:generateContent"
            combined_text = (
                f"{system_text}\n\n"
                f"CONTEXT_JSON:\n{user_text}\n\n"
                "Responde SOLO con JSON válido (sin markdown) y que cumpla exactamente el schema."
            )
            payload = {
                "contents": [{"role": "user", "parts": [{"text": combined_text}]}],
                "generationConfig": {
                    "temperature": 0.2,
                    "maxOutputTokens": 600,
                    "responseMimeType": "application/json",
                    "responseSchema": schema,
                },
            }
            resp = await client.post(url, headers=headers, params={"key": api_key}, json=payload)
            if resp.status_code >= 400:
                payload_fallback = {
                    "contents": [{"role": "user", "parts": [{"text": combined_text}]}],
                    "generationConfig": {"temperature": 0.2, "maxOutputTokens": 600},
                }
                resp = await client.post(url, headers=headers, params={"key": api_key}, json=payload_fallback)
            if resp.status_code >= 400:
                raise RuntimeError(f"Gemma API error {resp.status_code}: {resp.text}")
            data = resp.json()
        else:
            if base_url.endswith("/v1"):
                url = f"{base_url}/chat/completions"
            else:
                url = f"{base_url}/v1/chat/completions"
            if api_key:
                headers_auth = {**headers, "Authorization": f"Bearer {api_key}"}
            else:
                headers_auth = headers
            payload = {
                "model": model,
                "messages": [
                    {"role": "system", "content": system_text},
                    {"role": "user", "content": user_text},
                ],
                "temperature": 0.2,
                "max_tokens": 600,
                "response_format": {"type": "json_object"},
            }
            resp = await client.post(url, headers=headers_auth, json=payload)
            if resp.status_code >= 400:
                payload_retry = {k: v for k, v in payload.items() if k != "response_format"}
                resp = await client.post(url, headers=headers_auth, json=payload_retry)
            resp.raise_for_status()
            data = resp.json()

    if is_google_genai:
        parts = (
            (((data or {}).get("candidates") or [{}])[0].get("content") or {}).get("parts")
            or []
        )
        content = "\n".join([p.get("text", "") for p in parts if isinstance(p, dict)])
    else:
        content = (
            (((data or {}).get("choices") or [{}])[0].get("message") or {}).get("content")
            or ""
        )
    if not content:
        raise RuntimeError("Empty response from Gemma API")
    try:
        return _extract_json_object(content)
    except Exception:
        if not is_google_genai:
            raise
        fix_text = (
            "Convierte el siguiente texto en JSON válido ESTRICTO. "
            "Devuelve SOLO el JSON, sin markdown ni explicación.\n\n"
            f"TEXTO:\n{content}"
        )
        async with httpx.AsyncClient(timeout=timeout_s) as fix_client:
            resp = await fix_client.post(
                url,
                headers=headers,
                params={"key": api_key},
                json={
                    "contents": [{"role": "user", "parts": [{"text": fix_text}]}],
                    "generationConfig": {"temperature": 0.0, "maxOutputTokens": 700, "responseMimeType": "application/json"},
                },
            )
        if resp.status_code >= 400:
            raise RuntimeError(f"Gemma API error {resp.status_code}: {resp.text}")
        data_fix = resp.json()
        parts_fix = (
            (((data_fix or {}).get("candidates") or [{}])[0].get("content") or {}).get("parts")
            or []
        )
        content_fix = "\n".join([p.get("text", "") for p in parts_fix if isinstance(p, dict)])
        if not content_fix:
            raise RuntimeError("Empty response from Gemma API (json fix)")
        return _extract_json_object(content_fix)


async def get_ai_recommendations(
    supabase: Client,
    query: str,
    user_id: Optional[str] = None,
    monthly_budget: Optional[float] = None,
) -> Dict[str, Any]:
    products = await search_and_save_products(supabase, query, user_id=user_id)
    per_store_limit = 5
    offers, candidates = _best_candidates_by_store(products, query, per_store_limit=per_store_limit)
    best_offer = offers[0] if offers else None

    now = datetime.now()
    since_30d = (now - timedelta(days=30)).isoformat()
    history_rows: List[dict] = []
    if user_id:
        try:
            history_resp = (
                supabase.table("results")
                .select("*")
                .eq("user_id", user_id)
                .gte("created_at", since_30d)
                .execute()
            )
            history_rows = history_resp.data or []
        except Exception:
            history_rows = []

    inferred_budget = monthly_budget
    if inferred_budget is None:
        inferred_budget = _estimate_monthly_budget_from_history(
            [r for r in history_rows if (r.get("source") or "") == "purchase"]
        )

    projected_savings = 0.0
    if len(offers) >= 2:
        projected_savings = float(offers[-1]["price"] - offers[0]["price"])

    budget_impact_percent: Optional[float] = None
    if inferred_budget and best_offer:
        try:
            budget_impact_percent = (float(best_offer.get("price") or 0) / float(inferred_budget)) * 100.0
        except Exception:
            budget_impact_percent = None

    context = {
        "query": query,
        "offers_ranked": offers[:10],
        "best_offer": best_offer,
        "projected_savings": projected_savings,
        "monthly_budget": inferred_budget,
        "budget_impact_percent": budget_impact_percent,
        "consumption": _consumption_summary(history_rows) if user_id else {"events": 0, "top_categories": [], "top_stores": []},
        "notes": {
            "per_store_limit": per_store_limit,
            "history_window_days": 30,
        },
    }

    llm_output: Optional[Dict[str, Any]] = None
    llm_error: Optional[str] = None
    for attempt in range(2):
        try:
            llm_output = await _call_gemma_api(context)
            llm_error = None
            break
        except Exception as e:
            msg = str(e)
            if not msg:
                msg = f"{type(e).__name__}: {repr(e)}"
            llm_error = msg
            if attempt == 0 and isinstance(e, httpx.TimeoutException):
                continue
            break

    best_store = None
    if best_offer:
        best_store = best_offer.get("store")
    if llm_output and isinstance(llm_output, dict) and llm_output.get("best_store"):
        best_store = llm_output.get("best_store")

    response: Dict[str, Any] = {
        "query": query,
        "best_store": best_store,
        "projected_savings": projected_savings,
        "monthly_budget": inferred_budget,
        "budget_impact_percent": budget_impact_percent,
        "offers": offers,
        "consumption": context["consumption"],
        "ai": llm_output,
        "ai_error": llm_error,
    }
    return response
