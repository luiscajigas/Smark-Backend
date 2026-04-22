import httpx
import json
from typing import List

async def search_product(query: str) -> List[dict]:
    """
    Search products using Alkosto's Algolia API for reliable results.
    """
    app_id = "QX5IPS1B1Q"
    api_key = "7a8800d62203ee3a9ff1cdf74f99b268"
    index_name = "alkostoIndexAlgoliaPRD"
    
    url = f"https://{app_id}-dsn.algolia.net/1/indexes/{index_name}/query"
    
    headers = {
        "x-algolia-application-id": app_id,
        "x-algolia-api-key": api_key,
        "Content-Type": "application/json",
    }
    
    # Payload for Algolia search
    payload = {
        "params": f"query={query}&hitsPerPage=50&clickAnalytics=true"
    }
    
    results = []
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(url, headers=headers, json=payload, timeout=20)
            if response.status_code == 200:
                data = response.json()
                hits = data.get("hits", [])
                for hit in hits:
                    # Extracting data from actual Alkosto Algolia hit structure
                    price = hit.get("discountprice_double") or hit.get("pricevalue_cop_double") or 0
                    old_price = hit.get("pricevalue_cop_double") or 0
                    
                    # Discount calculation if not directly available
                    discount = 0
                    if old_price > price and old_price > 0:
                        discount = ((old_price - price) / old_price) * 100

                    # Brand is usually a list
                    brand_list = hit.get("brand_string_mv", [])
                    brand = brand_list[0] if brand_list else hit.get("marca_text") or ""
                    
                    # Image handling
                    image_url = hit.get("img-820wx820h_string") or hit.get("img-1400wx1400h_string")
                    
                    # Category
                    category_data = hit.get("hierarchicalcategory_string_mv", {})
                    category = category_data.get("lvl0", "General")

                    results.append({
                        "name": hit.get("name_text_es", "").strip(),
                        "brand": brand.strip(),
                        "price": float(price) if price else 0.0,
                        "old_price": float(old_price) if old_price > price else None,
                        "discount": float(discount) if discount > 0 else None,
                        "currency": "COP",
                        "images": [image_url] if image_url else [],
                        "description": " ".join(hit.get("keyfeatures_string_mv", [])) or hit.get("url_es_string", ""),
                        "stock": "Available" if hit.get("stocklevelstatus_string") == "inStock" else "Out of stock",
                        "category": category,
                        "source": "Alkosto",
                        "url": f"https://www.alkosto.com{hit.get('url_es_string', '')}",
                        "sku": hit.get("objectID", ""),
                        "product_id": hit.get("productid_string", "")
                    })
    except Exception as e:
        print(f"Alkosto Algolia error: {e}")
        
    return results
