import httpx
from typing import List

async def search_product(query: str) -> List[dict]:
    """
    Search products using Exito's VTEX API for more reliable and faster results.
    """
    # VTEX API allows fetching up to 50 products per request
    url = f"https://www.exito.com/api/catalog_system/pub/products/search?ft={query}&_from=0&_to=49"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
    }
    
    results = []
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=headers, timeout=20, follow_redirects=True)
            if response.status_code in [200, 206]:
                products = response.json()
                for p in products:
                    # Extract the first SKU/item
                    items = p.get("items", [])
                    if not items:
                        continue
                    
                    item = items[0]
                    sellers = item.get("sellers", [])
                    price = 0
                    old_price = 0
                    discount = 0
                    if sellers:
                        comm_sel = sellers[0].get("commertialOffer") or {}
                        price = comm_sel.get("Price") or 0
                        old_price = comm_sel.get("ListPrice") or 0
                        discount = comm_sel.get("DiscountHighSign") or 0
                    
                    results.append({
                        "name": p.get("productName", "").strip(),
                        "brand": p.get("brand", "").strip(),
                        "price": price,
                        "old_price": old_price if old_price > price else None,
                        "discount": discount if discount > 0 else None,
                        "currency": "COP",
                        "images": [img.get("imageUrl", "") for img in item.get("images", [])],
                        "description": p.get("description", "").strip(),
                        "stock": "Available" if price > 0 else "Out of stock",
                        "category": p.get("categories", ["Electrónica"])[0].strip("/"),
                        "source": "Exito",
                        "url": p.get("link", ""),
                        "sku": item.get("itemId", ""),
                        "product_id": p.get("productId", "")
                    })
    except Exception as e:
        print(f"Exito API error: {e}")
        
    return results
