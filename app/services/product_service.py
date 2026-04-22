from supabase import Client
from app.services.scraper_service import get_all_products_stream
from app.spark.processor import DataProcessor
from typing import List
from datetime import datetime, timedelta

async def search_and_save_products(supabase: Client, query: str, user_id: str = None) -> List[dict]:
    # 1. Check Cache first (if we have fresh results in the last hour)
    one_hour_ago = (datetime.now() - timedelta(hours=1)).isoformat()
    try:
        # We run the DB query in a way that doesn't block if possible, 
        # but supabase-py is mostly synchronous. 
        cached_response = supabase.table("products") \
            .select("*") \
            .ilike("name", f"%{query}%") \
            .gt("created_at", one_hour_ago) \
            .limit(100) \
            .execute()
        
        if cached_response.data and len(cached_response.data) >= 20:
            print(f"Returning {len(cached_response.data)} cached results for: {query}")
            # If user_id is provided, track this search by saving the first result as a 'search' record
            if user_id and cached_response.data:
                first_prod = cached_response.data[0]
                track_activity(supabase, {
                    "user_id": user_id,
                    "name": first_prod.get("name"),
                    "price": first_prod.get("price"),
                    "category": first_prod.get("category"),
                    "url": first_prod.get("url"),
                    "source": "search"
                })
            return cached_response.data
    except Exception as e:
        print(f"Cache check failed: {e}")

    # 2. Initialize Processor (Spark is disabled by default for speed)
    processor = DataProcessor(use_spark=False)
    all_processed_results = []
    base_columns = ["name", "brand", "price", "currency", "images", "description", "stock", "category", "source", "url"]

    try:
        # 3. Scrape and process simultaneously as results arrive
        async for spider_results in get_all_products_stream(query):
            if not spider_results:
                continue
                
            # Process this batch immediately
            processed_batch = processor.process_data(spider_results, query)
            
            if processed_batch:
                try:
                    # Save this batch to Supabase immediately
                    response = supabase.table("products").insert(processed_batch).execute()
                    all_processed_results.extend(response.data)
                except Exception as e:
                    print(f"Incremental insert failed, retrying with base columns: {e}")
                    clean_batch = []
                    for item in processed_batch:
                        clean_item = {k: v for k, v in item.items() if k in base_columns}
                        clean_batch.append(clean_item)
                    response = supabase.table("products").insert(clean_batch).execute()
                    all_processed_results.extend(response.data)
        
        # Track search if user_id is provided
        if user_id and all_processed_results:
            first_prod = all_processed_results[0]
            track_activity(supabase, {
                "user_id": user_id,
                "name": first_prod.get("name"),
                "price": first_prod.get("price"),
                "category": first_prod.get("category"),
                "url": first_prod.get("url"),
                "source": "search"
            })
                    
        return all_processed_results

    except Exception as e:
        print(f"Error during streaming search and save: {e}")
        return all_processed_results
    finally:
        processor.stop()

def track_activity(supabase: Client, data: dict):
    """
    Record a search or purchase in the 'results' table.
    """
    try:
        # Ensure only relevant fields are sent to 'results' table
        tracking_data = {
            "user_id": data.get("user_id"),
            "name": data.get("name"),
            "price": data.get("price"),
            "category": data.get("category"),
            "url": data.get("url"),
            "source": data.get("source", "search")
        }
        supabase.table("results").insert(tracking_data).execute()
    except Exception as e:
        print(f"Error tracking activity: {e}")

def get_user_favorites(supabase: Client, user_id: str, limit: int = 10) -> List[dict]:
    """
    Get most 'purchased' products for a user. 
    Fallback to most recent searches if not enough purchases.
    """
    try:
        # 1. Try to get top purchases
        # Supabase/PostgREST doesn't support GROUP BY directly in the same way SQL does through the client easily
        # but we can use RPC or just fetch and process here. 
        # Given the requirements, we'll fetch and process.
        
        response = supabase.table("results") \
            .select("*") \
            .eq("user_id", user_id) \
            .eq("source", "purchase") \
            .execute()
        
        purchases = response.data or []
        
        if len(purchases) >= 3: # arbitrary threshold for "enough" purchases
            # Group by name and count
            counts = {}
            last_entry = {} # To keep the most recent price/category/url
            for p in purchases:
                name = p['name']
                counts[name] = counts.get(name, 0) + 1
                last_entry[name] = p
            
            # Sort by count
            sorted_names = sorted(counts.keys(), key=lambda x: counts[x], reverse=True)
            
            favorites = []
            for name in sorted_names[:limit]:
                entry = last_entry[name]
                favorites.append({
                    "name": name,
                    "category": entry.get("category"),
                    "price": entry.get("price"),
                    "url": entry.get("url"),
                    "count": counts[name]
                })
            return favorites

        # 2. Fallback: Recent searches
        fallback_response = supabase.table("results") \
            .select("*") \
            .eq("user_id", user_id) \
            .eq("source", "search") \
            .order("created_at", desc=True) \
            .limit(limit) \
            .execute()
            
        return fallback_response.data or []

    except Exception as e:
        print(f"Error getting favorites: {e}")
        return []

def get_products(supabase: Client, skip: int = 0, limit: int = 100, query: str = None):
    # Base query
    db_query = supabase.table("products").select("*").order("created_at", desc=True)
    
    # If a filter query is provided, search by name
    if query:
        db_query = db_query.ilike("name", f"%{query}%")
        
    response = db_query.range(skip, skip + limit - 1).execute()
    return response.data

def delete_all_products(supabase: Client):
    # This deletes all records where name is not null (effectively all)
    response = supabase.table("products").delete().neq("name", "").execute()
    return response.data

def get_product_by_id(supabase: Client, product_id: int):
    response = supabase.table("products").select("*").eq("id", product_id).single().execute()
    return response.data
