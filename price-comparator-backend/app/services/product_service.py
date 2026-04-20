from supabase import Client
from app.services.scraper_service import get_all_products_stream
from app.spark.processor import DataProcessor
from typing import List
from datetime import datetime, timedelta

async def search_and_save_products(supabase: Client, query: str) -> List[dict]:
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
            return cached_response.data
    except Exception as e:
        print(f"Cache check failed: {e}")

    # 2. Initialize Processor (Spark is disabled by default for speed)
    processor = DataProcessor(use_spark=False)
    all_processed_results = []
    base_columns = ["name", "brand", "price", "currency", "images", "description", "stock", "category", "source"]

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
                    
        return all_processed_results

    except Exception as e:
        print(f"Error during streaming search and save: {e}")
        return all_processed_results
    finally:
        processor.stop()

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
