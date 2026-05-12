import asyncio
from app.connectors.alkosto_spider import search_product as search_alkosto
from app.connectors.exito_spider import search_product as search_exito
from app.connectors.jumbo_spider import search_product as search_jumbo
from typing import List, AsyncGenerator

async def get_all_products_stream(query: str, per_store_limit: int = 5) -> AsyncGenerator[List[dict], None]:
    """
    Yields results from each spider as soon as they finish using asyncio.
    """
    tasks = [
        search_alkosto(query, limit=per_store_limit),
        search_exito(query, limit=per_store_limit),
        search_jumbo(query, limit=per_store_limit)
    ]
    
    # Run all tasks in parallel and yield results as they complete
    for task in asyncio.as_completed(tasks):
        results = await task
        if results:
            yield results

async def get_all_products(query: str, per_store_limit: int = 5) -> List[dict]:
    # Returns all results combined
    all_results = []
    async for results in get_all_products_stream(query, per_store_limit=per_store_limit):
        all_results.extend(results)
    return all_results
