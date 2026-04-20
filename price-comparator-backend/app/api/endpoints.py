from fastapi import APIRouter, Depends, HTTPException, Query
from supabase import Client
from typing import List
from app.database.database import get_supabase
from app.models.schemas import ProductResponse
from app.services.product_service import search_and_save_products, get_products, get_product_by_id

router = APIRouter()

@router.get("/search", response_model=List[ProductResponse])
async def search_products(q: str = Query(..., min_length=1), supabase: Client = Depends(get_supabase)):
    """
    Search for products in Alkosto, Éxito and Jumbo, process them with Spark, 
    save to Supabase and return the clean data.
    """
    try:
        results = await search_and_save_products(supabase, q)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing search: {str(e)}")

@router.get("/products", response_model=List[ProductResponse])
def list_products(skip: int = 0, limit: int = 100, supabase: Client = Depends(get_supabase)):
    """
    List all products stored in the database.
    """
    return get_products(supabase, skip, limit)

@router.get("/products/{product_id}", response_model=ProductResponse)
def read_product(product_id: int, supabase: Client = Depends(get_supabase)):
    """
    Get a specific product by ID.
    """
    try:
        db_product = get_product_by_id(supabase, product_id)
        if db_product is None:
            raise HTTPException(status_code=404, detail="Product not found")
        return db_product
    except Exception:
        raise HTTPException(status_code=404, detail="Product not found")
