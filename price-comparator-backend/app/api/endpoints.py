from fastapi import APIRouter, Depends, HTTPException, Query
from supabase import Client
from typing import List
from app.database.database import get_supabase
from app.models.schemas import ProductResponse, TrackingCreate, FavoriteProduct
from app.services.product_service import (
    search_and_save_products, 
    get_products, 
    get_product_by_id,
    track_activity,
    get_user_favorites
)

router = APIRouter()

@router.get("/search", response_model=List[ProductResponse])
async def search_products(
    q: str = Query(..., min_length=1), 
    user_id: str = Query(None),
    supabase: Client = Depends(get_supabase)
):
    """
    Search for products in Alkosto, Éxito and Jumbo, process them with Spark, 
    save to Supabase and return the clean data.
    """
    try:
        results = await search_and_save_products(supabase, q, user_id)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing search: {str(e)}")

@router.post("/track")
def track_product_click(data: TrackingCreate, supabase: Client = Depends(get_supabase)):
    """
    Record a product click or purchase intention.
    """
    try:
        track_activity(supabase, data.model_dump())
        return {"status": "success", "message": "Activity tracked"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error tracking activity: {str(e)}")

@router.get("/favorites/{user_id}", response_model=List[FavoriteProduct])
def get_favorites(user_id: str, limit: int = 10, supabase: Client = Depends(get_supabase)):
    """
    Get the most frequent 'purchased' products for a user, or recent searches as fallback.
    """
    try:
        favorites = get_user_favorites(supabase, user_id, limit)
        return favorites
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching favorites: {str(e)}")

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
