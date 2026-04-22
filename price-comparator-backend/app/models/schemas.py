from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

class ProductBase(BaseModel):
    name: str
    brand: Optional[str] = None
    price: float
    old_price: Optional[float] = None
    discount: Optional[float] = None
    currency: str
    images: List[str]
    description: Optional[str] = None
    stock: Optional[str] = None
    category: Optional[str] = None
    source: str
    url: Optional[str] = None
    sku: Optional[str] = None
    product_id: Optional[str] = None

class ProductCreate(ProductBase):
    pass

class ProductResponse(ProductBase):
    id: int
    created_at: datetime

    class Config:
        from_attributes = True

class TrackingCreate(BaseModel):
    user_id: str
    name: str
    price: float
    category: Optional[str] = None
    url: str
    source: str  # 'search' or 'purchase'

class TrackingResponse(TrackingCreate):
    id: int
    created_at: datetime

    class Config:
        from_attributes = True

class FavoriteProduct(BaseModel):
    name: str
    category: Optional[str] = None
    price: float
    url: str
    count: Optional[int] = None
