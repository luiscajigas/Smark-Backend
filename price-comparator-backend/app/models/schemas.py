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
