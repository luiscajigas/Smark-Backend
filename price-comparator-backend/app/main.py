from fastapi import FastAPI
from app.api.endpoints import router

app = FastAPI(
    title="Smart Price Backend API",
    description="Backend API for product search and comparison using Scrapy, Spark, and Supabase",
    version="1.0.0"
)

# Include API routes
app.include_router(router)

@app.get("/")
def read_root():
    return {"message": "Welcome to Smart Price Backend API", "docs": "/docs"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
