from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware  # <-- IMPORTANTE
from app.api.endpoints import router

app = FastAPI(
    title="Smart Price Backend API",
    description="Backend API for product search and comparison using Scrapy, Spark, and Supabase",
    version="1.0.0"
)

# --- CONFIGURACIÓN DE CORS ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permite que Chrome/Flutter se conecte
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# -----------------------------

# Include API routes
app.include_router(router)

@app.get("/")
def read_root():
    return {"message": "Welcome to Smart Price Backend API", "docs": "/docs"}

if __name__ == "__main__":
    import uvicorn
    import os
    # Asegúrate de usar host="0.0.0.0" para permitir conexiones externas
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        reload=True,
    )
