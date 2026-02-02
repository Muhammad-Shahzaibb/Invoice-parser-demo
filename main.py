# main.py
import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=8000,
        reload=False,      # NEVER true in production
        workers=1,         # scale by CPU cores
        log_level="info"
    )
