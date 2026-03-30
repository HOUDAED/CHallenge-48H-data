"""
Entry point — starts the FastAPI server.
Usage: python3 main.py
       or: uvicorn main:app --reload
"""
import uvicorn
from src.api.app import app  # noqa: F401 — needed for `uvicorn main:app`

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
