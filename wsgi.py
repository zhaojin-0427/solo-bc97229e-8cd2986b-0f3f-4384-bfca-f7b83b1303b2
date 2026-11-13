"""WSGI entry point used by gunicorn."""
from app import create_app

app = create_app(seed_demo=True)

if __name__ == "__main__":
    # Local development only; the container runs gunicorn.
    app.run(host="0.0.0.0", port=8000)
