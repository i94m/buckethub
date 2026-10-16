"""WSGI entry: Flask API + serve the built React SPA from web/dist (stand-in for 202's nginx)."""
import os
from flask import send_from_directory, jsonify
from wsgi import app  # runs validate_config() + init_runtime()

DIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), "web", "dist")


@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def _spa(path):
    if path and os.path.exists(os.path.join(DIST, path)):
        return send_from_directory(DIST, path)
    return send_from_directory(DIST, "index.html")


@app.route("/api/<path:path>")
def _api404(path):
    """Unknown API route -> JSON 404 (do not let the SPA fallback answer API calls)."""
    return jsonify(error="not_found", path=path), 404
