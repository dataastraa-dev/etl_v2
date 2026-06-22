from flask import Flask, render_template
# 1. ADD 'engine' TO YOUR IMPORTS
from core.database import init_app, db_session, engine 
# 2. IMPORT YOUR BASE MODEL
from database.models import Base 
from api.routes import bridge_bp
from api.monitoring_routes import monitoring_bp 

import strategies.validations      
import strategies.transformations  

def create_app():
    app = Flask(__name__)

    # Initialize connection pooling teardown
    init_app(app)

    # 3. ─── AUTOMATIC TABLE CREATION ───
    with app.app_context():
        # This scans models.py and creates any missing tables in PostgreSQL
        Base.metadata.create_all(bind=engine)
        print("✅ Database tables verified/created successfully.")
    # ───────────────────────────────────

    # Register your API routes
    app.register_blueprint(bridge_bp, url_prefix="/v1")
    app.register_blueprint(monitoring_bp, url_prefix="/v1/monitoring") 

    # Serve the Rule Configuration UI
    @app.route("/")
    def index():
        return render_template("index.html")

    return app

# EXPOSE THE APP GLOBALLY FOR GUNICORN (Render Deployment)
app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)