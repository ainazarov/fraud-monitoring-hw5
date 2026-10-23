"""Idempotent local demo administrator creation; preserve existing password."""
import os
from airflow.www.app import create_app

app = create_app()
with app.app_context():
    security = app.appbuilder.sm
    username = os.getenv("AIRFLOW_ADMIN_USER", "admin")
    if not security.find_user(username=username):
        user = security.add_user(username, "Homework", "Admin", "admin@example.test",
                                 security.find_role("Admin"), os.getenv("AIRFLOW_ADMIN_PASSWORD", "admin"))
        if not user:
            raise RuntimeError("Could not create local demo administrator")
    print("Airflow administrator ready")
