import os
from datetime import timedelta
import firebase_admin
from firebase_admin import credentials, auth

SECRET_KEY = os.getenv("JWT_SECRET", "this-is-super-secure-use-in-prod")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE = timedelta(hours=12)

# Firebase Admin configuration
FIREBASE_SERVICE_ACCOUNT_PATH = os.getenv("FIREBASE_SERVICE_ACCOUNT_PATH")
FIREBASE_PROJECT_ID = os.getenv("FIREBASE_PROJECT_ID")

def initialize_firebase():
    """Initialize Firebase Admin SDK"""
    if not firebase_admin._apps:
        if FIREBASE_SERVICE_ACCOUNT_PATH:
            # Use service account key file
            cred = credentials.Certificate(FIREBASE_SERVICE_ACCOUNT_PATH)
            firebase_admin.initialize_app(cred, {
                'projectId': FIREBASE_PROJECT_ID
            })
        else:
            # Use default credentials (for production environments)
            firebase_admin.initialize_app()

# Initialize Firebase when module is imported
initialize_firebase()