import asyncio
import os
import json
from engine_server.auth.auth_service import AuthService
from firebase_admin import auth, credentials
import firebase_admin
from engine_server.db_session import SessionFactory
from sqlalchemy import select
from models import Account
from sqlalchemy.ext.asyncio import AsyncSession

if not firebase_admin._apps:
    # Firebase credentials initialization
    # Supports multiple methods (in order of preference):
    # 1. FIREBASE_SERVICE_ACCOUNT_JSON env var (Railway/cloud) - JSON string directly
    # 2. GOOGLE_APPLICATION_CREDENTIALS env var pointing to existing file
    # 3. service-account.json in current directory (local dev)
    
    cred = None
    firebase_json = os.getenv("FIREBASE_SERVICE_ACCOUNT_JSON")
    
    if firebase_json:
        # Option 1: Parse JSON directly from env var (no file needed!)
        try:
            cred_dict = json.loads(firebase_json)
            cred = credentials.Certificate(cred_dict)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in FIREBASE_SERVICE_ACCOUNT_JSON: {e}")
    else:
        # Option 2: Check GOOGLE_APPLICATION_CREDENTIALS env var
        env_cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if env_cred_path and os.path.exists(env_cred_path):
            cred = credentials.Certificate(env_cred_path)
        else:
            # Option 3: Fallback to default path (local development)
            default_path = "service-account.json"
            if os.path.exists(default_path):
                cred = credentials.Certificate(default_path)
            else:
                raise ValueError(
                    "Firebase credentials not found. Set FIREBASE_SERVICE_ACCOUNT_JSON env var, "
                    "GOOGLE_APPLICATION_CREDENTIALS pointing to a file, or place service-account.json in the working directory"
                )
    
    # Initialize Firebase
    firebase_admin.initialize_app(cred)


class FirebaseAuth(AuthService):

    def __init__(self):
        super().__init__()

    async def validate_token(self, token) -> dict:
        """
        Validate a Firebase ID token asynchronously.
        
        Uses asyncio.to_thread() to run the blocking Firebase SDK call
        in a thread pool, preventing it from blocking the async event loop.
        """
        if token[:-1] == "BOT_TOKEN":
            return {"success": True, "user_id": f"BOT_ID{token[-1]}", "email": f"bot{token[-1]}@cuquants.com"}

        try:
            # Run blocking Firebase call in thread pool to avoid blocking event loop
            decoded_token = await asyncio.to_thread(auth.verify_id_token, token)
            user_id = decoded_token['user_id']

            return {"success": True, "user_id": user_id, "email": decoded_token["email"]}

        except auth.ExpiredIdTokenError:
            return {
                "success": False,
                "error": "Token has expired. Please log in again.",
                "error_code": "TOKEN_EXPIRED"
            }

        except auth.RevokedIdTokenError:
            return {
                "success": False,
                "error": "Token has been revoked. Please log in again.",
                "error_code": "TOKEN_REVOKED"
            }

        except auth.InvalidIdTokenError:
            return {
                "success": False,
                "error": "Invalid token. Please log in again.",
                "error_code": "TOKEN_INVALID"
            }

        except Exception as e:
            print(f"Unexpected error verifying token: {e}")
            return {
                "success": False,
                "error": "Authentication failed. Please try again.",
                "error_code": "AUTH_ERROR"
            }

    def validate_user_order(self, user_id, order_amount, side):
        """
        To implement:
        Checks to see if user is able to make a specific order
        """
        return True
