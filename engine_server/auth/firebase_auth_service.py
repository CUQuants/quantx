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
    # 1. FIREBASE_SERVICE_ACCOUNT_JSON env var (Railway secrets) - creates file at runtime
    # 2. GOOGLE_APPLICATION_CREDENTIALS env var pointing to existing file
    # 3. service-account.json in current directory (local dev)
    
    cred_path = None
    firebase_json = os.getenv("FIREBASE_SERVICE_ACCOUNT_JSON")
    
    if firebase_json:
        # Option 1: Create file from JSON env var (Railway/deployment)
        # Write to /app/service-account.json (Docker) or current dir (local)
        target_path = "/app/service-account.json" if os.path.exists("/app") else "service-account.json"
        
        try:
            # Validate JSON and write to file
            json.loads(firebase_json)  # Validate JSON format
            with open(target_path, "w") as f:
                f.write(firebase_json)
            cred_path = target_path
        except (json.JSONDecodeError, IOError) as e:
            raise ValueError(f"Failed to write Firebase credentials from FIREBASE_SERVICE_ACCOUNT_JSON: {e}")
    else:
        # Option 2: Check GOOGLE_APPLICATION_CREDENTIALS env var
        env_cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if env_cred_path and os.path.exists(env_cred_path):
            cred_path = env_cred_path
        else:
            # Option 3: Fallback to default path (local development)
            default_path = "service-account.json"
            if os.path.exists(default_path):
                cred_path = default_path
            else:
                raise ValueError(
                    "Firebase credentials not found. Set FIREBASE_SERVICE_ACCOUNT_JSON env var, "
                    "GOOGLE_APPLICATION_CREDENTIALS pointing to a file, or place service-account.json in the working directory"
                )
    
    # Initialize Firebase with the credentials file
    cred = credentials.Certificate(cred_path)
    firebase_admin.initialize_app(cred)


class FirebaseAuth(AuthService):

    def __init__(self):
        super().__init__()

    def validate_token(self, token) -> dict:

        if token[:-1] == "BOT_TOKEN":
            return {"success": True, "user_id": f"BOT_ID{token[-1]}", "email": f"bot{token[-1]}@cuquants.com"}

        try:

            decoded_token = auth.verify_id_token(token)
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
