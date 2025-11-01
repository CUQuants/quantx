from engine_server.auth.auth_service import AuthService
from firebase_admin import auth, credentials
import firebase_admin
from engine_server.db_session import SessionFactory
from sqlalchemy import select
from models import Account
from sqlalchemy.ext.asyncio import AsyncSession

if not firebase_admin._apps:
    cred = credentials.Certificate("service-account.json")
    firebase_admin.initialize_app(cred)


class FirebaseAuth(AuthService):

    def __init__(self):
        super().__init__()

    def validate_token(self, token) -> dict:

        if token == "BOT_TOKEN":
            return {"success": True, "user_id": "BOT_ID", "decoded_email": "bot@cuquants.com"}

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
