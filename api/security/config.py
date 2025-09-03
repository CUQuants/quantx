import os
from datetime import timedelta

SECRET_KEY = os.getenv("JWT_SECRET", "this-is-super-secure-use-in-prod")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE = timedelta(hours=12)