# Firebase Authentication Setup

This document explains how to set up Firebase Admin authentication for the QuantX API.

## Environment Variables

Set the following environment variables:

### Required
- `FIREBASE_PROJECT_ID`: Your Firebase project ID
- `DATABASE_URL`: Database connection string (defaults to `sqlite:///./quantx.db`)

### Firebase Credentials (choose one)

#### Option 1: Service Account Key File (Recommended for Development)
```bash
FIREBASE_SERVICE_ACCOUNT_PATH=/path/to/your/firebase-service-account.json
```

#### Option 2: Application Default Credentials (Recommended for Production)
Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable or use other Google Cloud authentication methods.

## Firebase Project Setup

1. Go to the [Firebase Console](https://console.firebase.google.com/)
2. Create a new project or select an existing one
3. Enable Authentication and configure your sign-in methods
4. Go to Project Settings > Service Accounts
5. Generate a new private key and download the JSON file
6. Set the `FIREBASE_SERVICE_ACCOUNT_PATH` to point to this file

## Database Migration

After setting up Firebase, you'll need to run a database migration to add the `firebase_uid` column to the accounts table:

```sql
ALTER TABLE accounts ADD COLUMN firebase_uid VARCHAR UNIQUE;
CREATE INDEX ix_accounts_firebase_uid ON accounts (firebase_uid);
```

## How It Works

1. The client sends a Firebase ID token in the `Authorization: Bearer <token>` header
2. The `AuthMiddleware` validates the token using Firebase Admin SDK
3. If valid, it looks up the user by `firebase_uid` in the accounts table
4. If no account exists, it creates one automatically using information from the Firebase token
5. The user account is stored in `request.state.user` for use in route handlers

## Protected Routes

All routes are protected by default except:
- `/v1/health`
- `/docs`
- `/redoc`
- `/openapi.json`
- `/v1/auth/login`
- `/v1/auth/register`

To add more anonymous routes, update the `ANON_ROUTES` tuple in `main.py`.

## Usage in Route Handlers

Access the authenticated user in your route handlers:

```python
from fastapi import Request
from models import Account

async def my_protected_route(request: Request):
    user: Account = request.state.user
    firebase_uid: str = request.state.firebase_uid
    firebase_token: dict = request.state.firebase_token
    # ... your logic here
```

Or use the existing dependency injection:

```python
from api.security.deps import current_user
from models import Account

async def my_protected_route(user: Account = Depends(current_user)):
    # ... your logic here
```











