import asyncio
import sys
import os
import argparse
import bcrypt
from motor.motor_asyncio import AsyncIOMotorClient

# Add project root directory to path to load settings
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.config import settings


async def reset_user(username, password=None, disable_2fa=False):
    # Connect to MongoDB using connection URI
    client = AsyncIOMotorClient(settings.MONGO_URL)
    db = client.get_default_database()

    user = await db.users.find_one({"username": username})
    if not user:
        print(f"❌ Error: User '{username}' not found in database.")
        client.close()
        return False

    update_fields = {}

    if password:
        hashed_password = bcrypt.hashpw(
            password.encode("utf-8"), bcrypt.gensalt()
        ).decode("utf-8")
        update_fields["hashed_password"] = hashed_password
        print(
            f"🔑 Generating secure bcrypt hash and updating password for '{username}'..."
        )

    if disable_2fa:
        update_fields["two_factor_enabled"] = False
        update_fields["two_factor_secret"] = None
        update_fields["two_factor_backup_codes"] = []
        print(
            f"🔒 Disabling Two-Factor Authentication & clearing recovery codes for '{username}'..."
        )

    if not update_fields:
        print(
            "⚠️ No action specified. Use -p/--password to reset password or --disable-2fa to reset 2FA."
        )
        client.close()
        return False

    await db.users.update_one({"username": username}, {"$set": update_fields})
    print(f"✅ Success: Account for '{username}' has been successfully updated.")
    client.close()
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Velicor Admin Account Rescue & Password Reset Tool"
    )
    parser.add_argument(
        "-u", "--username", required=True, help="Username of the account to reset"
    )
    parser.add_argument("-p", "--password", help="New plain-text password to set")
    parser.add_argument(
        "--disable-2fa",
        action="store_true",
        help="Disable 2FA and clear recovery secrets",
    )

    args = parser.parse_args()

    asyncio.run(reset_user(args.username, args.password, args.disable_2fa))
