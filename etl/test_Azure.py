import os
from dotenv import load_dotenv

load_dotenv()

cs = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
print("Raw connection string:")
print(repr(cs))
print("\nSplit by semicolon:")
for part in cs.split(';'):
    print(f"  '{part}'")