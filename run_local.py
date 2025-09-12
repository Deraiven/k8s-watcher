#!/usr/bin/env python3
"""
Local development runner
"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
env_path = Path(__file__).parent / '.env'
if env_path.exists():
    load_dotenv(env_path)
    print(f"Loaded environment from {env_path}")
else:
    print(f"Warning: {env_path} not found. Using system environment variables.")

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent))

# Run the main application
if __name__ == "__main__":
    from src.main import main
    import asyncio
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutdown requested... exiting")
        sys.exit(0)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)