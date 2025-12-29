"""
Test script for Mahindra Bot Streamlit App

This script tests the app components without actually running the Streamlit server.
It verifies:
- Prerequisites check
- Service initialization
- Import functionality
- Data availability
"""

import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from dotenv import load_dotenv

# Load environment
load_dotenv()

print("=" * 80)
print("MAHINDRA BOT STREAMLIT APP - COMPONENT TEST")
print("=" * 80)
print()

# Test 1: Check imports
print("Test 1: Checking imports...")
try:
    import streamlit as st

    from mahindrabot.core import AgentToolKit, run_mahindra_bot
    from mahindrabot.services.car_service import CarService
    from mahindrabot.services.bike_service import BikeService
    from mahindrabot.services.ev_charger_service import EVChargerLocationService
    from mahindrabot.services.faq_service import FAQService
    from mahindrabot.services.llm_service import LLMConfig, ModelArgs
    from mahindrabot.services.llm_service.agent import AgentResponse
    print("✅ All imports successful")
except ImportError as e:
    print(f"❌ Import failed: {e}")
    sys.exit(1)

print()

# Test 2: Check prerequisites
print("Test 2: Checking prerequisites...")

api_key_set = bool(os.getenv("OPENAI_API_KEY"))
print(f"  {'✅' if api_key_set else '❌'} OPENAI_API_KEY: {'Set' if api_key_set else 'Not set'}")

car_data_path = Path("data/new_car_details")
car_data_exists = car_data_path.exists()
print(f"  {'✅' if car_data_exists else '❌'} Car data directory: {car_data_path}")

bike_data_path = Path("data/new_bike_details")
bike_data_exists = bike_data_path.exists()
print(f"  {'✅' if bike_data_exists else '❌'} Bike data directory: {bike_data_path}")

faq_data_path = Path("data/consolidated_faqs.json")
faq_data_exists = faq_data_path.exists()
print(f"  {'✅' if faq_data_exists else '❌'} FAQ data file: {faq_data_path}")

ev_locations_path = Path("data/ev-locations.json")
ev_locations_exists = ev_locations_path.exists()
print(f"  {'✅' if ev_locations_exists else '❌'} EV locations file: {ev_locations_path}")

all_prereqs = api_key_set and car_data_exists and faq_data_exists
if not all_prereqs:
    print()
    print("⚠️  Some prerequisites are missing. The app will show setup instructions.")
    print("   However, other components can still be tested.")

print()

# Test 3: Initialize services (if data available)
if car_data_exists and faq_data_exists:
    print("Test 3: Initializing services...")
    try:
        car_service = CarService(str(car_data_path))
        print("  ✅ CarService initialized")
        
        bike_service = BikeService(str(bike_data_path))
        print("  ✅ BikeService initialized")
        
        faq_service = FAQService(str(faq_data_path))
        print("  ✅ FAQService initialized")
        
        ev_charger_service = EVChargerLocationService(str(ev_locations_path))
        print("  ✅ EVChargerLocationService initialized")
        
        # Create toolkit
        toolkit = AgentToolKit(
            car_service=car_service,
            bike_service=bike_service,
            faq_service=faq_service,
            ev_charger_service=ev_charger_service
        )
        num_tools = len(toolkit.get_tools())
        print(f"  ✅ AgentToolKit created with {num_tools} tools")
        
        # List available tools
        print(f"    → Available tools:")
        for tool in toolkit.get_tools():
            print(f"      • {tool.name}")
        
    except Exception as e:
        print(f"  ❌ Service initialization failed: {e}")
        import traceback
        traceback.print_exc()
else:
    print("Test 3: Skipping service initialization (data not available)")

print()

# Test 4: Check LLM config
print("Test 4: Checking LLM configuration...")
try:
    llm_config = LLMConfig(
        model_id="gpt-4o-mini",
        model_args=ModelArgs(temperature=0.7, max_tokens=1500)
    )
    print(f"  ✅ LLMConfig created")
    print(f"    → Model: {llm_config.model_id}")
    print(f"    → Temperature: {llm_config.model_args.temperature}")
    print(f"    → Max tokens: {llm_config.model_args.max_tokens}")
except Exception as e:
    print(f"  ❌ LLM config failed: {e}")

print()

# Test 5: Verify app file structure
print("Test 5: Checking file structure...")

app_file = Path("streamlit_apps/mahindra_bot_app.py")
readme_file = Path("streamlit_apps/README.md")

print(f"  {'✅' if app_file.exists() else '❌'} Main app file: {app_file}")
print(f"  {'✅' if readme_file.exists() else '❌'} README file: {readme_file}")

print()

# Test 6: Test app module import
print("Test 6: Testing app module import...")
try:
    sys.path.insert(0, str(Path(__file__).parent))
    # We can't import the app directly as it will try to run streamlit
    # Instead, we'll check if the file is syntactically valid
    with open("streamlit_apps/mahindra_bot_app.py", "r") as f:
        code = f.read()
    compile(code, "mahindra_bot_app.py", "exec")
    print("  ✅ App code is syntactically valid")
except SyntaxError as e:
    print(f"  ❌ Syntax error in app: {e}")

print()

# Summary
print("=" * 80)
print("TEST SUMMARY")
print("=" * 80)

if all_prereqs:
    print("✅ All prerequisites met - App is ready to run!")
    print()
    print("To start the app, run:")
    print("  conda run -n scrape streamlit run streamlit_apps/mahindra_bot_app.py")
else:
    print("⚠️  Some prerequisites are missing:")
    if not api_key_set:
        print("  • Set OPENAI_API_KEY in environment or .env file")
    if not car_data_exists:
        print("  • Ensure car data exists at data/new_car_details/")
    if not faq_data_exists:
        print("  • Run: conda run -n scrape python scripts/consolidate_faqs.py")
    print()
    print("After fixing prerequisites, run:")
    print("  conda run -n scrape streamlit run streamlit_apps/mahindra_bot_app.py")

print()
