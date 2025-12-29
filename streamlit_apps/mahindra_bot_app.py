"""
Mahindra Bot Streamlit Application

A rich, interactive web interface for the Mahindra Bot that provides:
- Chat interface with streaming responses
- Intent classification visualization
- Tool execution tracking
- Conversation history management

Usage:
    streamlit run streamlit_apps/mahindra_bot_app.py
"""

import os
import sys
from pathlib import Path

import streamlit as st
from dotenv import load_dotenv

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from langfuse import observe

from mahindrabot.core import AgentToolKit, run_mahindra_bot
from mahindrabot.core.intents import classify_intent
from mahindrabot.core.models import Intent
from mahindrabot.services.car_service import CarService
from mahindrabot.services.ev_charger_service import EVChargerLocationService
from mahindrabot.services.faq_service import FAQService
from mahindrabot.services.llm_service import LLMConfig, ModelArgs, UserMessage
from mahindrabot.services.llm_service.agent import AgentResponse

# Load environment variables
load_dotenv()

# Langfuse will automatically track calls with @observe decorator
# ============================================================================
# Configuration and Setup
# ============================================================================

def check_login() -> bool:
    """Check if user is authenticated or if DEBUG mode is enabled."""
    # Skip login if DEBUG mode is enabled
    debug_mode = os.getenv("DEBUG", "").lower() in ("true", "1", "yes")
    if debug_mode:
        return True
    
    return st.session_state.get("authenticated", False)


def show_login_page():
    """Display login page and handle authentication."""
    st.set_page_config(
        page_title="Mahindra Bot - Login",
        page_icon="🚗",
        initial_sidebar_state="collapsed"
    )
    
    # Center the login form
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.markdown("<br><br>", unsafe_allow_html=True)
        st.title("🚗 Mahindra Bot")
        st.markdown("### Login to Continue")
        st.caption("Your AI Assistant for Cars, Insurance & Bookings")
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Login form
        with st.form("login_form"):
            password = st.text_input("Password", type="password", placeholder="Enter password")
            submit = st.form_submit_button("🔐 Login", use_container_width=True)
            
            if submit:
                # Get password from environment variable
                correct_password = os.getenv("APP_PASSWORD")
                
                if not correct_password:
                    st.error("❌ APP_PASSWORD not configured in environment variables")
                    st.info("Please set APP_PASSWORD in your .env file or environment")
                elif password == correct_password:
                    st.session_state.authenticated = True
                    st.success("✅ Login successful!")
                    st.rerun()
                else:
                    st.error("❌ Incorrect password")

def check_prerequisites() -> tuple[bool, list[str]]:
    """Check if all prerequisites are met for running the app."""
    errors = []
    
    # Check for API key
    if not os.getenv("OPENAI_API_KEY"):
        errors.append("❌ OPENAI_API_KEY not set in environment")
    
    # Check for data directories
    car_data_path = Path("data/new_car_details")
    if not car_data_path.exists():
        errors.append(f"❌ Car data directory not found: {car_data_path}")
    
    faq_data_path = Path("data/consolidated_faqs.json")
    if not faq_data_path.exists():
        errors.append(f"❌ FAQ data file not found: {faq_data_path}")
    
    ev_locations_path = Path("data/ev-locations.json")
    if not ev_locations_path.exists():
        errors.append(f"❌ EV locations data file not found: {ev_locations_path}")
    
    return len(errors) == 0, errors


def initialize_services():
    """Initialize car, FAQ, and EV charger services."""
    try:
        data_path = Path("data")
        faq_data_path = Path("data/consolidated_faqs.json")
        ev_locations_path = Path("data/ev-locations.json")
        
        car_service = CarService(str(data_path))
        faq_service = FAQService(str(faq_data_path))
        ev_charger_service = EVChargerLocationService(str(ev_locations_path))
        
        return car_service, faq_service, ev_charger_service, None
    except Exception as e:
        return None, None, None, str(e)


# ============================================================================
# Message Rendering Functions
# ============================================================================

def render_ai_response(response: AgentResponse, intent: Intent | None = None):
    """
    Render AI response with intent, tool calls, and final message.
    
    Args:
        response: AgentResponse object containing steps and final message
        intent: Optional Intent object to display as a badge
    """
    # Display intent badge if available
    if intent:
        intent_emoji = {
            "greeting": "👋",
            "general_qna": "🤔",
            "car_recommendation": "🚗",
            "car_comparison": "⚖️",
            "book_ride": "📅",
            "find_ev_charger_location": "🔌"
        }
        emoji = intent_emoji.get(intent.intent_name.value, "❓")
        intent_display = intent.intent_name.value.replace('_', ' ').title()
        
        # Display as a colored badge using markdown
        st.markdown(
            f'<span style="background-color: #0066cc; color: white; padding: 4px 12px; '
            f'border-radius: 12px; font-size: 0.85em; display: inline-block; margin-bottom: 8px;">'
            f'{emoji} {intent_display} • {intent.confidence:.0%}</span>',
            unsafe_allow_html=True
        )
    
    # Render each step with tool calls
    for step in response.steps:
        # Show AI message content if any (preamble before tool calls)
        if step.ai_message.content:
            st.markdown(step.ai_message.content)
        
        # Show tool executions if step is done
        if step.status == "done":
            for tool_result in step.tool_results:
                # Determine status icon
                status_icon = "✅" if tool_result.status == 1 else "❌"
                
                # Create expander for tool details
                with st.expander(
                    f"{status_icon} Tool: {tool_result.name}",
                    expanded=False
                ):
                    # Show input
                    if tool_result.input:
                        st.markdown("**Input:**")
                        st.json(tool_result.input)
                    
                    # Show output
                    if tool_result.output:
                        st.markdown("**Output:**")
                        # Limit output display to reasonable length
                        output_text = tool_result.output
                        if isinstance(output_text, dict):
                            st.json(output_text)
                        else:
                            st.code(output_text)
                    
                    # Show metadata if available
                    if tool_result.metadata:
                        st.markdown("**Metadata:**")
                        st.json(tool_result.metadata)
    
    # Display final message
    if response.final_message:
        if response.final_message.content:
            # Process content to find YouTube links and display them nicely
            content = response.final_message.content
            st.markdown(content)
            
            # Extract and embed YouTube videos
            import re
            yt_patterns = [
                r'https?://(?:www\.)?youtube\.com/watch\?v=([a-zA-Z0-9_-]{11})',
                r'https?://youtu\.be/([a-zA-Z0-9_-]{11})',
                r'\(YouTube: ([a-zA-Z0-9_-]{11})\)'
            ]
            
            found_yt_ids = []
            for pattern in yt_patterns:
                matches = re.findall(pattern, content)
                for match in matches:
                    if match not in found_yt_ids:
                        found_yt_ids.append(match)
            
            if found_yt_ids:
                st.markdown("---")
                st.markdown("### 🎥 Watch Review Videos")
                cols = st.columns(len(found_yt_ids[:2])) # Limit to 2 videos
                for i, yt_id in enumerate(found_yt_ids[:2]):
                    with cols[i]:
                        st.video(f"https://www.youtube.com/watch?v={yt_id}")


def render_conversation_history():
    """Render all previous messages in the conversation."""
    for msg in st.session_state.conversation_display:
        if msg["role"] == "user":
            with st.chat_message("user"):
                st.markdown(msg["content"])
        else:
            with st.chat_message("assistant"):
                render_ai_response(msg["response"], msg.get("intent"))



@observe(name="process_user_input")
def process_user_input(user_input: str):
    """
    Process user input by classifying intent, running the bot, and storing responses.
    
    Args:
        user_input: The user's message
    """
    response_container = st.empty()
    
    try:
        # Classify intent once before processing
        current_intent = None
        try:
            # Create a temporary message list for intent classification
            temp_messages = st.session_state.messages + [UserMessage(content=user_input)]
            current_intent = classify_intent(temp_messages, st.session_state.llm_config)
            
            # Update session state for sidebar
            st.session_state.intent_info = {
                "intent": current_intent.intent_name.value,
                "confidence": current_intent.confidence,
                "tools": []  # Will be populated by the bot
            }
        except Exception as e:
            st.warning(f"Intent classification failed: {e}")
        
        # Stream response from bot, passing the pre-classified intent
        final_response = None
        for response in run_mahindra_bot(
            user_input,
            st.session_state.messages,
            st.session_state.agent_toolkit,
            st.session_state.llm_config,
            current_intent  # Pass pre-classified intent
        ):
            with response_container.container():
                render_ai_response(response, current_intent)
            final_response = response
        
        # Store final response in display history with intent
        if final_response:
            st.session_state.conversation_display.append({
                "role": "assistant",
                "response": final_response,
                "intent": current_intent
            })
    
    except Exception as e:
        st.error(f"❌ Error processing request: {str(e)}")
        st.exception(e)


# ============================================================================
# Sidebar Components
# ============================================================================

def render_sidebar():
    """Render sidebar with configuration and controls."""
    with st.sidebar:
        st.title("🚗 Mahindra Bot")
        st.caption("Your AI Assistant for Cars, Insurance & Bookings")
        
        # Show DEBUG mode indicator if enabled
        debug_mode = os.getenv("DEBUG", "").lower() in ("true", "1", "yes")
        if debug_mode:
            st.warning("🔧 DEBUG MODE - Login Disabled")
        
        # Logout button (only show if not in debug mode)
        if not debug_mode:
            if st.button("🚪 Logout", use_container_width=True, type="secondary"):
                st.session_state.authenticated = False
                st.session_state.messages = []
                st.session_state.conversation_display = []
                st.session_state.intent_info = None
                st.rerun()
        
        # Reset chat button
        if st.button("🔄 Reset Chat", use_container_width=True):
            st.session_state.messages = []
            st.session_state.conversation_display = []
            st.session_state.intent_info = None
            st.rerun()
        
        # Display current configuration
        st.markdown("---")
        st.subheader("⚙️ Configuration")
        if "llm_config" in st.session_state:
            st.text(f"Model: {st.session_state.llm_config.model_id}")
            if hasattr(st.session_state.llm_config, "model_args"):
                st.text(f"Temperature: {st.session_state.llm_config.model_args.temperature}")
        
        # Display available intents
        st.markdown("---")
        st.subheader("🎯 Available Intents")
        st.markdown("""
        - 👋 **Greeting**  
          Say hello and get started
        
        - 🤔 **General Q&A**  
          Insurance and documentation questions
        
        - 🚗 **Car Recommendation**  
          Finding the right car for your needs
        
        - ⚖️ **Car Comparison**  
          Comparing multiple cars
        
        - 📅 **Book Test Drive**  
          Schedule a test drive
        
        - 🔌 **EV Charger Location**  
          Find nearby charging stations
        """)
        
        # Display service status
        st.markdown("---")
        st.subheader("📊 Service Status")
        if "services_initialized" in st.session_state and st.session_state.services_initialized:
            st.success("✅ Car Service Ready")
            st.success("✅ FAQ Service Ready")
            if "agent_toolkit" in st.session_state:
                num_tools = len(st.session_state.agent_toolkit.get_tools())
                st.info(f"🔧 {num_tools} tools available")
        else:
            st.error("❌ Services not initialized")
        
        # Display intent information if available
        if "intent_info" in st.session_state and st.session_state.intent_info:
            st.markdown("---")
            st.subheader("🎯 Current Intent")
            intent_info = st.session_state.intent_info
            
            # Intent name with emoji
            intent_emoji = {
                "greeting": "👋",
                "general_qna": "🤔",
                "car_recommendation": "🚗",
                "car_comparison": "⚖️",
                "book_ride": "📅",
                "find_ev_charger_location": "🔌"
            }
            emoji = intent_emoji.get(intent_info.get("intent", ""), "❓")
            st.markdown(f"**{emoji} {intent_info.get('intent', 'Unknown').replace('_', ' ').title()}**")
            
            # Confidence bar
            confidence = intent_info.get("confidence", 0)
            st.progress(confidence)
            st.caption(f"Confidence: {confidence:.0%}")
            
            # Active tools
            if intent_info.get("tools"):
                with st.expander("🔧 Active Tools"):
                    for tool in intent_info["tools"]:
                        st.text(f"• {tool}")
        
        # Footer
        st.markdown("---")
        st.caption("Powered by OpenAI & Mahindra Bot")


# ============================================================================
# Main Application
# ============================================================================

def main():
    """Main application entry point."""
    
    # Check authentication first
    if not check_login():
        show_login_page()
        return
    
    # Page configuration
    st.set_page_config(
        page_title="Mahindra Bot",
        page_icon="🚗",
        # layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Custom CSS for better appearance
    st.markdown("""
    <style>
    .stChatMessage {
        padding: 1rem;
        border-radius: 0.5rem;
    }
    .stExpander {
        border: 1px solid #e0e0e0;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .stChatMessage img {
        max-height: 200px;
        width: auto;
        object-fit: contain;
        border-radius: 0.5rem;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Check prerequisites
    prereqs_ok, errors = check_prerequisites()
    
    if not prereqs_ok:
        st.error("⚠️ Setup Required")
        st.markdown("### Missing Prerequisites")
        for error in errors:
            st.markdown(error)
        
        st.markdown("---")
        st.markdown("### Setup Instructions")
        st.markdown("""
        1. **Set OpenAI API Key**
           ```bash
           export OPENAI_API_KEY='your-api-key-here'
           ```
           Or add it to your `.env` file:
           ```
           OPENAI_API_KEY=your-api-key-here
           ```
        
        2. **Ensure Data Files Exist**
           - Car data: `data/new_car_details/` (with JSON files)
           - FAQ data: `data/consolidated_faqs.json`
        
        3. **Restart the Application**
           ```bash
           streamlit run streamlit_apps/mahindra_bot_app.py
           ```
        """)
        return
    
    # Initialize session state
    if "messages" not in st.session_state:
        st.session_state.messages = []
    
    if "conversation_display" not in st.session_state:
        st.session_state.conversation_display = []
    
    if "intent_info" not in st.session_state:
        st.session_state.intent_info = None
    
    if "services_initialized" not in st.session_state:
        # Initialize services
        with st.spinner("Initializing services..."):
            car_service, faq_service, ev_charger_service, error = initialize_services()
            
            if error:
                st.error(f"Failed to initialize services: {error}")
                st.session_state.services_initialized = False
                return
            
            # Create toolkit
            st.session_state.agent_toolkit = AgentToolKit(
                car_service=car_service,
                faq_service=faq_service,
                ev_charger_service=ev_charger_service
            )
            
            # Configure LLM
            st.session_state.llm_config = LLMConfig(
                model_id="gpt-5.2",
                model_args=ModelArgs(temperature=0, max_tokens=5000)
            )
            
            st.session_state.services_initialized = True
    
    # Render sidebar
    render_sidebar()
    
    # Main chat interface
    st.title("💬 Mahindra Bot")
    st.caption("Ask me anything about cars, insurance, or book a test drive!")
    
    # Render conversation history
    render_conversation_history()
    
    # Chat input
    if user_input := st.chat_input("Ask me anything about cars, insurance, or bookings..."):
        # Display user message
        with st.chat_message("user"):
            st.markdown(user_input)
        
        # Add to display history
        st.session_state.conversation_display.append({
            "role": "user",
            "content": user_input
        })
        
        # Stream AI response
        with st.spinner("🤔 Thinking..."), st.chat_message("assistant"):
            process_user_input(user_input)
    
    # Display helpful hints if conversation is empty
    if len(st.session_state.conversation_display) == 0:
        st.markdown("---")
        st.markdown("### 💡 Try saying...")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**👋 Getting Started**")
            st.markdown("- Hello!")
            st.markdown("- Hi, what can you do?")
            st.markdown("")
            st.markdown("**🤔 General Questions**")
            st.markdown("- What documents do I need for RC transfer?")
            st.markdown("- How does car insurance work?")
        
        with col2:
            st.markdown("**🚗 Car Queries**")
            st.markdown("- I want a car under 15 lakhs")
            st.markdown("- Compare Mahindra Thar and Scorpio")
            st.markdown("- Book a test drive for XUV700")
            st.markdown("- Show me SUVs with good mileage")
            st.markdown("")
            st.markdown("**🔌 EV Charging**")
            st.markdown("- Find EV charging station near 110092")
            st.markdown("- Where can I charge my EV in Delhi?")


if __name__ == "__main__":
    main()
