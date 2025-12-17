"""Intent classification for user messages."""

from langfuse import observe

from mahindrabot.services.llm_service import (
    LLMConfig,
    SystemMessage,
    UserMessage,
    get_llm_structured_response,
)
from mahindrabot.services.llm_service.messages import MessageType

from .models import Intent, IntentType

# Intent classification prompt focusing on last message only
INTENT_CLASSIFICATION_PROMPT = """You are an intent classifier for Mahindra Bot, an assistant for car buying and insurance.

Analyze the user's LAST message and classify their intent:

1. greeting - Initial greetings, hello, introductions, or casual conversation starters
   Examples: "Hi", "Hello", "Hey there", "Good morning", "How are you?", "What can you do?"

2. general_qna - Questions about insurance, processes, documentation, general information
   Examples: "What documents are needed?", "How does insurance work?", "What is RC transfer?"

3. car_recommendation - Finding the right car based on preferences and budget
   Examples: "I want a car under 15 lakhs", "Show me SUVs", "What cars have good mileage?"

4. car_comparison - Comparing specific cars or asking about differences
   Examples: "Compare Thar and Scorpio", "Which is better?", "Difference between X and Y"

5. book_ride - Booking a test drive or trial ride
   Examples: "I want to test drive", "Book a ride", "Schedule a test drive"

6. find_ev_charger_location - Finding nearby EV charging stations
   Examples: "Where can I charge my EV?", "EV charging station near 110092", "Find EV charger near me", "Charging station in Delhi"

7. bike_recommendation - Finding the right bike/scooter based on preferences and budget
   Examples: "I want a bike under 1 lakh", "Show me scooters", "Best mileage bike", "Royal Enfield price"

8. bike_comparison - Comparing specific bikes/scooters
   Examples: "Compare Activa and Jupiter", "Classic 350 vs Meteor", "Which scooter is better?"

Focus ONLY on the user's most recent message. CRITICAL RULES:
- If the message contains "bike", "scooter", "motorcycle", "two-wheeler", or bike brands (Royal Enfield, Mojo, Jawa), it MUST be classified as `bike_recommendation` or `bike_comparison`. NEVER classify these as car intents.
- Simple greetings should be classified as "greeting"
- Explicit keywords (e.g., "compare" → car_comparison OR bike_comparison based on vehicle type)
- Stated requirements: "under 15 lakhs" → usually car, but "under 2 lakhs" or "under 1 lakh" → check for bike keywords carefully.
- "Test drive" → book_ride

Provide your classification with confidence (0.0-1.0)."""

@observe(name="classify_intent")
def classify_intent(messages: list[MessageType], llm_config: LLMConfig) -> Intent:
    """
    Classify user intent based on the last message only.
    
    Uses LLM structured output to classify the intent into one of six types:
    - greeting: Initial greetings and casual conversation starters
    - general_qna: Insurance and documentation questions
    - car_recommendation: Finding the right car
    - car_comparison: Comparing multiple cars
    - book_ride: Booking a test drive
    - find_ev_charger_location: Finding nearby EV charging stations
    - bike_recommendation: Finding the right bike/scooter
    - bike_comparison: Comparing multiple bikes/scooters
    
    Args:
        messages: List of conversation messages
        llm_config: LLM configuration for classification
        
    Returns:
        Intent object with intent_name and confidence
    """
    # Extract only the last user message
    user_messages = [m for m in messages if isinstance(m, UserMessage)]
    
    if not user_messages:
        # Default to general_qna if no user messages found
        return Intent(
            intent_name=IntentType.GENERAL_QNA,
            confidence=0.5
        )
    
    last_user_message = user_messages[-1]
    
    # Build classification messages
    classification_messages = [
        SystemMessage(content=INTENT_CLASSIFICATION_PROMPT),
        UserMessage(content=f"User's last message: {last_user_message.content}")
    ]
    
    # Get structured response from LLM
    try:
        intent = get_llm_structured_response(
            llm_config=llm_config,
            messages=classification_messages,
            response_model=Intent
        )
        return intent
    except Exception as e:
        # Fallback to general_qna on error
        print(f"Error classifying intent: {e}")
        return Intent(
            intent_name=IntentType.GENERAL_QNA,
            confidence=0.3
        )
