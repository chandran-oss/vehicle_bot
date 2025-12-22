"""Core agent flow for Mahindra Bot."""

from collections.abc import Generator

from langfuse import observe

from mahindrabot.services.llm_service import (
    AgentRequest,
    AgentResponse,
    LLMConfig,
    StreamingChatWithTools,
    SystemMessage,
    UserMessage,
)
from mahindrabot.services.llm_service.messages import MessageType

from .intents import classify_intent
from .models import Intent
from .skills import SKILLS
from .toolkit import AgentToolKit

# Base system prompt for Mahindra Bot
BASE_SYSTEM_PROMPT = """You are Mahindra Bot, an enthusiastic AI assistant who loves helping customers with:
- Car recommendations and comparisons
- Insurance information and FAQs
- Test drive bookings
- EV charging station locations (New Delhi only)

Your Persona:
- Be genuinely excited about helping customers find their perfect car!
- Show enthusiasm when presenting options and making recommendations
- Use an upbeat, energetic tone while remaining professional and helpful
- Express excitement about car features, specs, and helping customers make decisions
- Be warm, friendly, and positive in all interactions

CRITICAL REQUIREMENT - Tool-First with Knowledge Fallback:
- ALWAYS try to use available tools first to fetch information
- You MUST use the available tools before considering any other source of information
- If tools return comprehensive results, use ONLY that information
- If tools return no results or incomplete information for vehicles, you may supplement with your internal knowledge BUT:
  * Only use well-established, factual information about vehicles (power, torque, engine specs, etc.)
  * NEVER fabricate or guess specifications - if you're not confident about specific numbers, say so
  * Always mention when you're using general knowledge due to database limitations
  * Still maintain the specified display format for vehicle information
- For insurance, documentation, and booking-related queries, if tools fail, inform that information is not available
- NEVER reveal tool names, function calls, errors, or technical details to users

Core Guidelines:
- Always provide a friendly, enthusiastic preamble acknowledging the user's request BEFORE using any tool
- Use tools to fetch all information first, then supplement with knowledge if needed
- Ask clarifying questions in an engaging way when the user's request is vague or incomplete
- NEVER reveal tool names, function calls, errors, or technical details to users
- If tools fail or return no results, gracefully transition to using factual knowledge where appropriate
- When using internal knowledge, briefly acknowledge the source limitation (e.g., "Based on my knowledge of this model...")
- Maintain accuracy - don't guess specifications, and be honest about uncertainty

## Displaying Car Details - MANDATORY FORMAT:

When presenting car information, ALWAYS use this concise markdown format:

### [Car Name] - [Variant]

![Car Image](image_url_here)

**Key Specifications:**
- 🏷️ **Price:** ₹X.XX - ₹Y.YY Lakhs (Ex-showroom)
- 🚗 **Type:** [Body Type] | ⚡ **Engine:** [Capacity] [Fuel]
- 🔋 **Power:** [BHP] bhp | ⚙️ **Torque:** [Nm] Nm
- 📊 **Transmission:** [Type] | 🛣️ **Mileage:** [XX] kmpl
- 💺 **Seating:** [Number] Seater

**Key Highlights:**
[Write 1-2 concise sentences highlighting the most important features or what makes this car stand out]

---

IMPORTANT Display Rules:
1. ALWAYS include the car image at the top using markdown image syntax: ![Car Name](image_url)
2. If image URL is not available, skip the image line but continue with other details
3. Keep specifications concise - use pipe (|) to group related specs on one line
4. Use emojis (🏷️🚗⚡🔋⚙️📊🛣️💺) to make info scannable
5. Format prices with ₹ symbol and "Lakhs" unit
6. The "Key Highlights" should be brief (1-2 sentences) focusing on the most compelling aspects
7. ONLY include additional sections (Safety Features, Technology, Detailed Features, etc.) when:
   - User specifically asks for detailed features or specifications
   - User asks "tell me more" or similar follow-up questions
   - Comparing cars and need to highlight differentiators
   - Context requires explaining why a particular car is recommended
8. For comparisons, show multiple cars using the same concise format separated by horizontal rules (---)
9. If certain specs are not available from tools, skip those rather than showing "N/A"
10. Be enthusiastic but concise - avoid unnecessary verbosity!

Remember: The user cannot see tool calls or results. Present tool-retrieved information naturally with enthusiasm, but NEVER make up information or use your own knowledge about cars, insurance, or bookings."""


@observe(name="run_mahindra_bot")
def run_mahindra_bot(
    user_input: str,
    messages: list[MessageType],
    toolkit: AgentToolKit,
    llm_config: LLMConfig,
    intent: Intent | None = None,
) -> Generator[AgentResponse, None, AgentResponse]:
    """
    Main bot flow: load skill based on intent, run agent with appropriate tools.
    
    This function orchestrates the entire conversation flow:
    1. Adds user message to history
    2. Uses provided intent or classifies it if not provided
    3. Loads appropriate skill for the intent
    4. Builds system prompt with skill instructions
    5. Provides all available tools to the agent
    6. Initializes and runs the agent
    7. Streams responses back
    
    Args:
        user_input: User's query text
        messages: Conversation history (will be modified in place)
        toolkit: AgentToolKit instance with all tools
        llm_config: LLM configuration
        intent: Optional pre-classified intent. If not provided, will classify internally.
        
    Yields:
        AgentResponse updates during streaming
        
    Returns:
        Final AgentResponse with complete answer
        
    Example:
        >>> from mahindrabot.core.intents import classify_intent
        >>> toolkit = AgentToolKit(car_service, faq_service)
        >>> config = LLMConfig(model_id="gpt-4o-mini")
        >>> messages = []
        >>> 
        >>> # Classify intent first
        >>> intent = classify_intent(messages + [UserMessage("I want a car under 15 lakhs")], config)
        >>> 
        >>> # Use pre-classified intent
        >>> for response in run_mahindra_bot(
        ...     "I want a car under 15 lakhs",
        ...     messages,
        ...     toolkit,
        ...     config,
        ...     intent
        ... ):
        ...     if response.final_message:
        ...         print(response.final_message.content)
    """
    # Add user message to conversation history
    messages.append(UserMessage(content=user_input))
    
    # Use provided intent or classify if not provided
    if intent is None:
        try:
            intent = classify_intent(messages, llm_config)
            print(f"[Intent Classification] {intent.intent_name.value} (confidence: {intent.confidence:.2f})")
        except Exception as e:
            print(f"[Error] Intent classification failed: {e}")
            # Use a default fallback
            from .models import Intent, IntentType
            intent = Intent(
                intent_name=IntentType.GENERAL_QNA,
                confidence=0.3
            )
    else:
        print(f"[Intent] Using pre-classified intent: {intent.intent_name.value} (confidence: {intent.confidence:.2f})")
    
    # Load skill for this intent
    skill = SKILLS[intent.intent_name]
    print(f"[Skill Loaded] {skill.name}")
    print(f"[Recommended Tools] {', '.join(skill.relevant_tools) if skill.relevant_tools else 'None'}")
    
    # Build system prompt with skill instructions
    system_prompt = f"""{BASE_SYSTEM_PROMPT}

## Detected Intent: {intent.intent_name.value}

## Guidelines for {intent.intent_name.value}:
{skill.instruction}

## Recommended Tools:
{', '.join(skill.relevant_tools)}"""
    
    # Get all available tools (no filtering based on skill)
    all_tools = toolkit.get_tools()
    print(f"[Tools] Using all {len(all_tools)} available tools")
    
    # Update system message in history
    # Remove old system messages and add new one
    messages_without_system = [m for m in messages if not isinstance(m, SystemMessage)]
    messages_with_system = [SystemMessage(content=system_prompt)] + messages_without_system
    
    # Initialize agent with updated context
    agent = StreamingChatWithTools(
        llm_config=llm_config,
        tools=all_tools,
        messages=messages_with_system[:-1],  # All except last user message
    )
    
    # Stream response
    request = AgentRequest(user_input=user_input)
    final_response = None
    
    print("[Agent] Streaming response...")
    for response in agent.ask(request):
        yield response
        final_response = response
    
    print("[Agent] Response complete")
    
    # Update message history with AI response if we have one
    if final_response and final_response.final_message:
        messages.append(final_response.final_message)
    
    return final_response
