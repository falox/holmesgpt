import logging
import uuid
from typing import Optional

from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events import EventQueue
from a2a.types import (
    Artifact,
    TaskArtifactUpdateEvent,
    TaskState,
    TaskStatus,
    TaskStatusUpdateEvent,
    TextPart,
)

from holmes.config import Config
from holmes.core.conversations import build_chat_messages
from holmes.core.supabase_dal import SupabaseDal
from holmes.utils.stream import StreamEvents


class HolmesAgentExecutor(AgentExecutor):
    """Bridges A2A protocol to HolmesGPT ToolCallingLLM."""

    def __init__(self, config: Config, dal: Optional[SupabaseDal] = None):
        self.config = config
        self.dal = dal

    async def execute(
        self,
        context: RequestContext,
        event_queue: EventQueue,
    ) -> None:
        """Execute a HolmesGPT investigation based on A2A request."""
        try:
            # Extract user message from A2A request
            user_message = self._extract_text_from_request(context)
            if not user_message:
                await self._send_error(context, event_queue, "No message content provided")
                return

            logging.info(f"A2A request received: {user_message[:100]}...")

            # Update status to working
            await event_queue.enqueue_event(
                TaskStatusUpdateEvent(
                    taskId=context.task_id,
                    contextId=context.context_id,
                    status=TaskStatus(state=TaskState.working),
                    final=False,
                )
            )

            # Create HolmesGPT LLM instance with console toolsets (enables all toolsets)
            ai = self.config.create_console_toolcalling_llm(dal=self.dal)

            # Build messages using existing HolmesGPT conversation builder
            messages = build_chat_messages(
                ask=user_message,
                conversation_history=None,  # Fresh conversation for each A2A task
                ai=ai,
                config=self.config,
            )

            # Stream HolmesGPT response
            accumulated_response = []
            artifact_id = str(uuid.uuid4())
            artifact_created = False

            for chunk in ai.call_stream(msgs=messages, enable_tool_approval=False):
                event_type = getattr(chunk, "event", None)
                chunk_data = getattr(chunk, "data", {})

                if event_type in (StreamEvents.AI_MESSAGE, StreamEvents.ANSWER_END):
                    content = chunk_data.get("content", "")
                    if content:
                        accumulated_response.append(content)
                        # First chunk creates the artifact (append=False)
                        # Subsequent chunks append to it (append=True)
                        await event_queue.enqueue_event(
                            TaskArtifactUpdateEvent(
                                taskId=context.task_id,
                                contextId=context.context_id,
                                artifact=Artifact(
                                    artifactId=artifact_id,
                                    name="response",
                                    parts=[TextPart(text=content)],
                                ),
                                append=artifact_created,
                                lastChunk=False,
                            )
                        )
                        artifact_created = True

                elif event_type == StreamEvents.TOOL_RESULT:
                    # Log tool usage (could be included in metadata if needed)
                    tool_name = chunk_data.get("tool_name", "unknown")
                    logging.debug(f"Tool called: {tool_name}")

                elif event_type == StreamEvents.ERROR:
                    error_msg = chunk_data.get("message", "Unknown error occurred")
                    await self._send_error(context, event_queue, error_msg)
                    return

            # Send final artifact chunk if we created an artifact
            if artifact_created:
                await event_queue.enqueue_event(
                    TaskArtifactUpdateEvent(
                        taskId=context.task_id,
                        contextId=context.context_id,
                        artifact=Artifact(
                            artifactId=artifact_id,
                            name="response",
                            parts=[TextPart(text="")],  # Empty final chunk
                        ),
                        append=True,
                        lastChunk=True,
                    )
                )

            # Mark task as completed
            await event_queue.enqueue_event(
                TaskStatusUpdateEvent(
                    taskId=context.task_id,
                    contextId=context.context_id,
                    status=TaskStatus(state=TaskState.completed),
                    final=True,
                )
            )

        except Exception as e:
            logging.error(f"Error in HolmesAgentExecutor: {e}", exc_info=True)
            await self._send_error(context, event_queue, str(e))

    async def cancel(
        self,
        context: RequestContext,
        event_queue: EventQueue,
    ) -> None:
        """Handle task cancellation request."""
        logging.info(f"Cancellation requested for task {context.task_id}")
        await event_queue.enqueue_event(
            TaskStatusUpdateEvent(
                taskId=context.task_id,
                contextId=context.context_id,
                status=TaskStatus(state=TaskState.canceled),
                final=True,
            )
        )

    def _extract_text_from_request(self, context: RequestContext) -> str:
        """Extract text content from A2A request message."""
        message = context.message
        if not message:
            logging.warning("No message in context")
            return ""

        # Get parts from the message
        parts = getattr(message, "parts", [])

        text_parts = []
        for part in parts:
            # A2A SDK wraps parts in a Part object with a 'root' attribute
            # that contains the actual TextPart/FilePart/DataPart
            actual_part = getattr(part, "root", part)

            # Handle TextPart objects
            if hasattr(actual_part, "text"):
                text_parts.append(actual_part.text)
            # Handle dict-like parts
            elif isinstance(actual_part, dict) and "text" in actual_part:
                text_parts.append(actual_part["text"])

        return " ".join(text_parts).strip()

    async def _send_error(
        self,
        context: RequestContext,
        event_queue: EventQueue,
        error_message: str,
    ) -> None:
        """Send error status update."""
        logging.error(f"A2A task failed: {error_message}")
        await event_queue.enqueue_event(
            TaskStatusUpdateEvent(
                taskId=context.task_id,
                contextId=context.context_id,
                status=TaskStatus(state=TaskState.failed),
                final=True,
                metadata={"error": error_message},
            )
        )
