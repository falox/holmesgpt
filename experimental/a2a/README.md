# A2A Protocol Adapter for HolmesGPT

This module implements Google's [Agent-to-Agent (A2A) protocol](https://google.github.io/A2A/) for HolmesGPT, enabling interoperability with other A2A-compatible agents and orchestrators.

## What is A2A?

A2A is an open protocol by Google that standardizes how AI agents communicate with each other. It defines:

- **Agent Cards**: JSON metadata describing an agent's capabilities, skills, and endpoints
- **Tasks**: Units of work that agents can execute
- **Streaming**: Real-time updates on task progress and results

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      A2A Client                             │
│              (orchestrator or another agent)                │
└───────────────────────────┬─────────────────────────────────┘
                            │ HTTP/JSON-RPC
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                   A2A Server (Starlette)                    │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  /.well-known/agent.json  →  Agent Card                │ │
│  │  /                        →  Task Handler              │ │
│  └────────────────────────────────────────────────────────┘ │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                  HolmesAgentExecutor                        │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  1. Extract user message from A2A request              │ │
│  │  2. Build chat messages via build_chat_messages()      │ │
│  │  3. Stream response from ToolCallingLLM                │ │
│  │  4. Convert chunks to A2A TaskArtifactUpdateEvents     │ │
│  └────────────────────────────────────────────────────────┘ │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    HolmesGPT Core                           │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │ToolCallingLLM│  │  Toolsets   │  │ Kubernetes/Prometheus│ │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Components

### `agent_card.py`

Defines the HolmesGPT agent card with:

- **Name/Description**: Identifies HolmesGPT as an infrastructure troubleshooting agent
- **Capabilities**: Streaming enabled, push notifications disabled
- **Skills**:
  - `investigate`: Analyze alerts, logs, metrics to diagnose problems
  - `ask`: Answer questions about infrastructure and observability

### `executor.py`

`HolmesAgentExecutor` bridges the A2A protocol to HolmesGPT:

1. Receives A2A task requests with user messages
2. Updates task status to "working"
3. Creates a `ToolCallingLLM` instance
4. Streams the response, converting chunks to A2A artifacts
5. Marks task as completed/failed

### `__main__.py`

Entry point that:

1. Loads HolmesGPT configuration from environment
2. Creates the A2A Starlette server
3. Serves the agent card at `/.well-known/agent.json`
4. Handles incoming A2A requests

## Running the Server

```bash
# From the repository root
python -m experimental.a2a --host 0.0.0.0 --port 9999
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `MODEL` | LLM model to use (default: gpt-4.1) |
| `LOG_LEVEL` | Logging level (default: INFO) |
| `CERTIFICATE` | Custom SSL certificate path |
| Standard HolmesGPT env vars | `OPENAI_API_KEY`, etc. |

## Example Usage

Once running, the agent card is available at:

```
http://localhost:9999/.well-known/agent.json
```

A2A clients can discover and interact with HolmesGPT by:

1. Fetching the agent card to discover capabilities
2. Sending task requests with questions like "Why is my pod crashing?"
3. Receiving streamed responses with investigation results

## Status

**Experimental** - This is an early implementation for testing A2A interoperability. The API may change.
