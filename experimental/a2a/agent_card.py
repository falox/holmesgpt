from a2a.types import AgentCard, AgentCapabilities, AgentSkill

HOLMES_AGENT_CARD = AgentCard(
    name="HolmesGPT",
    description="AI-powered infrastructure troubleshooting agent. Investigates Kubernetes, Prometheus, Grafana, and other observability platforms to diagnose and analyze issues.",
    url="http://localhost:9999/",
    version="1.0.0",
    capabilities=AgentCapabilities(
        streaming=True,
        pushNotifications=False,
    ),
    defaultInputModes=["text/plain"],
    defaultOutputModes=["text/plain"],
    skills=[
        AgentSkill(
            id="investigate",
            name="Investigate Infrastructure Issues",
            description="Analyze alerts, logs, metrics to diagnose problems in Kubernetes, Prometheus, Grafana, and other observability platforms.",
            tags=["kubernetes", "prometheus", "grafana", "observability", "troubleshooting"],
            examples=[
                "Why is my pod crashing?",
                "Investigate high CPU usage alert",
                "What's causing the latency spike?",
                "Debug OOMKilled pods in production",
            ],
        ),
        AgentSkill(
            id="ask",
            name="Ask Questions",
            description="Answer questions about infrastructure and observability. Query Kubernetes resources, Prometheus metrics, and more.",
            tags=["kubernetes", "prometheus", "queries", "observability"],
            examples=[
                "What pods are running in default namespace?",
                "Show me the Prometheus alerts",
                "List all deployments with less than 2 replicas",
                "What's the memory usage of my services?",
            ],
        ),
    ],
)
