"""Deploy RAG Search Agent to Vertex AI Agent Engine."""

import vertexai
from vertexai import agent_engines

from VertexRAGSearchAgent.agent import create_agent

PROJECT_ID = "gcp-poc-488614"
LOCATION = "us-central1"

vertexai.init(
    project=PROJECT_ID,
    location=LOCATION,
    staging_bucket="gs://gcp-poc-488614-adk-staging",
)

# Create the ADK agent
agent = create_agent(model="gemini-2.0-flash")

print("Deploying to Agent Engine...")
remote_agent = agent_engines.create(
    agent_engine=agent,
    requirements=[
        "google-cloud-aiplatform[agent_engines,adk]",
        "google-adk>=1.0.0",
        "google-cloud-spanner",
        "pydantic>=2.7,<3.0",
    ],
    extra_packages=[
        "VertexRAGSearchAgent/",
    ],
    display_name="RAG Search Agent",
    description="Expert discovery pipeline with intent-based routing, "
    "graph search, and result synthesis.",
    gcs_dir_name="rag-search-agent",
)

print(f"\nDeployed successfully!")
print(f"Resource name: {remote_agent.resource_name}")

# Extract the resource ID for console URL
resource_id = remote_agent.resource_name.split("/")[-1]
console_url = (
    f"https://console.cloud.google.com/ai/agent-engine/"
    f"{resource_id}/chat?project={PROJECT_ID}"
)
print(f"Console URL: {console_url}")
print(f"\nTo delete: remote_agent.delete(force=True)")
