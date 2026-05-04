#!/usr/bin/env python3
"""RTI Demo Orchestrator — Deploys all Real-Time Intelligence components.

Usage:
    python deploy.py                    # Full deployment
    python deploy.py --skip-infra       # Skip Azure infrastructure
    python deploy.py --only eventhouse  # Deploy single agent
    python deploy.py --validate         # Run validation only
    python deploy.py --cleanup          # Tear down Azure resources
"""

import argparse
import json
import logging
import os
import sys
import time

# Project root
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_DIR)

from agents.fabric_client import FabricClient
from agents.infra_agent import InfraAgent
from agents.eventhouse_agent import EventhouseAgent
from agents.eventstream_agent import EventstreamAgent
from agents.queryset_agent import QuerysetAgent
from agents.dashboard_agent import DashboardAgent
from agents.activator_agent import ActivatorAgent
from agents.ai_agents_agent import AIAgentsAgent
from agents.validator_agent import ValidatorAgent
from agents.simulator_agent import SimulatorAgent

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("orchestrator")

AGENT_ORDER = [
    "infra",
    "eventhouse",
    "eventstream",
    "queryset",
    "dashboard",
    "activator",
    "ai_agents",
    "validator",
    "simulator",
]


def load_config(config_path: str = None) -> dict:
    """Load configuration from config.json."""
    path = config_path or os.path.join(PROJECT_DIR, "config.json")
    with open(path, "r", encoding="utf-8") as f:
        config = json.load(f)

    # Validate required fields
    if not config["fabric"].get("workspace_id"):
        logger.warning(
            "fabric.workspace_id is empty in config.json. "
            "Set it before running Fabric agents."
        )

    return config


def run_agent(name: str, config: dict, client: FabricClient, results: dict) -> dict:
    """Run a single agent and return its result."""
    logger.info("━" * 60)
    logger.info("Running @%s agent...", name)
    logger.info("━" * 60)
    start = time.time()

    try:
        if name == "infra":
            agent = InfraAgent(config)
            result = agent.deploy()

        elif name == "eventhouse":
            agent = EventhouseAgent(config, client, PROJECT_DIR)
            result = agent.deploy()

        elif name == "eventstream":
            agent = EventstreamAgent(config, client)
            result = agent.deploy()

        elif name == "queryset":
            agent = QuerysetAgent(config, client, PROJECT_DIR)
            result = agent.deploy()

        elif name == "dashboard":
            db_id = results.get("eventhouse", {}).get("database_id", "")
            agent = DashboardAgent(config, client, db_id)
            result = agent.deploy()

        elif name == "activator":
            agent = ActivatorAgent(config, client)
            result = agent.deploy()

        elif name == "ai_agents":
            agent = AIAgentsAgent(config, client)
            result = agent.deploy()

        elif name == "validator":
            agent = ValidatorAgent(config, client)
            result = agent.validate()

        elif name == "simulator":
            agent = SimulatorAgent(config, PROJECT_DIR)
            result = agent.deploy()

        else:
            logger.error("Unknown agent: %s", name)
            return {"status": "error", "message": f"Unknown agent: {name}"}

        elapsed = time.time() - start
        logger.info("@%s completed in %.1fs", name, elapsed)
        return result

    except Exception as e:
        elapsed = time.time() - start
        logger.error("@%s FAILED after %.1fs: %s", name, elapsed, e)
        return {"status": "error", "message": str(e)}


def deploy(config: dict, skip_infra: bool = False, only: str = None,
           skip_simulator: bool = False):
    """Run the full deployment pipeline."""
    logger.info("=" * 60)
    logger.info("RTI Demo Deployment — %s", config["project"])
    logger.info("=" * 60)

    workspace_id = config["fabric"].get("workspace_id", "")
    client = None
    if workspace_id:
        client = FabricClient(workspace_id)
        logger.info("Fabric workspace: %s", workspace_id)
    else:
        logger.warning("No workspace_id configured — Fabric agents will be skipped")

    results = {}
    agents_to_run = AGENT_ORDER.copy()

    # Filter agents
    if only:
        agents_to_run = [only]
    else:
        if skip_infra:
            agents_to_run.remove("infra")
        if skip_simulator:
            agents_to_run = [a for a in agents_to_run if a != "simulator"]

    # Check which agents need Fabric client
    fabric_agents = {"eventhouse", "eventstream", "queryset", "dashboard", "activator", "ai_agents", "validator"}

    for agent_name in agents_to_run:
        if agent_name in fabric_agents and not client:
            logger.warning("Skipping @%s — no workspace_id configured", agent_name)
            results[agent_name] = {"status": "skipped", "reason": "no workspace_id"}
            continue

        result = run_agent(agent_name, config, client, results)
        results[agent_name] = result

        # Stop on critical failures (infra or eventhouse)
        if result.get("status") == "error" and agent_name in ("infra", "eventhouse"):
            logger.error("Critical agent @%s failed — aborting pipeline", agent_name)
            break

    # Print summary
    print_summary(results)
    return results


def print_summary(results: dict):
    """Print deployment summary."""
    logger.info("")
    logger.info("=" * 60)
    logger.info("DEPLOYMENT SUMMARY")
    logger.info("=" * 60)

    for agent_name, result in results.items():
        status = result.get("status", "completed")
        if status == "error":
            icon = "✗"
        elif status == "skipped":
            icon = "⊘"
        else:
            icon = "✓"
        logger.info("  %s @%-12s %s", icon, agent_name, status)

    # Manual steps
    logger.info("")
    logger.info("POST-DEPLOYMENT STEPS:")
    logger.info("  1. Wire Eventstream sources → Event Hub in Fabric UI")
    logger.info("  2. Wire Eventstream destinations → KQL Database in Fabric UI")
    logger.info("  3. Connect Dashboard data source to KQL Database")
    logger.info("  4. Configure Activator trigger rules in Fabric UI")
    logger.info("  5. Run medallion-architecture.kql after data flows")
    logger.info("  6. Open phone-telemetry.html on your phone")
    logger.info("  7. Connect Data Agent + Ops Agent to KQL Database in Fabric UI")
    logger.info("  8. Configure Anomaly Detector data source in Fabric UI")
    logger.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="RTI Demo Deployment Orchestrator")
    parser.add_argument("--config", default=None, help="Path to config.json")
    parser.add_argument("--skip-infra", action="store_true",
                        help="Skip Azure infrastructure deployment")
    parser.add_argument("--skip-simulator", action="store_true",
                        help="Skip starting the phone simulator")
    parser.add_argument("--only", choices=AGENT_ORDER,
                        help="Run only a specific agent")
    parser.add_argument("--validate", action="store_true",
                        help="Run validation only")
    parser.add_argument("--cleanup", action="store_true",
                        help="Clean up Azure resources")
    parser.add_argument("--export-dashboard", action="store_true",
                        help="Export dashboard definition to JSON file")
    args = parser.parse_args()

    config = load_config(args.config)

    if args.cleanup:
        agent = InfraAgent(config)
        agent.cleanup()
        return

    if args.validate:
        client = FabricClient(config["fabric"]["workspace_id"])
        agent = ValidatorAgent(config, client)
        result = agent.validate()
        sys.exit(0 if result["summary"]["status"] == "PASS" else 1)

    if args.export_dashboard:
        agent = DashboardAgent(config, None)
        path = agent.export_definition()
        print(f"Dashboard definition exported to: {path}")
        return

    deploy(
        config,
        skip_infra=args.skip_infra,
        only=args.only,
        skip_simulator=args.skip_simulator,
    )


if __name__ == "__main__":
    main()
