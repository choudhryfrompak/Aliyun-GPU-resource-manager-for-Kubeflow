#!/usr/bin/env python3
import json
import subprocess
import time
from datetime import datetime, timedelta
import re
import logging
import os
from typing import Dict, List, Optional
import pytz  # For timezone handling

# Get local timezone
local_tz = pytz.timezone('Asia/Karachi')  # Pakistan timezone

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('resource_manager.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class K8sResourceManager:
    def __init__(self, config_file: str = 'pod_config.json'):
        self.config_file = config_file
        self.creation_time_cache = {}
        self.cache_expiry = 300
        self.config = self.load_config()

    def get_local_time(self) -> str:
        """Get current time in local timezone"""
        return datetime.now(local_tz).isoformat()

    def format_duration(self, hours: float) -> str:
        """Format duration in hours to human readable string"""
        if hours < 1:
            minutes = int(hours * 60)
            return f"{minutes} minutes"
        elif hours < 24:
            return f"{hours:.1f} hours"
        else:
            days = hours / 24
            return f"{days:.1f} days"

    def save_config(self) -> None:
        """Save current configuration to JSON file"""
        try:
            with open(self.config_file, 'w') as f:
                json.dump(self.config, f, indent=2)
            logger.debug("Configuration saved successfully")
        except Exception as e:
            logger.error(f"Error saving config file: {str(e)}")

    def load_config(self) -> dict:
        """Load configuration from JSON file with error handling"""
        default_config = {
            "excluded_namespaces": ["kube-system", "kubeflow"],
            "default_termination_window": "2h",
            "namespaces": {},
            "pod_timestamps": {}
        }

        try:
            if not os.path.exists(self.config_file):
                with open(self.config_file, 'w') as f:
                    json.dump(default_config, f, indent=2)
                logger.info("Created default config file")
                return default_config

            with open(self.config_file, 'r') as f:
                content = f.read().strip()
                if not content:
                    with open(self.config_file, 'w') as f:
                        json.dump(default_config, f, indent=2)
                    return default_config
                    
                try:
                    config = json.loads(content)
                    if "pod_timestamps" not in config:
                        config["pod_timestamps"] = {}
                    if "namespaces" not in config:
                        config["namespaces"] = {}
                    return config
                except json.JSONDecodeError as e:
                    logger.error(f"Invalid JSON in config file: {str(e)}")
                    logger.info("Using default configuration")
                    return default_config

        except Exception as e:
            logger.error(f"Error handling config file: {str(e)}")
            logger.info("Using default configuration")
            return default_config

    def update_pod_timestamp(self, namespace: str, pod_name: str) -> None:
        """Update pod's last seen running timestamp only if new or previously stopped"""
        if "pod_timestamps" not in self.config:
            self.config["pod_timestamps"] = {}
            
        if namespace not in self.config["pod_timestamps"]:
            self.config["pod_timestamps"][namespace] = {}
            
        current_time = self.get_local_time()
        
        # Update timestamp only if:
        # 1. Pod is not in our records
        # 2. Pod was previously stopped
        pod_info = self.config["pod_timestamps"][namespace].get(pod_name, {})
        if not pod_info or "last_stopped" in pod_info:
            self.config["pod_timestamps"][namespace][pod_name] = {
                "last_seen_running": current_time
            }
            logger.info(f"Updated start time for pod {pod_name} in namespace {namespace}")
            self.save_config()

    def execute_command(self, command: List[str]) -> Optional[str]:
        """Execute command with timeout"""
        try:
            process = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=30
            )

            if process.returncode == 0:
                return process.stdout
            logger.error(f"Command failed: {process.stderr}")
            return None
        except Exception as e:
            logger.error(f"Command error: {e}")
            return None

    def parse_gpushare_output(self) -> List[dict]:
        """Parse kubectl inspect gpushare output"""
        logger.info("Fetching GPU allocations...")

        output = self.execute_command(['kubectl', 'inspect', 'gpushare', '-d'])
        if not output:
            return []

        pods = []
        current_node = None
        reading_pod_section = False

        for line in output.strip().split('\n'):
            line = line.strip()

            if not line or line.startswith('---'):
                continue

            if line.startswith('NAME:'):
                current_node = line.split()[1].strip()
                reading_pod_section = False
                continue

            if 'NAMESPACE' in line and 'GPU0(Allocated)' in line:
                reading_pod_section = True
                continue

            if any(line.startswith(x) for x in ['IPADDRESS:', 'Allocated :', 'Total :', 'Allocated/Total']):
                reading_pod_section = False
                continue

            if reading_pod_section and line:
                parts = line.split()
                if len(parts) >= 2 and not line.startswith(('NAME:', 'IPADDRESS:', 'Allocated :', 'Total :')):
                    pod_name = parts[0]
                    namespace = parts[1]

                    if namespace in self.config.get("excluded_namespaces", []):
                        logger.debug(f"Skipping pod in excluded namespace: {namespace}/{pod_name}")
                        continue

                    # Update timestamp when we see a running pod
                    self.update_pod_timestamp(namespace, pod_name)

                    pod_info = {
                        'name': pod_name,
                        'namespace': namespace,
                        'node': current_node,
                        'full_name': pod_name
                    }
                    pods.append(pod_info)
                    logger.info(f"\nFound pod: {pod_name}")
                    logger.info(f"  Namespace: {namespace}")
                    logger.info(f"  Node: {current_node}")

        found_pods = len(pods)
        logger.info(f"\nTotal pods found: {found_pods}")
        return pods

    def parse_notebook_name(self, name: str) -> str:
        """Parse notebook name by removing numerical suffixes while preserving alphabetical parts."""
        parts = name.split('-')
        result_parts = []
        
        for i, part in enumerate(parts):
            if part.isdigit():
                remaining_parts = parts[i+1:]
                if all(p.isdigit() or not p for p in remaining_parts):
                    break
            result_parts.append(part)
        
        return '-'.join(result_parts)

    def calculate_pod_age(self, start_time_str: str) -> float:
        """Calculate pod age in hours"""
        try:
            start_time = datetime.fromisoformat(start_time_str)
            current_time = datetime.now(local_tz)
            age = current_time - start_time.astimezone(local_tz)
            return age.total_seconds() / 3600
        except Exception as e:
            logger.error(f"Error calculating pod age: {e}")
            return 0

    def should_terminate_pod(self, namespace: str, pod_name: str, termination_window: str) -> tuple[bool, float]:
        """Check if pod should be terminated and return remaining time"""
        try:
            if namespace not in self.config["pod_timestamps"]:
                return False, 0
                
            if pod_name not in self.config["pod_timestamps"][namespace]:
                return False, 0
                
            pod_info = self.config["pod_timestamps"][namespace][pod_name]
            if "last_seen_running" not in pod_info:
                return False, 0

            age_hours = self.calculate_pod_age(pod_info["last_seen_running"])
            
            match = re.match(r'(\d*\.?\d+)([hd])', termination_window)
            if not match:
                return False, 0

            value = float(match.group(1))
            unit = match.group(2)

            limit_hours = value if unit == 'h' else value * 24
            remaining_hours = limit_hours - age_hours

            return age_hours > limit_hours, remaining_hours

        except Exception as e:
            logger.error(f"Error checking termination: {e}")
            return False, 0

    def terminate_pod(self, namespace: str, name: str, full_name: str) -> bool:
        """Terminate a notebook using the annotation method"""
        try:
            base_notebook_name = self.parse_notebook_name(name)
            
            logger.info(f"\nTerminating notebook:")
            logger.info(f"  Original name: {name}")
            logger.info(f"  Base name: {base_notebook_name}")
            logger.info(f"  Namespace: {namespace}")
            
            current_time = self.get_local_time()
            
            if namespace in self.config["pod_timestamps"] and name in self.config["pod_timestamps"][namespace]:
                self.config["pod_timestamps"][namespace][name]["last_stopped"] = current_time
                self.save_config()
            
            result = self.execute_command([
                'kubectl', 'annotate', 'notebook', 
                base_notebook_name,
                f'kubeflow-resource-stopped={current_time}',
                '-n', namespace,
                '--overwrite'
            ])
            
            if result is not None:
                logger.info(f"Successfully stopped notebook {base_notebook_name}")
                return True
            else:
                logger.error(f"Failed to stop notebook {base_notebook_name}")
                return False
                
        except Exception as e:
            logger.error(f"Error terminating notebook: {e}")
            return False

    def process_pods(self, pods: List[dict]) -> None:
        """Process all found pods"""
        for pod in pods:
            namespace = pod['namespace']
            name = pod['name']
            full_name = pod['full_name']

            termination_window = self.config.get("default_termination_window", "2h")
            should_terminate, remaining_hours = self.should_terminate_pod(namespace, name, termination_window)

            # Get pod age
            pod_info = self.config["pod_timestamps"][namespace].get(name, {})
            if "last_seen_running" in pod_info:
                age_hours = self.calculate_pod_age(pod_info["last_seen_running"])
                
                logger.info(f"\nPod Status: {name}")
                logger.info(f"  Namespace: {namespace}")
                logger.info(f"  Age: {self.format_duration(age_hours)}")
                logger.info(f"  Termination Window: {termination_window}")
                
                if remaining_hours > 0:
                    logger.info(f"  Time until termination: {self.format_duration(remaining_hours)}")
                else:
                    logger.info(f"  Exceeded termination window by: {self.format_duration(-remaining_hours)}")

            if should_terminate:
                logger.info(f"\nPod {full_name} exceeded window of {termination_window}")
                if self.terminate_pod(namespace, name, full_name):
                    logger.info(f"Successfully terminated pod {full_name}")
                else:
                    logger.error(f"Failed to terminate pod {full_name}")

    def run(self, interval: int = 3):  # Changed default interval to 3 seconds
        """Main loop"""
        logger.info("Starting Kubernetes Resource Manager")
        logger.info(f"Excluded namespaces: {', '.join(self.config.get('excluded_namespaces', []))}")

        while True:
            try:
                logger.info("\nStarting new check cycle...")

                # Reload config at start of each cycle
                self.config = self.load_config()

                pods = self.parse_gpushare_output()
                if pods:
                    logger.info("Processing pods...")
                    self.process_pods(pods)
                else:
                    logger.info("No pods found in non-excluded namespaces")

                logger.info(f"Sleeping for {interval} seconds...")
                time.sleep(interval)

            except KeyboardInterrupt:
                logger.info("Shutting down gracefully...")
                break
            except Exception as e:
                logger.error(f"Error in main loop: {str(e)}")
                time.sleep(interval)

if __name__ == "__main__":
    try:
        manager = K8sResourceManager()
        manager.run()
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
