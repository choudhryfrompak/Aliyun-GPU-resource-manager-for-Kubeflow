#!/usr/bin/env python3
import json
import subprocess
import time
from datetime import datetime, timezone
import re
import logging
import os
from typing import Dict, List, Optional

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

    def load_config(self) -> dict:
        """Load configuration from JSON file with error handling"""
        default_config = {
            "excluded_namespaces": ["kube-system", "kubeflow"],
            "default_termination_window": "2h",
            "namespaces": {}
        }
        
        try:
            if not os.path.exists(self.config_file):
                with open(self.config_file, 'w') as f:
                    json.dump(default_config, f, indent=2)
                logger.info("Created default config file")
                return default_config

            with open(self.config_file, 'r') as f:
                try:
                    config = json.load(f)
                    logger.info("Successfully loaded config file")
                    return config
                except json.JSONDecodeError as e:
                    logger.error(f"Invalid JSON in config file: {str(e)}")
                    logger.info("Using default configuration")
                    return default_config
                
        except Exception as e:
            logger.error(f"Error handling config file: {str(e)}")
            logger.info("Using default configuration")
            return default_config

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
                    base_name = re.sub(r'-\d+$', '', pod_name)
                    
                    # Skip excluded namespaces early
                    if namespace in self.config.get("excluded_namespaces", []):
                        logger.debug(f"Skipping pod in excluded namespace: {namespace}/{pod_name}")
                        continue
                    
                    pod_info = {
                        'name': base_name,
                        'namespace': namespace,
                        'node': current_node,
                        'full_name': pod_name
                    }
                    pods.append(pod_info)

        found_pods = len(pods)
        logger.info(f"Found {found_pods} pods in non-excluded namespaces")
        return pods

    def get_pod_creation_time(self, namespace: str, pod_name: str) -> Optional[datetime]:
        """Get pod creation timestamp with caching"""
        cache_key = f"{namespace}/{pod_name}"
        
        if cache_key in self.creation_time_cache:
            cached_time, timestamp = self.creation_time_cache[cache_key]
            if time.time() - timestamp < self.cache_expiry:
                return cached_time

        cmd = ['kubectl', 'get', 'pod', pod_name, '-n', namespace, 
               '-o', 'jsonpath={.metadata.creationTimestamp}']
        output = self.execute_command(cmd)
        
        if output:
            try:
                creation_time = datetime.strptime(
                    output.strip(), '%Y-%m-%dT%H:%M:%SZ'
                ).replace(tzinfo=timezone.utc)
                self.creation_time_cache[cache_key] = (creation_time, time.time())
                return creation_time
            except ValueError as e:
                logger.error(f"Error parsing creation time: {e}")
        return None

    def should_terminate_pod(self, creation_time: datetime, termination_window: str) -> bool:
        """Check if pod should be terminated based on its age"""
        if not creation_time:
            return False

        try:
            match = re.match(r'(\d*\.?\d+)([hd])', termination_window)
            if not match:
                return False

            value = float(match.group(1))
            unit = match.group(2)
            
            current_time = datetime.now(timezone.utc)
            age = current_time - creation_time
            age_hours = age.total_seconds() / 3600
            
            if unit == 'h':
                return age_hours > value
            elif unit == 'd':
                return age_hours > (value * 24)
            return False
            
        except Exception as e:
            logger.error(f"Error checking termination: {e}")
            return False

    def terminate_pod(self, namespace: str, name: str, full_name: str) -> bool:
        """Terminate a pod using kubectl"""
        try:
            logger.info(f"Terminating pod {full_name} in namespace {namespace}")
            # Try notebook deletion first
            result = self.execute_command(
                ['kubectl', 'delete', 'notebook', '-n', namespace, name]
            )
            if result is None:
                # If notebook deletion fails, try pod deletion
                logger.info("Notebook deletion failed, trying pod deletion...")
                result = self.execute_command(
                    ['kubectl', 'delete', 'pod', '-n', namespace, full_name]
                )
            return result is not None
        except Exception as e:
            logger.error(f"Error terminating pod: {e}")
            return False

    def process_pods(self, pods: List[dict]) -> None:
        """Process all found pods"""
        for pod in pods:
            namespace = pod['namespace']
            name = pod['name']
            full_name = pod['full_name']
            
            # Get termination window from config
            namespace_config = self.config["namespaces"].get(namespace, {})
            pod_config = namespace_config.get("pods", {}).get(name, {})
            termination_window = (
                pod_config.get("termination_window") or
                namespace_config.get("termination_window") or
                self.config.get("default_termination_window", "2h")
            )
            
            creation_time = self.get_pod_creation_time(namespace, full_name)
            if creation_time:
                age = datetime.now(timezone.utc) - creation_time
                age_hours = age.total_seconds() / 3600
                logger.info(f"Pod {full_name} in {namespace} age: {age_hours:.1f}h, window: {termination_window}")
                
                if self.should_terminate_pod(creation_time, termination_window):
                    logger.info(f"Pod {full_name} exceeded window of {termination_window}")
                    if self.terminate_pod(namespace, name, full_name):
                        logger.info(f"Successfully terminated pod {full_name}")
                    else:
                        logger.error(f"Failed to terminate pod {full_name}")
            else:
                logger.warning(f"Could not get creation time for pod {full_name}")

    def run(self, interval: int = 60):
        """Main loop"""
        logger.info("Starting Kubernetes Resource Manager")
        logger.info(f"Excluded namespaces: {', '.join(self.config.get('excluded_namespaces', []))}")
        
        while True:
            try:
                logger.info("Starting new check cycle...")
                
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
