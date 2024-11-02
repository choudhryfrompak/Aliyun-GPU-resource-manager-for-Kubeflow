# Kubernetes GPU Resource Manager

A Python-based tool for managing GPU resources in Kubernetes clusters with Aliyun GPU Share, specifically designed for Kubeflow environments. This tool automatically monitors and manages GPU-enabled pods based on configurable lifetime policies.

## Features

- Automatic monitoring of GPU-shared pods across namespaces
- Configurable pod termination windows based on age
- Namespace-level and pod-level configuration support
- Selective namespace exclusion
- Automatic cleanup of long-running GPU workloads
- Support for both Kubeflow notebooks and standard pods
- Built-in caching for improved performance
- Comprehensive logging system

## Prerequisites

- Python 3.6 or higher
- Kubernetes cluster with Aliyun GPU Share configured
- `kubectl` installed and configured with appropriate cluster access
- `kubectl inspect` plugin for GPU Share

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd k8s-gpu-resource-manager
```

2. Install required dependencies:
```bash
pip install kubernetes subprocess.run typing datetime
```

3. Create a default configuration file (optional - will be created automatically if not present):
```json
{
  "excluded_namespaces": [
    "kube-system",
    "kubeflow",
    "TEST"
  ],
  "default_termination_window": "2h",
  "namespaces": {
    "TEST": {
      "termination_window": "3h",
      "pods": {
        "example-pod": {
          "termination_window": "4h"
        }
      }
    }
  }
}
```

## Configuration

### Configuration File Structure

The tool uses a JSON configuration file (`pod_config.json`) with the following structure:

- `excluded_namespaces`: List of namespaces to exclude from monitoring
- `default_termination_window`: Default time window for pod termination (e.g., "2h" for 2 hours)
- `namespaces`: Namespace-specific configurations
  - `termination_window`: Override termination window for specific namespace
  - `pods`: Pod-specific configurations
    - `termination_window`: Override termination window for specific pods

### Time Window Format

Time windows can be specified in the following formats:
- Hours: e.g., "2h", "0.5h"
- Days: e.g., "1d", "0.5d"

## Running the Tool

### Standard Mode
```bash
python3 resource_manager.py
```

### Background Mode (using nohup)
```bash
nohup python3 resource_manager.py > resource_manager.out 2>&1 &
```

### Using Screen
```bash
screen -S gpu-manager
python3 resource_manager.py
# Press Ctrl+A+D to detach
```

## Logging

The tool maintains logs in two locations:
- Console output (stdout)
- `resource_manager.log` file

Log entries include:
- Timestamp
- Log level (INFO, ERROR, WARNING)
- Detailed messages about pod processing and termination

## Monitoring

To monitor the tool's operation:

1. Check the log file:
```bash
tail -f resource_manager.log
```

2. View active processes:
```bash
ps aux | grep resource_manager.py
```

## How It Works

1. **Configuration Loading**:
   - Loads configuration from JSON file
   - Creates default config if none exists

2. **Pod Discovery**:
   - Uses `kubectl inspect gpushare` to identify GPU-enabled pods
   - Filters out excluded namespaces
   - Caches pod creation times for performance

3. **Pod Processing**:
   - Checks pod age against configured termination windows
   - Applies namespace and pod-specific configurations
   - Attempts notebook deletion first, falls back to pod deletion

4. **Continuous Monitoring**:
   - Runs in a continuous loop with configurable intervals
   - Reloads configuration on each cycle
   - Implements graceful shutdown handling

## Troubleshooting

### Common Issues

1. **Permission Errors**:
   - Ensure kubectl is properly configured
   - Verify cluster access permissions

2. **Pod Not Terminating**:
   - Check logs for specific error messages
   - Verify namespace/pod configuration
   - Ensure correct RBAC permissions

3. **GPU Share Detection Issues**:
   - Verify Aliyun GPU Share installation
   - Check `kubectl inspect gpushare` functionality

### Debug Mode

For more detailed logging, modify the logging level in the code:
```python
logging.basicConfig(level=logging.DEBUG)
```

## Best Practices

1. Start with longer termination windows and adjust based on usage patterns
2. Regularly monitor the logs for unexpected terminations
3. Keep the configuration file well-organized and documented
4. Test configuration changes in a non-production environment first

## Contributing

Feel free to submit issues, fork the repository, and create pull requests for any improvements.

## License

[MIT LICENSE]
