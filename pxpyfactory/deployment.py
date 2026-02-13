import requests
import os
import pxpyfactory.utils
import pxpyfactory.config
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


def trigger_github_deployment(environment=None, branch=None):
    """
    Trigger deployment workflow on pxweb-api repository
    
    Args:
        environment: Environment to deploy to (default from config)
        branch: Branch to deploy from (default from config)
    
    Returns:
        bool: True if deployment was triggered successfully, False otherwise
    """
    # Use config defaults if not specified
    if environment is None:
        environment = pxpyfactory.config.github.DEFAULT_ENVIRONMENT
    if branch is None:
        branch = pxpyfactory.config.github.DEFAULT_BRANCH
    
    github_token = os.environ.get(pxpyfactory.config.github.ENV_VAR_TOKEN)
    if not github_token:
        pxpyfactory.utils.print_filter(f"ERROR: {pxpyfactory.config.github.ENV_VAR_TOKEN} environment variable not set", 0)
        return False
    
    url = f"https://api.github.com/repos/{pxpyfactory.config.github.OWNER}/{pxpyfactory.config.github.REPO}/actions/workflows/{pxpyfactory.config.github.WORKFLOW_FILE}/dispatches"
    
    headers = {
        'Accept': pxpyfactory.config.github.ACCEPT_HEADER,
        'Authorization': f'Bearer {github_token}',
        'X-GitHub-Api-Version': pxpyfactory.config.github.API_VERSION
    }
    
    data = {
        'ref': branch,
        'inputs': {
            'environment': environment
        }
    }
    
    pxpyfactory.utils.print_filter(f"Triggering deployment to '{environment}' from branch '{branch}'...", 1)
    
    try:
        response = requests.post(url, headers=headers, json=data)
        
        if response.status_code == pxpyfactory.config.github.SUCCESS_STATUS_CODE:
            pxpyfactory.utils.print_filter(f"✓ Deployment triggered successfully", 0)
            return True
        else:
            pxpyfactory.utils.print_filter(f"✗ Failed to trigger deployment: {response.status_code}", 0)
            pxpyfactory.utils.print_filter(f"Response: {response.text}", 1)
            return False
    except Exception as e:
        pxpyfactory.utils.print_filter(f"✗ Error triggering deployment: {str(e)}", 0)
        return False
