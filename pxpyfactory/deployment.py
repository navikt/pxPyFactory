import requests
import os
import pxpyfactory.utils
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


def trigger_github_deployment(environment='test-px', branch='main'):
    """
    Trigger deployment workflow on pxweb2-api-nais repository
    
    Args:
        environment: Environment to deploy to (default: 'test-px')
        branch: Branch to deploy from (default: 'main')
    
    Returns:
        bool: True if deployment was triggered successfully, False otherwise
    """
    github_token = os.environ.get('GITHUB_TOKEN_PX')
    if not github_token:
        pxpyfactory.utils.print_filter("ERROR: GITHUB_TOKEN_PX environment variable not set", 0)
        return False
    
    owner = 'navikt'
    repo = 'pxweb2-api-nais'
    workflow_file = 'deploy.yml'
    
    url = f"https://api.github.com/repos/{owner}/{repo}/actions/workflows/{workflow_file}/dispatches"
    
    headers = {
        'Accept': 'application/vnd.github+json',
        'Authorization': f'Bearer {github_token}',
        'X-GitHub-Api-Version': '2022-11-28'
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
        
        if response.status_code == 204:
            pxpyfactory.utils.print_filter(f"✓ Deployment triggered successfully", 0)
            return True
        else:
            pxpyfactory.utils.print_filter(f"✗ Failed to trigger deployment: {response.status_code}", 0)
            pxpyfactory.utils.print_filter(f"Response: {response.text}", 1)
            return False
    except Exception as e:
        pxpyfactory.utils.print_filter(f"✗ Error triggering deployment: {str(e)}", 0)
        return False
