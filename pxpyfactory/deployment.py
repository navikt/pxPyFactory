import requests
import os
import pxpyfactory.config
from dotenv import load_dotenv
import pxpyfactory.helpers

# Load environment variables from .env file
load_dotenv()


def trigger_deployment(environment=None, branch=None):
    deployment_success = trigger_github_deployment(environment, branch)

    if deployment_success:
        pxpyfactory.helpers.print_filter(f"--- Deployment triggered successfully - environment='{environment}', branch='{branch}'---", 0)
    else:
        pxpyfactory.helpers.print_filter(f"--- Deployment trigger failed - environment='{environment}', branch='{branch}'---", 0)

    return deployment_success


def trigger_github_deployment(environment=None, branch=None):
    # Use config defaults if not specified
    if environment is None:
        environment = pxpyfactory.config.github.DEFAULT_ENVIRONMENT
    if branch is None:
        branch = pxpyfactory.config.github.DEFAULT_BRANCH
    
    github_token = os.environ.get(pxpyfactory.config.github.ENV_VAR_TOKEN)
    if not github_token:
        pxpyfactory.helpers.print_filter(f"ERROR: {pxpyfactory.config.github.ENV_VAR_TOKEN} environment variable not set", 0)
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
    
    pxpyfactory.helpers.print_filter(f"Triggering deployment to '{environment}' from branch '{branch}'...", 1)
    
    try:
        response = requests.post(url, headers=headers, json=data)
        
        if response.status_code == pxpyfactory.config.github.SUCCESS_STATUS_CODE:
            return True
        else:
            pxpyfactory.helpers.print_filter(f"GitHub API returned status code: {response.status_code}", 0)
            pxpyfactory.helpers.print_filter(f"Response: {response.text}", 1)
            return False
    except Exception as e:
        pxpyfactory.helpers.print_filter(f"GitHub deployment request error: {str(e)}", 0)
        return False
