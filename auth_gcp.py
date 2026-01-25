from google.auth import default

def get_credentials():
    credentials, project_id = default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return credentials, project_id
