import os
from gitlab import GitLab

client = GitLab("https://gitlab.itseez.com/api/v3", "Test",  private_token=os.environ['GITLAB_APIKEY'])
res = client.projects().get()
print res
