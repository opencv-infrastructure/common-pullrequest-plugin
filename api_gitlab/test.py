from gitlab import GitLab

client = GitLab("https://gitlab.itseez.com/api/v3", "Test", private_token='EAnTU7CNLHc4pFdZCxsc')
res = client.projects().get()
print res
