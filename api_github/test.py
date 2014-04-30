from github import GitHub

gh = GitHub("Test")
res = gh.users('octocats').get()
print res
