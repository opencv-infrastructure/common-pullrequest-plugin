import re

from .database import Database

class Context:

    name = 'Pull Requests'

    debug = False
    debug_db = False
    dbname = 'pullrequests'

    urlpath = 'pullrequests'

    def __init__(self):
        self.db = Database(self)

    updatePullRequestsDelay = 120

    master = None  # : :type master: buildbot.master.BuildMaster


    trustedAuthors = None # No limitations
    reviewers = None # No limitations

    builders = {
        # runtests1=dict(name='t1', builders=['runtests1'], order=0, isPerf=False),
    }

    def updatePullRequests(self):
        assert False

    def getBuildProperties(self, pr, properties, sourcestamps):
        assert False

    def getWebAddressPullRequest(self, pr):
        assert False

    def getWebAddressPerfRegressionReport(self, pr):
        assert False

    def extractRegressionTestFilter(self, desc):
        try:
            return self.extractParameter(desc, 'check_regression[s]?')
        except ValueError:
            return None

    def extractParameter(self, desc, nameFilter):
        if not desc:
            return None
        if re.search(nameFilter + r'=', desc):
            m = re.search(nameFilter + r'=([^\r\n\t\s`]*)(\r|\n|`|$)', desc)
            if m:
                v = m.group(1)
                for s in v:
                    if not s.isdigit() and not s.isalpha() and s != ',' and s != '-' and s != '_' and s != ':' and s != '*' and s != '\\'  and s != '\/':
                        raise ValueError('Parameter check failed: %s' % v)
                return v
        return None

    def pushBuildProperty(self, properties, desc, nameFilter, propertyName):
        v = self.extractParameter(desc, nameFilter)
        if v is not None:
            properties.setProperty(propertyName, v, 'Pull request')

    def getListOfAutomaticBuilders(self, pr):
        assert False
