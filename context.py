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

    def validateParameterValue(self, v):
        if re.search(r'\\[^a-zA-Z0-9_]', v):
            raise ValueError('Parameter check failed (escape rule): "%s"' % v)
        for s in v:
            if not s.isdigit() and not s.isalpha() and s != ',' and s != '-' and s != '+' and s != '_' and s != ':' and s != '.' and s != '*' and s != '\\'  and s != '/':
                raise ValueError('Parameter check failed: "%s"' % v)

    def validateParameter(self, name, value):
        try:
            self.validateParameterValue(value)
        except ValueError as e:
            raise ValueError('Parameter "%s"="%s": %s' % (name, value, re.sub('^Parameter ', '', str(e))))
        return value

    def extractParameterEx(self, desc, nameFilter, validationFn=None):
        if not desc:
            return None
        if re.search(nameFilter + r'=', desc):
            m = re.search(r'(^|`|\n|\r)(?P<name>' + nameFilter + r')=(?P<value>[^\r\n\t\s`]*)(\r|\n|`|$)', desc)
            if m:
                name = m.group('name')
                value = m.group('value')
                if validationFn is None:
                    value = self.validateParameter(name, value)
                else:
                    value = validationFn(name, value)
                return (name, value)
        return None

    def extractParameter(self, desc, nameFilter, validationFn=None):
        def validationFnWrap(name, value):
            validationFn(value)
            return value
        res = self.extractParameterEx(desc, nameFilter, None if validationFn is None else validationFnWrap)
        if res is None:
            return None
        return res[1]

    def pushBuildProperty(self, properties, desc, nameFilter, propertyName):
        v = self.extractParameterEx(desc, nameFilter)
        if v is not None:
            print("%s: Apply property '%s'='%s' (from field '%s')" % (self.name, propertyName, v[1], v[0]))
            properties.setProperty(propertyName, v[1], 'Pull request')
            return v
        return None

    def getListOfAutomaticBuilders(self, pr):
        assert False

    def onUpdatePullRequest(self, prid):
        pass

    def onPullRequestBuildFinished(self, prid, bid, builderName, build, results):
        return self.onUpdatePullRequest(prid)
