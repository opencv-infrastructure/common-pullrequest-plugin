import os

from twisted.internet import defer

from buildbot.status.web.auth import AuthBase, IAuth
import buildbot.status.web.authz
from zope.interface.declarations import implements
from buildbot.status.web.session import SessionManager

from pullrequest.utils import CacheFunction, cacheFileAccess
from twisted.python import log

@cacheFileAccess
def getHTPASSWD(fileName):
    print('Loading htpasswd file: %s' % fileName)
    with open(fileName, "r") as f:
        lines = f.readlines()
    lines = [l.rstrip().split(':', 3)  # user:password hash:comment:rights
             for l in lines if len(l) > 0 and not l.startswith('#')]
    return lines

class HTPasswdAuth(AuthBase):
    implements(IAuth)
    fileName = ""

    def __init__(self, fileName):
        assert os.path.exists(fileName)
        self.fileName = fileName

    @CacheFunction(30)
    def authenticate(self, user, passwd):
        try:
            lines = getHTPASSWD(self.fileName)

            lines = [l for l in lines if l[0] == user]
            if not lines:
                self.err = "Invalid user/passwd"
                return False
            pwdhash = lines[0][1]
            res = self.validatePassword(passwd, pwdhash)
            if res:
                self.err = ""
            else:
                self.err = "Invalid user/passwd"
            return res
        except:
            log.err()
            self.err = "Internal error"
            return False

    def validatePassword(self, passwd, pwdhash):
        from crypt import crypt  # @UnresolvedImport
        if pwdhash == crypt(passwd, pwdhash[0:2]):
            return True
        # openssl passwd -1
        if len(pwdhash) > 12 and pwdhash == crypt(passwd, pwdhash[0:12]):
            return True
        return False


class Authz(buildbot.status.web.authz.Authz):

    def __init__(self,
                 fileName,
                 default_action=False,
                 useHttpHeader=True,
                 httpLoginUrl=False,
                 view=True,
                 **kwargs):

        self.fileName = fileName
        auth = HTPasswdAuth(fileName)

        for act in kwargs.keys():
            if not act in self.knownActions:
                self.knownActions.append(act)

        buildbot.status.web.authz.Authz.__init__(self, default_action, auth, useHttpHeader, httpLoginUrl, view, **kwargs)

    def authenticated(self, request):
        if self.useHttpHeader:
            user = request.getUser()
            if user != '':
                pwd = request.getPassword()
                if self.auth.authenticate(user, pwd):
                    return True
        return self.session(request) is not None

    @CacheFunction(30)
    def isActionAllowed(self, user, action):
        try:
            lines = getHTPASSWD(self.fileName)

            lines = [l for l in lines if l[0] == user]
            if not lines:
                return False
            actionsStr = lines[0][3]
            actions = actionsStr.split(',')
            return action in actions
        except:
            return False

    def advertiseAction(self, action, request):
        if self.isActionAllowed(self.getUsername(request), action):
            return True
        return buildbot.status.web.authz.Authz.advertiseAction(self, action, request)

    def actionAllowed(self, action, request, *args):
        if self.isActionAllowed(self.getUsername(request), action):
            return True
        return buildbot.status.web.authz.Authz.actionAllowed(self, action, request, *args)
