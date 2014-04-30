from twisted.python import log
from twisted.python.failure import Failure
from twisted.internet import defer
from twisted.web import resource
from twisted.web import server

from buildbot.status.web import baseweb
from buildbot.status.web.base import AccessorMixin
from buildbot.status.web.base import StaticFile
from .prstatus import PullRequestsResource
from .prstatus import JsonResource
from .utils import NotFound

class WebStatus(baseweb.WebStatus):

    def __init__(self, *args, **kw):
        self.pullrequests = kw.pop('pullrequests', None)
        baseweb.WebStatus.__init__(self, *args, **kw)

    def setupUsualPages(self, numbuilds, num_events, num_events_max):
        baseweb.WebStatus.setupUsualPages(self, numbuilds, num_events, num_events_max)
        for context in self.pullrequests:
            self.putChild(context.urlpath, PullRequestsResource(context=context))
        self.putChild('pullrequest', StaticFile('pullrequest_ui/src'));
        self.putChild('login', LoginResource());
        self.putChild('authInfo', AuthInfoResource());


class LoginResource(resource.Resource, AccessorMixin):

    def render(self, request):
        assert isinstance(request, server.Request)
        @defer.inlineCallbacks
        def handle():
            try:
                try:
                    authz = self.getAuthz(request)
                    res = yield authz.authenticated(request)

                    if res:
                        status = request.site.buildbot_service.master.status
                        root = status.getBuildbotURL()
                        request.redirect(request.requestHeaders.getRawHeaders('referer', [root])[0])
                    else:
                        request.setResponseCode(401)
                        request.setHeader("WWW-Authenticate", "Basic realm=\"%s\"" % 'Buildbot')
                except Exception as e:
                    log.err()
                    request.setResponseCode(500)
                request.finish()
            except Exception as e:
                request.processingFailed(Failure(e))
                return
        defer.maybeDeferred(handle)
        return server.NOT_DONE_YET


class AuthInfoResource(JsonResource, AccessorMixin):
    def __init__(self):
        JsonResource.__init__(self)

    @defer.inlineCallbacks
    def asDict(self, request):
        res = {}
        authz = self.getAuthz(request)
        authenticated = yield authz.authenticated(request)
        if authenticated:
            res['user'] = request.getUser()
        else:
            raise NotFound('Not authorized')
        defer.returnValue(res)
