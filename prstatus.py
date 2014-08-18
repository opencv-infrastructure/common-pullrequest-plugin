import cgi
import datetime
import logging

from twisted.internet import defer
from twisted.web import resource
from twisted.web import server
from twisted.web.resource import NoResource

from buildbot.util import json

from buildbot.status.web.base import AccessorMixin

from . import context, database, serviceloops
from .constants import BuildStatus
from buildbot.status.web.status_json import RequestArgToBool
from twisted.web.server import Request
from twisted.python import log
from pullrequest.utils import NotFound, Forbidden, NeedUpdate, Conflict, BadRequest, RequestArg
from pullrequest.database import getTimestamp, mainThreadCall, DBMethodCall
from twisted.python.failure import Failure

logger = logging.getLogger(__package__)

class ApiData(AccessorMixin):
    def __init__(self, context, request):
        self.context = context
        self.request = request

    @DBMethodCall
    def initialize(self, publicOnly=False):
        self.authz = self.getAuthz(self.request)
        if not publicOnly:
            @defer.inlineCallbacks
            def fn():
                self.showOperations = yield self.authz.actionAllowed('forceBuild', self.request)
                self.showPerf = yield self.authz.actionAllowed('prShowPerf', self.request)
                self.showRevertOperation = yield self.authz.actionAllowed('prRevertBuild', self.request)
            yield mainThreadCall(fn)
        else:
            self.showOperations = False
            self.showPerf = False

        self.db = db = self.context.db
        assert(isinstance(db, database.Database))

        self.active_builders = db.bcc.getActiveBuilders()
        self.active_pullrequests = db.prcc.getActivePullRequests()
        self.all_bstatuses = db.scc.getAllActiveStatuses()

    @DBMethodCall
    def getBuildersList(self):
        result = {}
        for builder in self.active_builders:
            if builder.isPerf and not self.showPerf:
                continue
            b = {}
            b.update(dict(name=builder.name, short_name=builder.name, order=builder.order, id=str(builder.bid)))
            b['status'] = 'active'  # TODO
            result[builder.order] = b
        return result

    def getPr(self, prid):
        prs = [pr for pr in self.active_pullrequests if str(pr.prid) == str(prid)]
        if len(prs) == 0:
            return None
        assert len(prs) == 1
        return prs[0]

    def getBuilder(self, bid):
        b = [b for b in self.active_builders if str(b.bid) == str(bid)]
        if len(b) == 0:
            return None
        assert len(b) == 1
        return b[0]

    @DBMethodCall
    def getPullrequestInfo(self, pr=None, prid=None):
        if pr is None:
            pr = self.getPr(prid)
            if pr is None:
                return None

        result = {}
        dict_ = pr.__dict__
        for k, v in dict_.items():
            if k.startswith('_'):
                continue
            if k in ['created_at', 'updated_at']:
                v = getTimestamp(v)
            if k == 'prid':
                k = 'id'
            result[k] = v
        result['url'] = self.context.getWebAddressPullRequest(pr)

        testFilter = self.context.extractRegressionTestFilter(pr.description)
        havePerfReport = testFilter is not None
        if havePerfReport:
            result['url_perf_report'] = self.context.getWebAddressPerfRegressionReport(pr)

        result['buildstatus'] = self.getPullrequestStatuses(pr)

        return result

    @DBMethodCall
    def getPullrequestStatusShort(self, pr=None, prid=None):
        if pr is None:
            pr = self.getPr(prid)
            if pr is None:
                return None

        result = {}
        result['buildstatus'] = self.getPullrequestStatusesShort(pr)

        return result


    @DBMethodCall
    def getPullrequestStatuses(self, pr):
        bstatuses = [s for s in self.all_bstatuses if str(s.prid) == str(pr.prid)]

        result = {}
        for b in self.active_builders:
            s = self.getPullrequestStatus(pr=pr, bid=b.bid, bstatuses=bstatuses)

            if s:
                result[b.bid] = s
        return result

    @DBMethodCall
    def getPullrequestStatus(self, pr=None, prid=None, b=None, bid=None, bstatuses=None, shortMode=False):
        if pr is None:
            pr = self.getPr(prid)
            if pr is None:
                return None
        prid = pr.prid

        if b is None:
            b = self.getBuilder(bid)
            if b is None:
                return None
        bid = b.bid

        if bstatuses is None:
            bstatuses = [s for s in self.all_bstatuses if str(s.prid) == str(prid)]

        if b.isPerf and not self.showPerf:
            return None

        testFilter = self.context.extractRegressionTestFilter(pr.description)

        bstatus = [s for s in bstatuses if str(s.bid) == str(bid)]
        bstatus = bstatus[0] if len(bstatus) > 0 else None

        s = {}
        operations = []
        if testFilter is not None or not b.isPerf:
            if bstatus:
                if not shortMode:
                    s['created_at'] = getTimestamp(bstatus.created_at)
                    s['updated_at'] = getTimestamp(bstatus.updated_at)
                    if bstatus.build_number >= 0:
                        s['build_number'] = bstatus.build_number
                        s['build_url'] = 'builders/%s/builds/%s' % (b.builders[0], bstatus.build_number)
                s['last_update'] = (datetime.datetime.utcnow() - bstatus.updated_at).total_seconds()

                stopAvailable = True
                status = bstatus.status
                if status == BuildStatus.INQUEUE:
                    s['status'] = 'queued'
                elif status == BuildStatus.SCHEDULING:
                    s['status'] = "scheduling"
                elif status == BuildStatus.SCHEDULED:
                    s['status'] = "scheduled"
                elif status == BuildStatus.BUILDING:
                    s['status'] = "building"
                elif status == BuildStatus.SUCCESS:
                    s['status'] = "success"
                    stopAvailable = False
                elif status == BuildStatus.WARNINGS:
                    s['status'] = "warnings"
                    stopAvailable = False
                elif status == BuildStatus.FAILURE:
                    s['status'] = "failure"
                    stopAvailable = False
                else:
                    s['status'] = "exception"
                    stopAvailable = False
                if self.showOperations:
                    if status != BuildStatus.INQUEUE:
                        operations.append('restart')
                    if stopAvailable:
                        operations.append('stop')
                    elif self.showRevertOperation:
                        operations.append('revert')
            else:
                s['status'] = 'not_queued'
                if self.showOperations:
                    operations.append('restart')
        else:
            pass

        if not shortMode and len(operations) > 0:
            s['operations'] = operations
            s['operations_url'] = '%s/%s/%s' % (self.context.urlpath, prid, bid)

        if len(s) > 0:
            return s
        return None

    @DBMethodCall
    def getPullrequestStatusesShort(self, pr):
        bstatuses = [s for s in self.all_bstatuses if str(s.prid) == str(pr.prid)]

        result = {}
        for b in self.active_builders:
            s = self.getPullrequestStatus(pr=pr, bid=b.bid, bstatuses=bstatuses, shortMode=True)

            if s:
                result[b.name] = s
        return result

class JsonResource(resource.Resource):
    requiredAuthAction = None

    def getRequiredAuthAction(self, request):
        return self.requiredAuthAction

    def render(self, request):
        assert isinstance(request, Request)
        @defer.inlineCallbacks
        def handle():
            try:
                try:
                    authAction = self.getRequiredAuthAction(request)
                    if authAction is not None:
                        authz = self.getAuthz(request)
                        res = yield authz.actionAllowed(authAction, request)
                        if not res:
                            logger.info("Auth action '%s' is not allowed: %s" % (authAction, request.uri))
                            raise Forbidden('Not allowed: %s' % request.uri)

                    data = yield self.asDict(request)
                    if data is None:
                        raise NotFound("Not found: %s" % request.uri)
                except NotFound as e:
                    data = dict(message=str(e), _httpCode=404)
                except Forbidden as e:
                    data = dict(message=str(e), _httpCode=403)
                except Conflict as e:
                    data = dict(message=str(e), _httpCode=409)
                except NeedUpdate as e:
                    data = dict(message=str(e), _httpCode=410)
                except BadRequest as e:
                    data = dict(message=str(e), _httpCode=400)
                except Exception as e:
                    log.err()
                    data = dict(message=str(e), _httpCode=500)

                assert isinstance(data, dict)
                httpCode = data.get('_httpCode', None)
                if httpCode is not None:
                    request.setResponseCode(httpCode)
                    del data['_httpCode']

                compact = RequestArgToBool(request, 'compact', False)
                if compact:
                    data = json.dumps(data, sort_keys=True, separators=(',', ':'))
                else:
                    data = json.dumps(data, sort_keys=True, indent=2)
                data = data.encode("utf-8")

                request.setHeader("Access-Control-Allow-Origin", "*")
                request.setHeader("content-type", "application/json")

                if httpCode is None or httpCode == 200:
                    if RequestArgToBool(request, 'as_file', False):
                        request.setHeader("content-disposition",
                                          "attachment; filename=\"%s.json\"" % request.path)

                # Make sure we get fresh pages.
                request.setHeader("Pragma", "no-cache")

                request.write(data)
                request.finish()
            except Exception as e:
                request.processingFailed(Failure(e))
                return
        defer.maybeDeferred(handle)
        return server.NOT_DONE_YET

    @defer.inlineCallbacks
    def asDict(self, request):
        raise NotImplementedError()


# /pullrequest*
class PullRequestsResource(JsonResource):

    def __init__(self, *args, **kw):
        JsonResource.__init__(self)
        self.context = kw.pop('context', None)

    @defer.inlineCallbacks
    def asDict(self, request):
        def fn(_):
            import time
            start = time.time()

            apiData = ApiData(self.context, request)
            yield apiData.initialize()

            result = {}
            result['builders'] = yield apiData.getBuildersList()

            result['pullrequests'] = {}
            for prOrigin in apiData.active_pullrequests:
                pr = yield apiData.getPullrequestInfo(pr=prOrigin)
                result['pullrequests'][prOrigin.prid] = pr

            end = time.time()
            print 'PR API status time: %s' % (end - start)
            defer.returnValue(result)
        result = yield self.context.db.asyncRun(fn)
        defer.returnValue(result)

    def getChild(self, path, req):
        try:
            if path == '':
                return self
            prid = path
            return OnePullRequestResource(self.context, prid)
        except KeyError:
            return NoResource("No such pullrequest '%s'" % cgi.escape(path))


class OnePullRequestResource(JsonResource):
    def __init__(self, context, prid):
        JsonResource.__init__(self)
        self.context = context
        self.prid = prid

    def getChild(self, path, req):
        if path == 'status':
            return OnePullRequestStatusResource(self.context, self.prid)
        bid = path
        return OnePullRequestBuildResource(self.context, self.prid, bid)

    @defer.inlineCallbacks
    def asDict(self, request):
        def fn(_):
            apiData = ApiData(self.context, request)
            yield apiData.initialize()

            result = yield apiData.getPullrequestInfo(prid=self.prid)
            defer.returnValue(result)
        result = yield self.context.db.asyncRun(fn)
        defer.returnValue(result)

# for merge service
class OnePullRequestStatusResource(JsonResource):
    def __init__(self, context, prid):
        JsonResource.__init__(self)
        self.context = context
        self.prid = prid

    @defer.inlineCallbacks
    def asDict(self, request):
        def fn(_):
            apiData = ApiData(self.context, request)
            yield apiData.initialize(publicOnly=True)

            result = yield apiData.getPullrequestStatusShort(prid=self.prid)
            defer.returnValue(result)
        result = yield self.context.db.asyncRun(fn)
        defer.returnValue(result)


class OnePullRequestBuildResourceBase(JsonResource, AccessorMixin):
    def __init__(self, context, prid, bid):
        JsonResource.__init__(self)
        self.context = context
        self.prid = prid
        self.bid = bid

    @defer.inlineCallbacks
    def asDict(self, request):
        def fn(_):
            apiData = ApiData(self.context, request)
            yield apiData.initialize()

            result = yield apiData.getPullrequestStatus(prid=self.prid, bid=self.bid)
            defer.returnValue(result)

        result = yield self.context.db.asyncRun(fn)
        defer.returnValue(result)

class OnePullRequestBuildResource(OnePullRequestBuildResourceBase):

    def getChild(self, path, request):
        action = None
        if path == "restart":
            action = RestartBuildResource(self.context, self.prid, self.bid)
        if path == "stop":
            action = StopBuildResource(self.context, self.prid, self.bid)
        if path == "revert":
            action = RevertBuildResource(self.context, self.prid, self.bid)
        if action is None:
            return NoResource()
        return action

    @defer.inlineCallbacks
    def asDict(self, request):
        def fn(_):
            apiData = ApiData(self.context, request)
            yield apiData.initialize()

            result = yield apiData.getPullrequestStatus(prid=self.prid, bid=self.bid)
            defer.returnValue(result)

        result = yield self.context.db.asyncRun(fn)
        defer.returnValue(result)


class RestartBuildResource(OnePullRequestBuildResourceBase):

    requiredAuthAction = "prRestartBuild"

    @defer.inlineCallbacks
    def asDict(self, request):
        updated_at = RequestArg(request, 'updated_at', None)

        yield serviceloops.retryBuild(self.context, self.prid, self.bid, updated_at=updated_at)

        res = yield OnePullRequestBuildResourceBase.asDict(self, request)
        defer.returnValue(res)


class StopBuildResource(OnePullRequestBuildResourceBase):

    requiredAuthAction = "prStopBuild"

    @defer.inlineCallbacks
    def asDict(self, request):
        updated_at = RequestArg(request, 'updated_at', None)
        if updated_at is None:
            raise BadRequest('updated_at parameter is missing')

        yield serviceloops.stopBuild(self.context, self.prid, self.bid, updated_at=updated_at)

        res = yield OnePullRequestBuildResourceBase.asDict(self, request)
        defer.returnValue(res)

class RevertBuildResource(OnePullRequestBuildResourceBase):

    requiredAuthAction = "prRevertBuild"

    @defer.inlineCallbacks
    def asDict(self, request):
        updated_at = RequestArg(request, 'updated_at', None)
        if updated_at is None:
            raise BadRequest('updated_at parameter is missing')

        yield serviceloops.revertBuild(self.context, self.prid, self.bid, updated_at=updated_at)

        res = yield OnePullRequestBuildResourceBase.asDict(self, request)
        defer.returnValue(res)
