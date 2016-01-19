import logging

from twisted.internet import defer, reactor, task
from twisted.python import log, failure

from zope.interface.declarations import implements

import buildbot
import buildbot.process
from buildbot.interfaces import IStatusReceiver
from buildbot.master import BuildMaster
from buildbot.process.properties import Properties
from buildbot.process.builder import Builder
from buildbot.status.builder import BuilderStatus
import buildbot.status.builder  # BuildStatus
from buildbot.status.buildrequest import BuildRequestStatus
from buildbot.status.master import Status

from .constants import BuildStatus
from pullrequest import constants, database
from pullrequest.database import mainThreadCall
from pullrequest.utils import NotFound, BadRequest, NeedUpdate

logger = logging.getLogger(__package__)

class PullRequestsWatchLoop():
    isStarted = False

    def __init__(self, context):
        self.context = context

    @defer.inlineCallbacks
    def start(self):
            db = self.context.db
            res = yield db.prcc.getActivePullRequests()
            print "Number of active pull requests: %d" % len(res)

            self.context.allowScheduling = False

            self.isStarted = True

            task.deferLater(reactor, 1, self.updatePullRequests)

    def stop(self):
            self.isStarted = False

    @defer.inlineCallbacks
    def updatePullRequests(self):
        if not self.isStarted:
            print 'Pull requests service is stopping, exit from update loop...'
            defer.returnValue(None)

        task.deferLater(reactor, self.context.updatePullRequestsDelay, self.updatePullRequests)

        db = self.context.db

        try:
            pullrequests = yield self.context.updatePullRequests()
            if pullrequests is not None:
                processed_prs = []
                for pullrequest in pullrequests:
                    pr = yield self.updatePR(pullrequest)
                    processed_prs.append(pr['id'])

                active_pullrequests = yield db.prcc.getActivePullRequests()

                for pullRequest in active_pullrequests:
                    if not pullRequest.prid in processed_prs:
                        print "Mark PR #%s inactive" % pullRequest.prid
                        pullRequest.status = -1
                        yield db.prcc.updatePullRequest(pullRequest)
                        ss = yield db.asyncRun(lambda _: pullRequest.getBuildStatus())
                        for s in ss:
                            if s.active:
                                try:
                                    yield cancelBuild(s)
                                except:
                                    log.err()
        except:
            log.err(failure.Failure(), 'while updating pull requests: %s' % self.context.name)
            pass

        try:
            self.context.allowScheduling = True
            active_bulders = yield db.bcc.getActiveBuilders()
            for b in active_bulders:
                yield tryScheduleForBuilder(self.context, b.builders[0])
        except:
            log.err(failure.Failure(), 'while updating pull requests: %s' % self.context.name)
            pass


    @defer.inlineCallbacks
    def updatePR(self, pr):
        db = self.context.db
        prid = pr['id']
        head_sha = pr['head_sha']

        def fn(session):
            head_sha_old = None
            current = db.prcc.getPullRequest(prid)
            if current:
                if current.status < 0:
                    current.status = 0
                head_sha_old = current.head_sha
                for k in pr.keys():
                    if k == 'id':
                        continue
                    v = getattr(current, k)
                    if v != pr[k]:
                        setattr(current, k, pr[k])
                if current in session.dirty:
                    persistent_info = current.info.get('persistent', None)
                    current.info = {'persistent': persistent_info} if persistent_info is not None else {}
                    current.info.update(pr.get('info', {}))
                    session.commit()
            else:
                current = database.Pullrequest(prid)
                for k in pr.keys():
                    if k == 'id':
                        continue
                    setattr(current, k, pr[k])
                current = db.prcc.insertPullRequest(current);
            return head_sha_old
        head_sha_old = yield db.asyncRun(fn)
        if head_sha != head_sha_old:
            yield self.queueBuildersForPR(prid, head_sha, head_sha_old)

        defer.returnValue(pr)

    @defer.inlineCallbacks
    def queueBuildersForPR(self, prid, head_sha, head_sha_old):
        db = self.context.db
        print "Reschedule builders for PR #%d ('%s' -> '%s')" % (prid, head_sha_old, head_sha)

        active_builders = yield db.bcc.getActiveBuilders()
        pr = yield db.prcc.getPullRequest(prid);
        queueBuilders = self.context.getListOfAutomaticBuilders(pr)
        testFilter = self.context.extractRegressionTestFilter(pr.description)
        for b in active_builders:
            bid = b.bid
            bstatus = yield db.scc.getStatus(prid, bid)
            try:
                if bstatus:
                    yield stopBuild(self.context, prid, bid)
                    bstatus.active = False
                    yield db.scc.updateStatus(bstatus)
            except:
                log.err()
            if not queueBuilders or not (b.name in queueBuilders or b.internal_name in queueBuilders):
                continue
            if testFilter is None and b.isPerf:
                continue
            if bstatus is None and not (self.context.trustedAuthors is None or self.context.reviewers is None) \
                    and (head_sha_old is not None or not \
                        (pr.author in self.context.trustedAuthors and pr.assignee in self.context.reviewers)):
                continue
            bstatus = database.Status()
            bstatus.bid = bid
            bstatus.head_sha = head_sha
            yield pr.addBuildStatus(bstatus)
        yield self.context.onUpdatePullRequest(prid)

def _getInternalNameByBuilderName(context, builderName):
    for internal_name, b in context.builders.items():
        if builderName in b['builders']:
            return internal_name
    raise Exception('Unknown builder: %s' % builderName)

schedulerLock = defer.DeferredLock()

@defer.inlineCallbacks
def tryScheduleForBuilder(context, builderName, resetScheduledBuilds=True):
    if not context.allowScheduling:
        return

    db = context.db
    master = context.master
    assert isinstance(master, BuildMaster)

    yield schedulerLock.acquire()

    try:
        b = yield db.bcc.getBuilderByName(_getInternalNameByBuilderName(context, builderName))
        try:
            builders = master.botmaster.builders
            builder = builders.get(builderName, None)
            if builder is None:
                return
            assert isinstance(builder, Builder)
            builder_status = builder.builder_status
            assert isinstance(builder_status, BuilderStatus)
            if builder_status.currentBigState == 'offline':
                return
            pending = yield builder_status.getPendingBuildRequestStatuses()
            if len(pending) > 0:
                return
        except:
            log.err()
            return

#         if resetScheduledBuilds:
#             # reset lost jobs with 'scheduled' state, but buildbot doesn't have pending builds
#             yield db.execute(db.m.status.update()
#                     .where(db.m.status.c.builder_id == b['id'])
#                     .where(db.m.status.c.status == BuildStatus.SCHEDULED)
#                     .values(brid=-1, build_number=-1, status=BuildStatus.INQUEUE))

        prb_status = yield db.scc.getStatusToSchedule(b.bid);
        if prb_status:
            prid = prb_status.prid

            print 'PR #%s scheduling job on builder=%s' % (prid, b.name)
            try:
                pr = yield db.asyncRun(lambda _: prb_status.pr)

                properties = Properties()
                properties.setProperty('pullrequest_service', context.name, 'Pull request')
                sourcestamps = []
                result = yield context.getBuildProperties(pr, b, properties, sourcestamps)

                if not result:
                    print "ERROR: Can't get build properties: PR #%s builder=%s" % (prid, builder.name)
                    prb_status.status = BuildStatus.FAILURE
                    yield db.scc.updateStatus(prb_status)
                    return

                setid = yield master.db.sourcestampsets.addSourceStampSet()
                for ss in sourcestamps:
                    assert isinstance(ss, dict)
                    yield master.db.sourcestamps.addSourceStamp(
                                codebase=ss.get('codebase', None),
                                repository=ss.get('repository', ''),
                                branch=ss.get('branch', None),
                                revision=ss.get('revision', None),
                                project=ss.get('project', ''),
                                changeids=[c['number'] for c in ss.get('changes', [])],
                                patch_body=ss.get('patch_body', None),
                                patch_level=ss.get('patch_level', None),
                                patch_author=ss.get('patch_author', None),
                                patch_comment=ss.get('patch_comment', None),
                                sourcestampsetid=setid)

                prb_status.status = BuildStatus.SCHEDULING
                yield db.scc.updateStatus(prb_status)

                (bsid, brids) = yield master.addBuildset(sourcestampsetid=setid,
                        reason="#%s (%s) on %s" % (prid, pr.head_sha, builderName),
                        properties=properties.asDict(),
                        builderNames=[builderName],
                        external_idstring="PR #%s" % prid)
                assert len(brids) == 1

                prb_status.brid = brids[builderName]
                yield db.scc.updateStatus(prb_status)
            except:
                log.err()
                prb_status.status = BuildStatus.EXCEPTION
                yield db.scc.updateStatus(prb_status)

    finally:
        schedulerLock.release()

class BuildBotStatusReceiver():
    implements(IStatusReceiver)

    def __init__(self, context):
        self.context = context

    @defer.inlineCallbacks
    def builderAdded(self, builderName, builder):
        assert isinstance(builder, BuilderStatus)

        db = self.context.db

        b = None
        for internal_name, b in self.context.builders.items():
            if builderName in b['builders']:
                b = yield db.bcc.getBuilderByName(internal_name)
                assert b is not None
                break
        else:
            logger.error('Unknown builder: %s' % builderName)
            return

        print "+%s" % builderName
#         b = yield db.bcc.getBuilderByName(builderName)
#         assert b
#
#         # reset jobs with 'scheduling' state
#         yield db.execute(db.m.status.update()
#                 .where(db.m.status.c.builder_id == b['id'])
#                 .where(db.m.status.c.status == BuildStatus.SCHEDULING)
#                 .values(brid=-1, build_number=-1, status=BuildStatus.INQUEUE))
#
#         # reset jobs with 'building' state
#         yield db.execute(db.m.status.update()
#                 .where(db.m.status.c.builder_id == b['id'])
#                 .where(db.m.status.c.status == BuildStatus.BUILDING)
#                 .values(brid=-1, build_number=-1, status=BuildStatus.INQUEUE))
#
#         # reset jobs with 'building' state
#         yield db.execute(db.m.status.update()
#                 .where(db.m.status.c.builder_id == b['id'])
#                 .where(db.m.status.c.status >= BuildStatus.SUCCESS)
#                 .where(db.m.status.c.build_number < 0)
#                 .values(brid=-1, build_number=-1, status=BuildStatus.INQUEUE))
#
#         bstatuses = yield db.execute(db.m.status.select()
#                 .where(db.m.status.c.builder_id == b['id'])
#                 .where(db.m.status.c.status > BuildStatus.FAILURE))
#         if bstatuses:
#             for bstatus in bstatuses:
#                 try:
#                     if bstatus['retry_count'] > 0:
#                         yield db.scc.updateStatus(bstatus['pullrequest_id'], bstatus['builder_id'],
#                                 retry_count=bstatus['retry_count'] - 1,
#                                 order=bstatus['order'] + 1000000,
#                                 brid=-1, build_number=-1, status=BuildStatus.INQUEUE)
#                     else:
#                         print "ERROR: Can't complete build for PR#%d" % bstatus['pullrequest_id']
#                 except:
#                     log.err()

        defer.returnValue(BuilderStatusReceiver(self.context, b))

class BuilderStatusReceiver():
    implements(IStatusReceiver)
    # builderChangedState, buildStarted, buildFinished, requestSubmitted, requestCancelled

    def __init__(self, context, builder):
        self.context = context
        self.bid = builder.bid
        self.name = builder.builders[0]

    @defer.inlineCallbacks
    def builderChangedState(self, builderName, state):
        if state == 'idle':
            print "idle: %s" % builderName
            yield tryScheduleForBuilder(self.context, builderName, resetScheduledBuilds=True)
        elif state == 'offline':
            print "offline: %s" % builderName

    @defer.inlineCallbacks
    def buildStarted(self, builderName, build):
        try:
            assert isinstance(build, buildbot.status.builder.BuildStatus)
            properties = build.properties
            assert isinstance(properties, Properties)
            if properties.getProperty('pullrequest_service', None) != self.context.name:
                return
            prid = properties.getProperty('pullrequest', None)
            if prid is None:
                return
            db = self.context.db
            def fn(session):
                # pr = yield db.prcc.getPullRequest(prid)
                builders = self.context.master.botmaster.builders
                builder = builders.get(self.name, None)  # : type builder: buildbot.process.builder.Builder
                assert isinstance(builder, Builder)
                b = builder.getBuild(build.number)
                bstatus = db.scc.getStatusForBuildRequest(prid, self.bid, b.requests[0].id)
                if not bstatus:  # TODO Workaround
                    logger.warning("buildStarted(%s): #PR%s: can't find build status. Ignore" % (builderName, prid))
                    return
                sha = properties.getProperty('head_sha', None)
                if sha != bstatus.head_sha:
                    logger.error('buildStarted(%s): #PR%d: wrong commit hash (build %s vs expected %s). Ignore' % (builderName, prid, sha, bstatus.head_sha))
                    return
                logger.info('buildStarted(%s): #PR%d' % (builderName, prid))
                bstatus.status = BuildStatus.BUILDING
                bstatus.build_number = build.number
                db.scc.updateStatus(bstatus)
                if not bstatus.active:
                    logger.warning('buildStarted(%s): #PR%d. Stop inactive build' % (builderName, prid))
                    try:
                        logger.info("Cancel build #%s on %s..." % (bstatus.build_number, builderName))
                        @defer.inlineCallbacks
                        def cancel():
                            yield b.stopBuild("canceled by PR service (run inactive)")
                        yield mainThreadCall(cancel)
                    except:
                        log.err()
            yield db.asyncRun(fn)
        except:
            log.err()

    @defer.inlineCallbacks
    def buildFinished(self, builderName, build, results):
        try:
            assert isinstance(build, buildbot.status.builder.BuildStatus)
            properties = build.properties
            assert isinstance(properties, Properties)
            if properties.getProperty('pullrequest_service', None) != self.context.name:
                return
            prid = properties.getProperty('pullrequest', None)
            if prid is None:
                return
            db = self.context.db
            def fn(session):
                # pr = yield db.prcc.getPullRequest(prid)
                bstatus = db.scc.getStatusForBuildNumber(prid, self.bid, build.number)
                if bstatus is None:
                    logger.warning("buildFinished(%s): #PR%s: can't find build status. Ignore" % (builderName, prid))
                    return
                sha = properties.getProperty('head_sha', None)
                if sha != bstatus.head_sha:
                    logger.error('buildFinished(%s): #PR%s: wrong commit hash (build %s vs expected %s)' % (builderName, prid, sha, bstatus.head_sha))
                    return
                logger.info('buildFinished(%s): #PR%s' % (builderName, prid))
                bstatus.status = results
                db.scc.updateStatus(bstatus)
            yield db.asyncRun(fn)
            yield self.context.onPullRequestBuildFinished(prid, self.bid, builderName, build, results)
        except:
            log.err()

    @defer.inlineCallbacks
    def requestSubmitted(self, request):
        try:
            assert isinstance(request, BuildRequestStatus)
            properties = yield request.getBuildProperties()
            assert isinstance(properties, Properties)
            if properties.getProperty('pullrequest_service', None) != self.context.name:
                return
            prid = properties.getProperty('pullrequest', None)
            if prid is None:
                return
            if self.name != request.buildername:
                return
            db = self.context.db
            def fn(session):
                pr = yield db.prcc.getPullRequest(prid)
                bstatus = db.scc.getStatusForBuildRequest(prid, self.bid, request.brid)
                if not bstatus:
                    logger.info("requestSubmitted(%s #%d): #PR%s: adding new builder status" % (request.buildername, request.brid, prid))
                    bstatus = database.Status()
                    bstatus.active = True
                    bstatus.status = BuildStatus.SCHEDULED
                    bstatus.bid = self.bid
                    bstatus.brid = request.brid
                    bstatus.head_sha = properties.getProperty('head_sha', None)
                    yield pr.addBuildStatus(bstatus)
                    return
                sha = properties.getProperty('head_sha', None)
                if sha != bstatus.head_sha:
                    print 'requestSubmitted(%s): #PR%s: wrong commit hash (request %s vs pr build status %s). Ignore' % (request.buildername, prid, sha, bstatus.head_sha)
                    return
                print 'requestSubmitted(%s): #PR%s' % (request.buildername, prid)
                if bstatus.active:
                    bstatus.status = BuildStatus.SCHEDULED
                    yield db.scc.updateStatus(bstatus)
                else:
                    @defer.inlineCallbacks
                    def cancel():
                        buildrequest = yield request._getBuildRequest()
                        assert isinstance(buildrequest, buildbot.process.buildrequest.BuildRequest)
                        yield buildrequest.cancelBuildRequest()
                        logger.info("Build request for PR #%s (on %s) canceled (start inactive build)" % (prid, self.name))
                    yield mainThreadCall(cancel)
            yield db.asyncRun(fn)
        except:
            log.err()

    @defer.inlineCallbacks
    def requestCancelled(self, builder, request):
        try:
            assert isinstance(request, BuildRequestStatus)
            properties = yield request.getBuildProperties()
            assert isinstance(properties, Properties)
            if properties.getProperty('pullrequest_service', None) != self.context.name:
                return
            prid = properties.getProperty('pullrequest', None)
            if prid is None:
                return
            if self.name != request.buildername:
                return
            db = self.context.db
            def fn(session):
                # pr = db.prcc.getPullRequest(prid)
                bstatus = db.scc.getStatus(prid, self.bid)
                sha = properties.getProperty('head_sha', None)
                if sha != bstatus.head_sha:
                    print 'requestCancelled(%s): #PR%s: wrong commit hash (request %s vs pr build status %s). Ignore' % (request.buildername, prid, sha, bstatus.head_sha)
                    return
                print 'requestCancelled(%s): #PR%s' % (request.buildername, prid)
                if bstatus.active:
                    bstatus.status = BuildStatus.INQUEUE
                    bstatus.build_number = -1
                    bstatus.brid = -1
                    db.scc.updateStatus(bstatus)
            yield db.asyncRun(fn)
        except:
            log.err()


class SchedulerLoop():
    isStarted = False

    def __init__(self, context):
        self.context = context
        self.statusReceiver = BuildBotStatusReceiver(context)

    @defer.inlineCallbacks
    def start(self):
        print "PR: Start scheduler service..."

        db = self.context.db
        def fn(session):
            database.Builder.startup(self.context)
            active_builders = db.bcc.getActiveBuilders()
            print "Number of active builders: %d" % len(active_builders)
        yield db.asyncRun(fn)

        while True:
            master = self.context.master
            if not master:
                yield task.deferLater(reactor, 5, lambda _: None)
                continue
            assert isinstance(master, BuildMaster)

            status = master.getStatus()
            assert isinstance(status, Status)

            yield status.subscribe(self.statusReceiver)
            break

        self.isStarted = True


    def stop(self):
        print "PR: Stop scheduler service..."

        self.isStarted = False
        try:
            master = self.context.master
            status = master.getStatus()
            assert isinstance(status, Status)
            status.unsubscribe(self.statusReceiver)
            self.statusReceiver = None
        except:
            f = failure.Failure()
            log.err(f, 'while stop scheduler loop')
            reactor.stop()


@defer.inlineCallbacks
def retryBuild(context, prid, bid, updated_at=None):
    db = context.db  # : :type db: database.Database
    s = yield db.scc.getStatus(prid, bid)
    if s:
        try:
            yield cancelBuild(s, updated_at)
        except NeedUpdate:
            raise
        except:
            log.err()
        def cancelCommit(_):
            s.active = False
            db.scc.updateStatus(s)
        yield db.asyncRun(cancelCommit)
    def fn(session):
        pr = db.prcc.getPullRequest(prid)
        b = db.bcc.getBuilder(bid)
        if pr is None:
            raise NotFound("Invalid PR: %s" % prid)
        if b is None:
            raise NotFound("Invalid builder ID: %s" % bid)
        testFilter = context.extractRegressionTestFilter(pr.description)
        if testFilter is None and b.isPerf:
            raise BadRequest("Can't queue perf builder without regression filter")
        s = database.Status()
        s.status = BuildStatus.INQUEUE
        s.bid = bid
        s.head_sha = pr.head_sha
        pr.addBuildStatus(s)
        session.commit()
    yield db.asyncRun(fn)

@defer.inlineCallbacks
def stopBuild(context, prid, bid, updated_at=None):
    db = context.db  # : :type db: database.Database
    s = yield db.scc.getStatus(prid, bid)
    yield cancelBuild(s, updated_at)

@defer.inlineCallbacks
def revertBuild(context, prid, bid):
    assert False

@defer.inlineCallbacks
def cancelBuild(buildStatus, updated_at=None):
    # :param pullrequest.database.Status: buildStatus
    buildStatus.checkUpdatedTimestamp(updated_at)
    context = buildStatus.getContext()  # : :type context: context.Context
    db = context.db  # : :type db: database.Database
    def fn(session):
        builderNames = buildStatus.builder.builders
        master = context.master  # : :type master: buildbot.master.BuildMaster
        if buildStatus.status in [constants.BuildStatus.INQUEUE]:
            buildStatus.active = False
            return
        elif buildStatus.status in [constants.BuildStatus.SCHEDULING]:
            buildStatus.active = False
            return
        elif buildStatus.status in [constants.BuildStatus.SCHEDULED]:
            logger.info("Cancel scheduled build: PR=%s, builders=%s" % (buildStatus.pr.prid, ','.join(builderNames)))
            buildStatus.active = False
            session.commit()
            builders = master.botmaster.builders
            found = False
            for bName in builderNames:
                builder = builders.get(bName, None)  # : type builder: buildbot.process.builder.Builder
                if builder is None:
                    continue
                assert isinstance(builder, Builder)
                builder_status = builder.builder_status
                assert isinstance(builder_status, BuilderStatus)
                pendings = yield mainThreadCall(builder_status.getPendingBuildRequestStatuses)
                if len(pendings) > 0:
                    for pending in pendings:
                        assert isinstance(pending, BuildRequestStatus)
                        if pending.brid == buildStatus.brid:
                            found = True
                            try:
                                @defer.inlineCallbacks
                                def cancel():
                                    buildrequest = yield pending._getBuildRequest()
                                    assert isinstance(buildrequest, buildbot.process.buildrequest.BuildRequest)
                                    yield buildrequest.cancelBuildRequest()
                                    logger.info("Build request for PR #%s (on %s) canceled" % (buildStatus.pr.prid, bName))
                                yield mainThreadCall(cancel)
                            except:
                                log.err('during canceling build')
                                raise
            if not found:
                logger.info("Can't find pending build: PR=%s, builders=%s" % (buildStatus.pr.prid, ','.join(builderNames)))
            return
        elif buildStatus.status in [constants.BuildStatus.BUILDING]:
            builders = master.botmaster.builders
            logger.info("Stop processing build: PR=%s, builders=%s" % (buildStatus.pr.prid, ','.join(builderNames)))
            for bName in builderNames:
                builder = builders.get(bName, None)  # : type builder: buildbot.process.builder.Builder
                if builder is None:
                    continue
                assert isinstance(builder, Builder)
                build = builder.getBuild(buildStatus.build_number)
                if build:
                    try:
                        assert isinstance(build, buildbot.process.build.Build)
                        logger.info("Cancel build #%s on %s" % (buildStatus.build_number, bName))
                        @defer.inlineCallbacks
                        def cancel():
                            yield build.stopBuild("canceled by PR service")
                        yield mainThreadCall(cancel)
                    except:
                        log.err()
            return
        elif buildStatus.status >= constants.BuildStatus.SUCCESS:
            pr = buildStatus.pr
            logger.info("Build was already finished with status=%s: PR=%s, builders=%s" % (BuildStatus.toString[buildStatus.status], pr.prid, ','.join(builderNames)))
            return
        assert False
    yield db.asyncRun(fn)
