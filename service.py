from twisted.application.service import Service
from twisted.internet import defer, reactor
from twisted.python import log, failure


class PullRequestsService(Service):

    watchLoop = None
    schedulerLoop = None

    def __init__(self, *args, **kw):
        self.context = kw.get('context', None)
        # Service.__init__(*args, **kw)

    _already_started = False
    @defer.inlineCallbacks
    def startService(self, _reactor=reactor):
        assert not self._already_started, "can start the PullRequest service once"
        self._already_started = True

        Service.startService(self)

        print 'PullRequest service starting: %s ...' % self.context.name

        d = defer.Deferred()
        _reactor.callWhenRunning(d.callback, None)
        yield d

        try:
            from .serviceloops import PullRequestsWatchLoop, SchedulerLoop

            self.watchLoop = PullRequestsWatchLoop(self.context)
            yield self.watchLoop.start()

            self.schedulerLoop = SchedulerLoop(self.context)
            yield self.schedulerLoop.start();
        except:
            f = failure.Failure()
            log.err(f, 'while starting PullRequest service: %s' % self.context.name)
            _reactor.stop()

        log.msg('PullRequest service is running: %s' % self.context.name)

    def stopService(self):
        print 'Stop PullRequest service: %s ...' % self.context.name
        Service.stopService(self)
        if self.watchLoop:
            self.watchLoop.stop()
        if self.schedulerLoop:
            self.schedulerLoop.stop()


    def setServiceParent(self, parent):
        Service.setServiceParent(self, parent)
        self.context.master = parent
