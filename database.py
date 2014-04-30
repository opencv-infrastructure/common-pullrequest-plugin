import datetime
import json
import logging
import operator
import pprint
import sys
import threading
import types

from pullrequest.constants import BuildStatus
from pullrequest import constants
from pullrequest.utils import NeedUpdate
instance_dict = operator.attrgetter("__dict__")

# from . import constants
from twisted.python import threadpool, log, failure
from twisted.internet import threads, reactor, defer

import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import backref, relationship
from sqlalchemy.orm.session import object_session
from twisted.python.threadpool import ThreadPool

logger = logging.getLogger(__package__)

Base = declarative_base()

_epoch = datetime.datetime(1970, 1, 1)
def getTimestamp(d):
    t = (d - _epoch).total_seconds()
    return t

def fromTimestamp(timestamp):
    t = _epoch + timestamp
    return t

def checkUpdatedAtTimestamp(obj, updated_at_timestamp):
    return str(getTimestamp(obj.updated_at)) == str(updated_at_timestamp)

class _DeferredWrap():
    def __init__(self, d):
        self.deferred = d

class _GeneratorWrap():
    def __init__(self, gen):
        self.generator = gen

# Only one thread
class PRDBThread(threadpool.ThreadPool):

    running = False
    session = None  # DB session
    workerThread = None

    def __init__(self, context):
        self.context = context

        threadpool.ThreadPool.__init__(self, minthreads=1, maxthreads=1, name='PRDB-%s' % context.dbname)

        self._start_evt = reactor.callWhenRunning(self._start)

    def asyncRun(self, fn, *args, **kwargs):
        def proxy():
            try:
                assert self.session
                return fn(self.session, *args, **kwargs)
            except:
                log.err()
                raise
        if self.workerThread == threading.current_thread():
            return proxy()
        else:
            d = defer.maybeDeferred(self._asyncRunEx, None, fn, *args, **kwargs)
            return d

    @defer.inlineCallbacks
    def asyncRunEx(self, fn, *args, **kwargs):
        gen = fn(self.session, *args, **kwargs)
        if not isinstance(gen, types.GeneratorType):
            raise TypeError(
                "asyncRunEx requires %r to produce a generator; "
                "instead got %r" % (fn, gen))
        result = yield self._asyncRunEx(gen, None)
        defer.returnValue(result)

    @defer.inlineCallbacks
    def _asyncRunEx(self, gen, fn, *args, **kwargs):
        params = [gen]
        result = None
        while 1:
            try:
                def worker():
                    try:
                        gen = params[0]
                        if gen is None:
                            res = fn(self.session, *args, **kwargs)
                            if not isinstance(res, types.GeneratorType):
                                defer.returnValue(res)
                            else:
                                gen = res
                                params[0] = gen
                        res = result
                        while 1:
                            isFailure = isinstance(res, failure.Failure)
                            if isFailure:
                                res = res.throwExceptionIntoGenerator(gen)
                            else:
                                res = gen.send(res)
                            if isinstance(res, defer.Deferred):
                                res = _DeferredWrap(res)
                                return res
                            if isinstance(res, types.GeneratorType):
                                res = _GeneratorWrap(res)
                                return res
                    finally:
                        self.session.commit()
                result = yield threads.deferToThreadPool(reactor, self, worker)
                if isinstance(result, _DeferredWrap):
                    d = result.deferred
                    d.callback(None)
                    result = yield d
                elif isinstance(result, _GeneratorWrap):
                    try:
                        result = yield defer.maybeDeferred(self._asyncRunEx, result.generator, None)
                    except:
                        log.err()
                        result = failure.Failure()
            except StopIteration:
                return
            except defer._DefGen_Return as e:
                defer.returnValue(e.value)
            except:
                raise

    def _worker(self):
        self.workerThread = threading.current_thread()
        assert self.session is None
        self.session = self.context.db._createSession()
        try:
            ThreadPool._worker(self)
        finally:
            assert self.session is not None
            self.session.commit()
            self.session.close()

    def _start(self):
        self._start_evt = None
        if not self.running:
            self.start()
            self._stop_evt = reactor.addSystemEventTrigger('during', 'shutdown', self._stop)
            self.running = True

    def _stop(self):
        self._stop_evt = None
        self.stop()
        self.running = False


def getContext(session):
    # :rtype Context
    return session.bind.user_context

class BaseMixin(object):

    created_at = sa.Column('created_at', sa.DateTime, nullable=False)
    updated_at = sa.Column('updated_at', sa.DateTime, nullable=False)

    @staticmethod
    def create_time(mapper, connection, instance):
        instance._context = getContext(object_session(instance))
        now = datetime.datetime.utcnow()
        instance.created_at = now
        instance.updated_at = now
        if isinstance(instance, Pullrequest):
            instance._jsoninfo = json.dumps(instance.info if hasattr(instance, 'info') else {})
        if isinstance(instance, Builder):
            instance._builders = json.dumps(instance.builders)
            b = instance._context.builders[instance.internal_name]
            instance.isPerf = b.get('isPerf', False)

    @staticmethod
    def update_time(mapper, connection, instance):
        instance._context = getContext(object_session(instance))
        now = datetime.datetime.utcnow()
        instance.updated_at = now
        if isinstance(instance, Pullrequest):
            instance._jsoninfo = json.dumps(instance.info if hasattr(instance, 'info') else {})
        if isinstance(instance, Builder):
            instance._builders = json.dumps(instance.builders)

    @staticmethod
    def load(instance, context):
        instance._context = getContext(object_session(instance))
        if isinstance(instance, Pullrequest):
            instance.info = json.loads(instance._jsoninfo) if instance._jsoninfo is not None else {}
        if isinstance(instance, Builder):
            instance.builders = json.loads(instance._builders) if instance._builders is not None else []
            b = instance._context.builders[instance.internal_name]
            instance.isPerf = b.get('isPerf', False)

    @classmethod
    def register(cls):
        sa.event.listen(cls, 'load', cls.load)
        sa.event.listen(cls, 'before_insert', cls.create_time)
        sa.event.listen(cls, 'before_update', cls.update_time)

    def getContext(self):
        ':rtype context.Context'
        return getContext(object_session(self))

    def checkUpdatedTimestamp(self, updated_at):
        if updated_at is not None:
            if isinstance(updated_at, datetime.datetime):
                updated_at = getTimestamp(updated_at)
            if not checkUpdatedAtTimestamp(self, updated_at):
                raise NeedUpdate('Object state was changed')
        return


class Pullrequest(Base, BaseMixin):
    __tablename__ = 'pullrequest'

    prid = sa.Column('id', sa.Integer, primary_key=True)
    branch = sa.Column(sa.String)
    author = sa.Column(sa.String)
    assignee = sa.Column(sa.String)
    head_user = sa.Column(sa.String)
    head_repo = sa.Column(sa.String)
    head_branch = sa.Column(sa.String)
    head_sha = sa.Column(sa.String)
    _jsoninfo = sa.Column('info', sa.String, default='{}', nullable=False)  # JSON string
    title = sa.Column(sa.String)
    description = sa.Column(sa.String)
    priority = sa.Column(sa.Integer, default=0, nullable=False)
    status = sa.Column(sa.Integer, default=0)

    # buildstatus = relationship("Status", order_by="Status.sid", backref="pr")

    def __init__(self, prid):
        self.prid = prid
        self.info = {}

    def __repr__(self):
        return "<PR(%s,...)>" % (self.prid)

    def getBuildStatus(self):
        ss = self._buildstatus
        return ss

    def addBuildStatus(self, status):
        def fn(session):
            ss = self._buildstatus
            assert not status in ss
            ss = [s for s in ss if s.active and s.bid == status.bid]
            if len(ss) > 0:
                logger.info('Leave active PR build status: %s' % repr(status))
                for s in ss:
                    s.active = False
                    logger.info('Deactivate PR build status: %s' % repr(s))
            self._buildstatus.append(status)
            session.commit()
        self.getContext().db.asyncRun(fn)


    @staticmethod
    def query(session):
        return session.query(Pullrequest)

Pullrequest.register()


class Builder(Base, BaseMixin):
    __tablename__ = 'builder'

    bid = sa.Column('id', sa.Integer, primary_key=True)
    internal_name = sa.Column(sa.String, unique=True, nullable=False)
    name = sa.Column(sa.String, nullable=False)
    _builders = sa.Column('builders', sa.String)  # JSON
    builders = []
    order = sa.Column(sa.Integer, default=-1, nullable=False)
    active = sa.Column(sa.BOOLEAN, default=False, nullable=False)

    def __init__(self, internal_name, name, builders, order):
        self.internal_name = internal_name
        self.name = name
        self.builders = builders
        self.order = order

    @staticmethod
    def query(session):
        return session.query(Builder)

    @staticmethod
    def startup(context):
        def fn(session):
            for b in session.query(Builder).all():
                b.active = False
            items = [(internal_name, builder) for internal_name, builder in context.builders.items()]
            items = sorted(items, key=lambda _: _[0])
            for internal_name, builder in items:
                b = session.query(Builder).filter_by(internal_name=internal_name).first()
                if b is None:
                    b = session.query(Builder).filter_by(name=builder['name']).first()
                    if b is None:
                        b = Builder(internal_name, builder['name'], builder['builders'], builder['order'])
                        session.add(b)
                    else:
                        assert not b.active, "Duplicated builder name"
                        b.internal_name = internal_name
                b.active = True
                b.name = builder['name']
                b.builders = builder['builders']
                b.order = builder['order']
            session.commit()
            session.expire_all()
        return context.db.asyncRun(fn)

Builder.register()


class Status(Base, BaseMixin):
    __tablename__ = 'status'

    sid = sa.Column('id', sa.Integer, primary_key=True)
    prid = sa.Column(None, sa.ForeignKey('pullrequest.id'))
    bid = sa.Column(None, sa.ForeignKey('builder.id'))
    head_sha = sa.Column(sa.String)
    brid = sa.Column(sa.Integer)  # build request ID
    build_number = sa.Column(sa.Integer)
    status = sa.Column(sa.Integer, default=BuildStatus.INQUEUE, nullable=False)
    active = sa.Column(sa.BOOLEAN, default=True)

    pr = relationship("Pullrequest", backref=backref('_buildstatus', order_by=bid))  # : :type builder: Pullrequest
    builder = relationship("Builder", backref=None)  # : :type builder: Builder

    @staticmethod
    def query(session):
        return session.query(Status)

    def __repr__(self):
        return "<Status(%s,prid=%s,bid=%s,active=%s,build_number=%s...)>" % (self.sid, self.prid. self.bid, self.active, self.build_number)

Status.register()


class Database():

    def __init__(self, context):
        context.db = self
        self.context = context
        self.engine = sa.create_engine('sqlite:///%s.sqlite' % context.dbname, echo=context.debug_db)
        self.engine.user_context = context

        if context.debug_db:
            self.engine.logger.logger.handlers[0].formatter = logging.Formatter(
                                '%(asctime)s %(thread)d %(levelname)s %(name)s %(message)s')

        from sqlalchemy.orm import sessionmaker
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)

        context.thread = PRDBThread(context)

        self.prcc = PullRequestConnectorComponent(self)
        self.bcc = BuilderConnectorComponent(self)
        self.scc = StatusConnectorComponent(self)

        Base.metadata.create_all(self.engine)

    def _createSession(self):
        # :rtype sqlalchemy.orm.session.Session
        return self.Session()

    def asyncRun(self, fn, *args, **kwargs):
        return self.context.thread.asyncRun(fn, *args, **kwargs)

    def asyncRunEx(self, fn, *args, **kwargs):
        return self.context.thread.asyncRunEx(fn, *args, **kwargs)

class PullRequestConnectorComponent():
    def __init__(self, db):
        self.db = db

    def getPullRequest(self, prid):
        # :rtype: Pullrequest
        def thd(session):
            pr = session.query(Pullrequest).filter_by(prid=prid).first()
            # session.expunge(pr)
            return pr
        return self.db.asyncRun(thd)

    def getActivePullRequests(self):
        def thd(session):
            prs = session.query(Pullrequest).filter(Pullrequest.status >= 0).order_by(Pullrequest.prid.desc()).all()
            # session.expunge_all()
            return prs
        return self.db.asyncRun(thd)

    def insertPullRequest(self, pr):
        def thd(session):
            session.add(pr)
            session.commit()
            # session.expunge(pr)
            return pr
        return self.db.asyncRun(thd)

    def updatePullRequest(self, pr):
        def thd(session):
            _pr = session.merge(pr)
            session.commit()
            # session.expunge(_pr)
            return _pr
        return self.db.asyncRun(thd)

class BuilderConnectorComponent():
    def __init__(self, db):
        self.db = db

    def getBuilder(self, bid):
        def thd(session):
            b = session.query(Builder).filter(Builder.bid == bid).first()
            # session.expunge(b)
            return b
        return self.db.asyncRun(thd)

    def getBuilderByName(self, internal_name):
        def thd(session):
            b = session.query(Builder).filter(Builder.internal_name == internal_name).first()
            # session.expunge(b)
            return b
        return self.db.asyncRun(thd)

    def getActiveBuilders(self):
        def thd(session):
            builders = session.query(Builder).filter(Builder.active == True).order_by(Builder.order.asc()).all()
            # session.expunge_all()
            return builders
        return self.db.asyncRun(thd)

    def insertBuilder(self, b):
        def thd(session):
            _b = session.add(b)
            session.commit()
            # session.expunge(_b)
            return _b
        return self.db.asyncRun(thd)

    def updateBuilder(self, b):
        def thd(session):
            _b = session.merge(b)
            session.commit()
            # session.expunge(_b)
            return _b
        return self.db.asyncRun(thd)

class StatusConnectorComponent():
    def __init__(self, db):
        self.db = db

    def getStatus(self, prid, bid):
        def thd(session):
            s = session.query(Status).filter(Status.active == True).filter(Status.prid == prid).filter(Status.bid == bid).first()
            return s
        return self.db.asyncRun(thd)

    def getStatusForBuildRequest(self, prid, bid, brid):
        def thd(session):
            s = session.query(Status).filter(Status.prid == prid).filter(Status.bid == bid).filter(Status.brid == brid).first()
            return s
        return self.db.asyncRun(thd)

    def getStatusForBuildNumber(self, prid, bid, build_number):
        def thd(session):
            s = session.query(Status).filter(Status.prid == prid).filter(Status.bid == bid).filter(Status.build_number == build_number).first()
            return s
        return self.db.asyncRun(thd)

    def insertStatus(self, s):
        def thd(session):
            _s = session.add(s)
            session.commit()
            return _s
        return self.db.asyncRun(thd)

    def updateStatus(self, s):
        def thd(session):
            _s = session.merge(s)
            session.commit()
            return _s
        return self.db.asyncRun(thd)

    def deleteStatus(self, s):
        def thd(session):
            _s = session.merge(s)
            session.delete(_s)
            session.commit()
            return
        return self.db.asyncRun(thd)

    def getStatusesForPullRequest(self, prid):
        def thd(session):
            ss = session.query(Status).filter(Status.active == True).filter(Status.prid == prid).all()
            return ss
        return self.db.asyncRun(thd)

    def getAllActiveStatuses(self):
        def thd(session):
            ss = session.query(Status).filter(Status.active == True).all()
            return ss
        return self.db.asyncRun(thd)

    def getStatusToSchedule(self, bid):
        def thd(session):
            s_pr = session.query(Status, Pullrequest) \
                    .filter(Status.active == True) \
                    .filter(Status.status == constants.BuildStatus.INQUEUE) \
                    .filter(Status.bid == bid) \
                    .order_by(Pullrequest.priority) \
                    .order_by(Pullrequest.prid) \
                    .first()
            return s_pr[0] if s_pr is not None else None  # returns status
        return self.db.asyncRun(thd)

def mainThreadCall(fn, *args, **kwargs):
    d = defer.Deferred()
    def callback(_):
        return fn(*args, **kwargs)
    d.addCallback(callback)
    return d

def DBMethodCall(fn):
    def wrap(*args, **kwargs):
        self = args[0]
        if hasattr(self, 'db'):
            assert self.db.context.thread.workerThread == threading.current_thread()
        if hasattr(self, 'context'):
            assert self.context.thread.workerThread == threading.current_thread()
        return fn(*args, **kwargs)
    return wrap

if __name__ == '__main__':
    # Run database tests

    logging.basicConfig(stream=sys.stdout,
                        level=logging.DEBUG,
                        format='%(asctime)s %(thread)d %(levelname)s %(name)s %(message)s')

    class TestContext():
        dbname = 'test'
        debug_db = False
        builders = dict(runtests1=dict(name='t1', builders=['runtests1'], order=0),
                        runtests2=dict(name='t2', builders=['runtests2'], order=1),
                        runtests3=dict(name='t3', builders=['runtests3'], order=2),
                        runtests4=dict(name='t4', builders=['runtests4'], order=3),
                        runtests5=dict(name='optional', builders=['runtests5'], order=100, isPerf=True))

    ctx = TestContext()
    db = Database(ctx)

    session = db._createSession()

    pr = session.query(Pullrequest).filter_by(prid=11).first()
    if pr is None:
        pr = Pullrequest(11)
        session.add(pr)
        session.commit()

    pprint.pprint(pr.info)

    pr.author = 'me'
    pr.info['extra'] = True

    pprint.pprint(pr)

    session.commit()
    session.close()

    pprint.pprint(pr)

    session = db._createSession()
    pr2 = session.query(Pullrequest).filter_by(prid=11).first()

    pr.assignee = 'me2'

    pr3 = session.merge(pr)

    print pr2 is pr
    pprint.pprint(pr2)

    session.commit()
    session.close()

    pprint.pprint(pr)
    pprint.pprint(pr2)
    pprint.pprint(pr3)

    print threading.current_thread()
    @defer.inlineCallbacks
    def main_thread():
        yield None
        print threading.current_thread()
        defer.returnValue("?")

    class TestObj():
        def __init__(self, context):
            self.db = context.db

        @DBMethodCall
        def test2(self, arg):
            print "test2", arg
            res = yield mainThreadCall(main_thread)

        @DBMethodCall
        def test(self, arg):
            print "test", arg
            res = yield mainThreadCall(main_thread)
            yield self.test2(res)

    o = TestObj(ctx)

    @defer.inlineCallbacks
    def fn():
        try:
            yield Builder.startup(ctx)

            def throwfn():
                yield None
                raise Exception('test exception2')

            def fn(session):
                try:
                    raise Exception('test exception')
                except Exception as e:
                    print(e)
                try:
                    yield throwfn()
                except Exception as e:
                    print(e)

                b = Builder.query(session).filter(Builder.internal_name == 'runtests1').first()  # : :type b: Builder
                pr = db.prcc.getPullRequest(11)
                pr.assignee = 'test'
                pr = db.prcc.updatePullRequest(pr)
                pr.assignee = 'test2'
                pr = db.prcc.updatePullRequest(pr)
                pprint.pprint(pr.getBuildStatus())
                s = Status()
                s.bid = b.bid
                res = yield mainThreadCall(main_thread)
                print res
                pr.addBuildStatus(s)
                session.commit()
                s = db.scc.getStatusToSchedule(b.bid)
                pprint.pprint(s)
                # print(s.sid, s.pr.prid, s.builder.name)
                defer.returnValue(pr)
            pr = yield db.asyncRun(fn)
            pr.assignee = 'test3'
            pr.assignee = 'test4'
            pr = yield db.prcc.updatePullRequest(pr)
            pprint.pprint(pr.getBuildStatus())

            r = yield db.asyncRun(o.test)
            print r
        except:
            log.err()
        finally:
            reactor.stop()

    fn()
    reactor.run()
