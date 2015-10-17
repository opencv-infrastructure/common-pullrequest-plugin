from github import GitHub

if False:
    gh = GitHub("Test")
    res = gh.users('octocats').get()
    print res
else:
    from twisted.internet import threads, reactor, defer
    gh = GitHub("Test", async=True)
    @defer.inlineCallbacks
    def f():
        print "Async"
        res = yield gh.users('octocats').get()
        print res
        reactor.stop()
    f()
    reactor.run()
