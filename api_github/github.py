#!/usr/bin/env python

'''
Client for GitHub API v3
'''

import os, json, urllib, urllib2
from urlparse import urlparse

from twisted.web.client import Agent, ProxyAgent, readBody
from twisted.internet import defer, reactor
from twisted.internet.endpoints import TCP4ClientEndpoint
from twisted.python import log, failure
from twisted.web.http_headers import Headers
from twisted.web.iweb import IBodyProducer
from zope.interface.declarations import implements

from twisted_connect import HTTPProxyConnector

GITHUB_URL = 'https://api.github.com'
HTTPS_CONNECT=True
TIMEOUT = 60

# Exception base class
class Error(Exception):

    def __init__(self, url, request, response):
        super(Error, self).__init__(url)
        self.request = request
        self.response = response

# 404 Exception
class ErrorNotFound(Error):
    pass

def getAgent(reactor):
    http_proxy = os.environ.get('http_proxy', None)
    if http_proxy:
        c = urlparse(http_proxy)
        if HTTPS_CONNECT:
            proxy = HTTPProxyConnector(proxy_host=c.hostname, proxy_port=c.port)
            agent = Agent(reactor=proxy)
        else:
            endpoint = TCP4ClientEndpoint(reactor, c.hostname, c.port)
            agent = ProxyAgent(endpoint)
        return agent
    return Agent(reactor)


class GitHub(object):

    status = 0
    x_ratelimit_remaining = -1
    x_ratelimit_limit = -1

    def __init__(self, userAgent, access_token=None, async=False, reuseETag=False):
        self.userAgent = userAgent
        self.ETag = None
        self._authorization = 'token %s' % access_token if access_token else None
        self._async = async
        self._reuseETag = reuseETag

    def _process(self, method, path, **kw):
        # prepare HTTP request input parameters
        url_params = None
        http_body = None
        if method == 'GET' and kw:
            args = []
            for key, value in kw.iteritems():
                args.append('%s=%s' % (key, urllib.quote(str(value))))
            url_params = '&'.join(args)
        if method in ['POST', 'PATCH', 'PUT']:
            http_body = json.dumps(kw)
        url = '%s%s%s' % (GITHUB_URL, path, '' if url_params is None else '?' + url_params)

        def _parse_headers(self, headers):
            isValid = False
            for k in headers:
                h = k.lower()
                if h == 'status':
                    self.status = int(headers[k].split(' ')[0])
                elif h == 'content-type':
                    isValid = headers[k].startswith('application/json')
                elif h == 'etag':
                    self.ETag = headers[k]
                elif h == 'x-ratelimit-remaining':
                    self.x_ratelimit_remaining = int(headers[k])
                elif h == 'x-ratelimit-limit':
                    self.x_ratelimit_limit = int(headers[k])
            return isValid

        if not self._async:
            # process synchronous call
            request = urllib2.Request(url, data=http_body)
            request.get_method = lambda: method
            request.add_header('User-Agent', self.userAgent)
            if self._authorization:
                request.add_header('Authorization', self._authorization)
            if self._reuseETag and self.ETag:
                request.add_header('If-None-Match', self.ETag)
            if method in ['POST', 'PATCH', 'PUT']:
                request.add_header('Content-Type', 'application/x-www-form-urlencoded')
            try:
                response = urllib2.build_opener(urllib2.HTTPSHandler).open(request, timeout=TIMEOUT)
                isValid = _parse_headers(self, response.headers)
                if isValid:
                    return json.loads(response.read())
            except urllib2.HTTPError, e:
                isValid = _parse_headers(self, e.headers)
                if isValid:
                    json_data = json.loads(e.read())
                req = dict(method=method, url=url)
                resp = dict(code=e.code, json=json_data)
                if resp['code'] == 404:
                    raise ErrorNotFound(url, req, resp)
                raise Error(url, req, resp)
        else:
            # process asynchronous calls (Twisted)
            if method in ['GET', 'DELETE']:
                @defer.inlineCallbacks
                def asyncGet():
                    agent = getAgent(reactor)
                    headers = {'User-Agent':[self.userAgent]}
                    if self._authorization:
                        headers['Authorization'] = [self._authorization]
                    if self._reuseETag and self.ETag and method == 'GET':
                        headers['If-None-Match'] = [self.ETag]
                    response = yield agent.request(method, url, headers=Headers(headers))
                    self.status = response.code
                    resp_headers = {}
                    for k in response.headers._rawHeaders:
                        resp_headers[k] = response.headers._rawHeaders[k][0];
                    isValid = _parse_headers(self, resp_headers)
                    if isValid:
                        body = yield readBody(response)
                        defer.returnValue(json.loads(body))
                    defer.returnValue(None)
                return asyncGet()
            if method in ['POST', 'PATCH', 'PUT']:
                @defer.inlineCallbacks
                def asyncPost():
                    agent = getAgent(reactor)
                    headers = {'User-Agent':[self.userAgent]}
                    if self._authorization:
                        headers['Authorization'] = [self._authorization]
                    class StringProducer(object):
                        implements(IBodyProducer)
                        def __init__(self):
                            self.length = len(http_body)
                        def startProducing(self, consumer):
                            consumer.write(http_body)
                            return defer.succeed(None)
                        def stopProducing(self):
                            pass
                        def pauseProducing(self):
                            pass
                        def resumeProducing(self):
                            pass
                    response = yield agent.request(method, url, headers=Headers(headers), bodyProducer=StringProducer() if http_body else None)
                    resp_headers = {}
                    for k in response.headers._rawHeaders:
                        resp_headers[k] = response.headers._rawHeaders[k][0];
                    isValid = _parse_headers(self, resp_headers)
                    if isValid:
                        body = yield readBody(response)
                        defer.returnValue(json.loads(body))
                    defer.returnValue(None)
                return asyncPost()

    '''
    Helper classes for smart path processing
    '''
    def __getattr__(self, attr):
        return self._Entry(self, '/%s' % attr)

    class _EndPoint(object):

        def __init__(self, client, path, method):
            self._client = client
            self._path = path
            self._method = method

        def __call__(self, **kw):
            return self._client._process(self._method, self._path, **kw)

    class _Entry(object):

        def __init__(self, client, path):
            self._client = client
            self._path = path

        def __getattr__(self, attr):
            if attr in ['get', 'put', 'post', 'patch', 'delete']:
                return self._client._EndPoint(self._client, self._path, attr.upper())
            name = '%s/%s' % (self._path, attr)
            return self._client._Entry(self._client, name)

        def __call__(self, *args):
            if len(args) == 0:
                return self
            name = '%s/%s' % (self._path, '/'.join([str(arg) for arg in args]))
            return self._client._Entry(self._client, name)


from pprint import pprint

'''
Commit status updater
'''
class GitHubCommitStatus(object):

    def __init__(self, client, username, repo, context='default'):
        self.client = client
        self.username = username
        self.repo = repo
        self.context = context

    @defer.inlineCallbacks
    def updateCommit(self, sha, state, description, url, context=None):
        if context is None:
            context = self.context
        result = None
        try:
            need_update = True
            try:
                current_statuses = yield self.client.repos(self.username)(self.repo).statuses(sha).get()
                if self.client.status == 200:
                    for s in current_statuses:
                        if s['context'] == context:
                            if s['state'] == state and s['description'] == description and s['target_url'] == url:
                                log.msg('Commit status update for %s is NOT required' % sha)
                                return
                            break
                    log.msg('Commit status update for %s is needed' % sha)
            except:
                log.err(failure.Failure(), 'while receiving old commit status')
                pass

            result = yield self.client.repos(self.username)(self.repo).statuses(sha).post(state=state, target_url=url, description=description, context=context)
            log.msg('Commit status for %s is updated to "%s":"%s"' % (sha, state, description))
        except:
            log.err(failure.Failure(), 'while updating commit status')
            pass
        defer.returnValue(result)
