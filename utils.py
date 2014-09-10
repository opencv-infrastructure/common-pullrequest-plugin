
import json


class NotFound(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)

class Forbidden(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)

class Conflict(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)

class NeedUpdate(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)

class BadRequest(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)

def RequestArg(request, arg, default):
    na = {}
    v = request.args.get(arg, na)
    if v is not na:
        return v[0]
    dataStr = request.content.getvalue()
    try:
        data = json.loads(dataStr)
    except:
        data = None
    if data and isinstance(data, dict):
        v = data.get(arg, na)
        if v is not na:
            return v
    return default



from os.path import os, stat
import collections
import json
import time
import urllib2

def _sig(fileName):
    st = os.stat(fileName)
    return (stat.S_IFMT(st.st_mode),
            st.st_size,
            st.st_mtime)

def cacheFileAccess(fn):
    cache = {}
    def decorator(fileName):
        signature = _sig(fileName)
        if not fileName in cache:
            cache[fileName] = dict(signature=None, result=None)
        e = cache[fileName]
        if e['signature'] != signature:
            e['result'] = fn(fileName)
            e['signature'] = signature
        return e['result']
    return decorator


class CacheFunction(object):
    def __init__(self, ttl, initialCleanupThreshold=64):
        self.ttl = ttl
        self.cache = {}
        self.initialCleanupThreshold = initialCleanupThreshold
        self.cleanupThreshold = initialCleanupThreshold

    def __call__(self, fn):
        self.fn = fn
        def decorator(*args):
            if not isinstance(args, collections.Hashable):
                raise Exception("uncacheable parameters")
                # return self.fn(*args)
            now = time.time()
            try:
                value, last_update = self.cache[args]
                if self.ttl > 0 and now - last_update > self.ttl:
                    raise AttributeError
            except (KeyError, AttributeError):
                if len(self.cache) >= self.cleanupThreshold:
                    self.cache = {k: v for (k, v) in self.cache.items() if now - v[1] <= self.ttl}
                    if len(self.cache) >= self.cleanupThreshold:
                        self.cleanupThreshold = self.cleanupThreshold * 2
                    elif len(self.cache) < self.cleanupThreshold / 2:
                        self.cleanupThreshold = max(self.cleanupThreshold / 2, self.initialCleanupThreshold)
                value = self.fn(*args)
                self.cache[args] = (value, now)
            return value
        return decorator


import json, urllib, urllib2

from twisted.web.client import Agent, readBody  
from twisted.internet import defer, reactor
from twisted.python import log, failure
from twisted.web.http_headers import Headers
from twisted.web.iweb import IBodyProducer
from zope.interface.declarations import implements

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

class JSONClient(object):

    status = 0

    def __init__(self, url, userAgent = None, async=True):
        self._url = url
        self.userAgent = userAgent
        self._async = async

    def _process(self, method, path, **kw):
        url_params = None
        http_body = None 
        if method == 'GET' and kw:
            args = []
            for key, value in kw.iteritems():
                args.append('%s=%s' % (key, urllib.quote(str(value))))
            url_params = '&'.join(args)
        if method in ['POST', 'PATCH', 'PUT']:
            http_body = json.dumps(kw)
        url = '%s%s%s' % (self._url, path, '' if url_params is None else '?' + url_params)

        def _parse_headers(self, headers):
            isValid = False
            for k in headers:
                h = k.lower()
                if h == 'status':
                    self.status = int(headers[k].split(' ')[0])
                elif h == 'content-type':
                    isValid = headers[k].startswith('application/json')
            return isValid

        if not self._async:
            # process synchronous call
            request = urllib2.Request(url, data=http_body)
            request.get_method = lambda: method
            if self.userAgent is not None:
                request.add_header('User-Agent', self.userAgent)
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
                    agent = Agent(reactor)
                    if self.userAgent:
                        headers = {}
                    else:
                        headers = {'User-Agent':[self.userAgent]}
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
                    agent = Agent(reactor)
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
