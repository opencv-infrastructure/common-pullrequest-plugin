#!/usr/bin/env python

'''
Client for GitLab API v3
'''

import json, urllib, urllib2

from twisted.web.client import Agent, readBody
from twisted.internet import defer, reactor
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

class GitLab(object):

    status = 0
    x_ratelimit_remaining = -1
    x_ratelimit_limit = -1

    def __init__(self, apiURL, userAgent, private_token, async=False):
        self._apiUrl = apiURL;
        self.userAgent = userAgent
        self._private_token = private_token
        self._async = async

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
        url = '%s%s%s' % (self._apiUrl, path, '' if url_params is None else '?' + url_params)

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
            request.add_header('User-Agent', self.userAgent)
            request.add_header('PRIVATE-TOKEN', self._private_token)
            if method in ['POST', 'PATCH', 'PUT']:
                request.add_header('Content-Type', 'application/x-www-form-urlencoded')
            try:
                response = urllib2.build_opener(urllib2.HTTPHandler, urllib2.HTTPSHandler).open(request, timeout=TIMEOUT)
                isValid = self._parse_headers(response.headers)
                if isValid:
                    return json.loads(response.read())
            except urllib2.HTTPError, e:
                isValid = self._parse_headers(e.headers)
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
                    headers = {'User-Agent':[self.userAgent],
                               'PRIVATE-TOKEN':[self._private_token]}
                    response = yield agent.request(method, url, headers=Headers(headers))
                    self.status = response.code
                    resp_headers = {}
                    for k in response.headers._rawHeaders:
                        resp_headers[k] = response.headers._rawHeaders[k][0];
                    isValid = self._parse_headers(resp_headers)
                    if isValid:
                        body = yield readBody(response)
                        defer.returnValue(json.loads(body))
                    defer.returnValue(None)
                return asyncGet()
            if method in ['POST', 'PATCH', 'PUT']:
                @defer.inlineCallbacks
                def asyncPost():
                    agent = Agent(reactor)
                    headers = {'User-Agent':[self.userAgent],
                               'PRIVATE-TOKEN':[self._private_token],
                               'Content-Type': ['application/json']}
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
                    isValid = self._parse_headers(resp_headers)
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
