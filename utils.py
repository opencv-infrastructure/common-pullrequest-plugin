
from buildbot.util import json


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
