import time
import connection

from scrapy.dupefilters import BaseDupeFilter
from scrapy.utils.request import request_fingerprint
import pyreBloom

class RFPDupeFilter(BaseDupeFilter):
    """Redis-based request duplication filter"""

    def __init__(self, server, key):
        """Initialize duplication filter

        Parameters
        ----------
        server : Redis instance
        key : str
            Where to store fingerprints
        """
        self.server = server
        self.key = key

    @classmethod
    def from_settings(cls, settings):
        server = connection.from_settings(settings)
        # create one-time key. needed to support to use this
        # class as standalone dupefilter with scrapy's default scheduler
        # if scrapy passes spider on open() method this wouldn't be needed
        key = "dupefilter:%s" % int(time.time())
        return cls(server, key)

    @classmethod
    def from_crawler(cls, crawler):
        return cls.from_settings(crawler.settings)

    def request_seen(self, request):
        fp = request_fingerprint(request)
        added = self.server.sadd(self.key, fp)
        return not added

    def close(self, reason):
        """Delete data on close. Called by scrapy's scheduler"""
        self.clear()

    def clear(self):
        """Clears fingerprints data"""
        self.server.delete(self.key)
 
class RedisBloomDupeFilter(BaseDupeFilter):
    """Redis backed bloomfilter duplication filter"""
 
    def __init__(self, server, key):
        self.filter = pyreBloom.pyreBloom(key, 10000000, 0.00001)
 
    @classmethod
    def from_settings(cls, settings):
        server = connection.from_settings(settings)
        key = "dupefilter:bloom:%s" % int(time.time())
        return cls(server, key)

    @classmethod
    def from_crawler(cls, crawler):
        return cls.from_settings(crawler.settings)
 
    def request_seen(self, request):
        fp = request.url
        if fp in self.filter:
            return True
        self.filter.add(fp)
 
    def close(self, reason):
        pass

