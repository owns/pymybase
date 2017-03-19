"""
Elias Wood (owns13927@yahoo.com)
2015-04-06
a base class for all APIs.  I have several...
"""

import time
import datetime
import requests
# stop lower level logging from requests
import logging
logging.getLogger('requests').setLevel(logging.WARNING)

from myloggingbase import MyLoggingBase

class MyAPIBase(MyLoggingBase):
    """
    this 'abstract' base class is only
    helpful in simplifying API things.
    Assumed authentication is by a token.  Override set_auth and make sure to
        add whatever class variables needed.  And set in init!
    NOTE: to get an idea of how to use, view some of the API classes I've built.
          There are a few key methods each API will need to override.
          
    CHANGELOG:
    2016-07-05 v0.2.2: Elias Wood
        _make_api_call: exceptions are not all critical - just track #
    2016-07-05 v0.2.1: Elias Wood
        MyError (abstract exception class) has response param to hold http(s) repsonse
    2016-06-21 v0.2.0: Elias Wood
        extracted_dt is now a datetime instead of a str!
    2016-06-21 v0.1.4: Elias Wood
        bug fix: .reset_session() wan't applying mounts set previously
    2016-06-17 v0.1.3: Elias Wood
        bug fix for .get(...)
    2016-06-17 v0.1.2: Elias Wood
        fixed is_waiting() to not return negative time
        added mount(prefix,adapter) and unmount(prefix)
        little code fixes (most just for looks) 
    2016-05-31 v0.1.1: Elias Wood
        _try_api_call also returns the request object to handle status_code
        added is_waiting fn to tell how long API is waiting for (if at all)
    2016-05-27 v0.1.0: Elias Wood
        First Version!!!
        
    """
    __version__ = '0.2.2'
    #===========================================================================
    # Variables
    #===========================================================================
    _session = None
    __mounts = None
    __proxies = None
    __verify_requests = None
    __cert_requests = None
    debug_api_calls = None
    __waiting_till = None
    
    #===========================================================================
    # Static Variables
    #===========================================================================
    DEFAULT_PROTOCOLS = ('ftp://','http://','sftp://','https://')
    BASE_URL = None #BASE_URL should NOT end with /
    DEFAULT_WAIT_DURATION = 900 # 15 minutes (from Twitter...)
    RETRY_CONNECTION_TIME = 30 # wait time if connection issue.
    MAX_TRY_COUNT = 6 # make times to re-try a request
    #BATCH_LIMITS = dict()
    #BATCH_LIMIT_DEFAULT = 100
    
    #===========================================================================
    # Tracked Metrics
    #===========================================================================
    __item_counts = None # use to track how many items pulled
    __requests_get_count = None # num of Session.get calls
    __api_request_count = None # num of _make_api_call calls
    __requests_error_count = None # num of Sessions.get fails and API errors
    __start_time = None # when session open
    __end_time = None # when session closed (to get open time)
    
    
    
    def __init__(self,*args,**keys):
        """This method should be overridden to set anything
        specific for the API.  Proxy is set. Remember
        to call self.reset_session() at the end to init the session"""
        super(MyAPIBase, self).__init__(*args,**keys)
        self.debug_api_calls = False
        self.__mounts = dict()
        self.__waiting_till = 0
        
        # set proxy if provided
        self.__proxies = dict()
        if 'proxies' in keys:
            if isinstance(keys['proxies'],dict):
                self.set_proxies(keys['proxies'])
            else: self.logger.warning('proxies must be a dictionary, e.g. {"http": "http://user:pass@my.proxy.com:123"}')
        
        #super(<MY CLASS HERE>, self).__init__(*args,**keys)
        # start the connection session (to be called in in override fn)
        #self.reset_session()
    
    #===========================================================================
    # Session
    #===========================================================================
    def reset_session(self,log_stats=False):
        """closes the current session (if one is already up)
        log_stats: will call log_summary_info(...)
                   before reseting the stats for the new session
        """
        if self.session_is_open():
            self.close_session()
            # log stats if asked
            if log_stats: self.log_summary_info()
        
        # open a new session
        self._session = requests.Session()
        self.__start_time = time.clock()
        self.__end_time = None
        self.set_auth()
        self._session.proxies = self.__proxies
        if self.__verify_requests is not None:
            self._session.verify = self.__verify_requests
        if self.__cert_requests is not None:
            self._session.cert = self.__cert_requests
        for prefix,adapter in self.__mounts.iteritems():
            self._session.mount(prefix,adapter)
        
        # set metrics start
        self.__item_counts = {}
        self.__requests_get_count = 0
        self.__api_request_count = 0
        self.__requests_error_count = 0
        
    def session_is_open(self):
        """ determines if the session is open (not None) """
        return isinstance(self._session, requests.Session)
    
    def close_session(self):
        """closes session if open"""
        if self.session_is_open():
            self.__end_time = time.clock()
            self._session.close()
            self._session = None
    
    def close(self,*args,**keys):
        """closes session & anything else...
        Override if the API requires additional
        things checked-out, etc"""
        self.close_session()
    
    def get(self,*args,**keys):
        """wrapper around session get to track
        get requests and errors."""
        r = None
        try:
            self.__requests_get_count += 1
            r = self._session.get(*args,**keys)
        except Exception as e:
            self.__requests_error_count += 1
            raise e
        else: return r
    
    #===========================================================================
    # Getters / Setters for requests
    #===========================================================================
    def _add_to_count(self,item_type,n=1):
        """add to metric counting of items pulled. Use in your custom
        pull functions to track how many of each type of time pulled."""
        try: self.__item_counts[item_type] += n
        except KeyError: self.__item_counts[item_type] = n
    
    #def get_proxies(self):
    #    """returns a copy of the proxies being used by the session"""
    #    return self.__proxies.copy()
    def update_proxies(self,proxies):
        """updates the proxies used by the session"""
        self.__proxies.update(proxies)
        if self.session_is_open(): self._session.proxies = self.__proxies
    def set_proxies(self,proxies):
        """set the proxies used by the session"""
        self.__proxies = proxies
        if self.session_is_open(): self._session.proxies = self.__proxies
        
    def get_verify_requests(self):
        """returns what the verify flag is set to"""
        return self.__verify_requests
    def set_verify_requests(self,verify=True):
        """sets the verify flag for the requests Sessions instance"""
        self.__verify_requests = verify
        if self.session_is_open(): self._session.verify = verify
    
    def get_cert_requests(self):
        """returns what the verify flag is set to"""
        return self.__cert_requests
    def set_cert_requests(self,cert=True):
        """sets the verify flag for the requests Sessions instance"""
        self.__cert_requests = cert
        if self.session_is_open(): self._session.cert = cert
    
    def mount(self,prefix,adapter):
        """Registers a connection adapter to a prefix"""
        self.__mounts[prefix] = adapter
        if self.session_is_open(): self._session.mount(prefix,adapter)
    def unmount(self,prefix):
        """Removes the mount, but requires .reset_session()
        to remove it from the session if .session_is_open().
        Returns True if an adapter was removed"""
        if prefix in self.__mounts:
            del self.__mounts[prefix]
            return True
        else: return False
    
    def is_waiting(self):
        """returns how long the api is currently waiting for...
        """
        w = self.__waiting_till-time.time() if self.__waiting_till else 0
        return (w) if w>0 else 0
    
    #===========================================================================
    # Getters / Setters for requests
    #===========================================================================
    def set_auth(self,*args,**keys):
        """Override this method to implement the
        authentication process. Assumes session is open.
        No params are passed when called from reset_session()."""
        #super(<MY CLASS HERE>, self).set_auth(*args,**keys)
        pass
    ''    
    #===========================================================================
    # Prep for API call
    #===========================================================================
    def default_url_append(self,path,params):
        """override if your url just requires some addition to the path
        if it doesn't start with /"""
        return '/'
    
    def _format_call_params(self,path,params):
        """override to format the url or params in a very custom way...
        Default removed None value keys from params.  and returns."""
        # fix path if needed
        if any(path.startswith(i) for i in self.DEFAULT_PROTOCOLS):
            # if it's already got a protocol,
            # assume already formatted
            url = path
        else:
            url = (self.BASE_URL+('' if path.startswith('/')
                 else self.default_url_append(path,params))+path)
        
        # remove None params if exist
        for k in tuple(k for k,v in params.iteritems() if v is None):
            del params[k]
        
        return url
    
    def before_api_call(self,try_count,url,params):
        """this method is called before the API call is made"""
        pass
    ''
    #===========================================================================
    # Parse the API response
    #===========================================================================
    def _api_parse(self,r):
        """if JSON parsing failed - raising requests.ConnectionError
        Override to parse JSON specific errors."""
        try: j = r.json()
        except Exception as e:          # TO JSON ERROR?
            j = {}
            self.logger.warn('request error - %s: %r',e.__class__.__name__,e)
            # will wait and try again
            raise requests.ConnectionError(
                'Error converting server reponse to JSON...')
        else: return j
    ''
    #===========================================================================
    # Make the API call!!! 
    #===========================================================================
    def after_api_call(self,*args,**keys):
        """this method is called right after the API call is
        made. It will be skipped if a requests error is raised."""
        pass
    
    def _try_api_call(self,url,params,try_count):
        """This function should be overridden to handle (capture)
        API specific errors. returns (extracted_dt,JSON body data,request obj).
        If JSON obj is None, continues trying."""
        if try_count > self.MAX_TRY_COUNT: raise MaxRetryLimit(
                'max retry limit of %s reached' % (self.MAX_TRY_COUNT,))
            
        # make the call
        self.__requests_get_count += 1
        # debug API calls if requested...
        if self.debug_api_calls:
            self.logger.debug('%03d:%s?%s',self.__api_request_count,
                              url,'&'.join('{}={}'.format(k,v)
                                           for k,v in params.iteritems()))
        
        r = self._session.get(url,params=params)
        
        self.after_api_call(try_count,url,params,r) # TO BE overridden fn
    
        # parse timestamp
        extracted_dt = datetime.datetime.strptime(r.headers['date'],
                                                  '%a, %d %b %Y %H:%M:%S %Z')
        
        # did we hit the rate limit 'Too Many Requests'?
        if r.status_code == 429:
            raise TooManyRequests('HTTP 429: Too Many Requests',code=429,
                                  response=r)
        
        # error handling...
        #if r.status_code == 200: pass
        #else: pass
        
        # parse to json and handle errors
        j = self._api_parse(r)
        
        return extracted_dt,j,r # all done
    
    def _make_api_call(self,path,**params):
        """This method should not be overridden. It has basic
        retry logic for connection fails, etc. If more advanced, API
        specific error handling is needed, look at _try_api_call(...).
        If some code needs to be ran before/after the API call, use
        before_api_call(...) or after_api_call(...)"""
        
        url = self._format_call_params(path,params)
        
        self.__api_request_count += 1
        
        try_count = 0
        while True:
            try_count += 1
            self.before_api_call(try_count, url, params) #TO BE overridden fn
            
            extracted_dt = j = None
            
            try:
                extracted_dt,j,r = self._try_api_call(url,params,try_count) #@UnusedVariable
            #=======================================================================
            # Error Handling
            #=======================================================================
            # if it's a connection issue, wait some and try again.
            except (requests.exceptions.ChunkedEncodingError,
                    requests.ConnectionError) as e:
                self.__requests_error_count += 1
                #RETRY_CONNECTION_TIME
                self.logger.warn('connection error - waiting %ds. %r',
                                    self.RETRY_CONNECTION_TIME,e)
                
                self.reset_session(log_stats=True)
                self.wait_to_retry(try_count,self.RETRY_CONNECTION_TIME)
            
            except (requests.RequestException,requests.Timeout,
                    requests.URLRequired,requests.TooManyRedirects,
                    requests.HTTPError) as e:
                self.__requests_error_count += 1
                #self.logger.critical('error: %r',e)
                raise e
            
            except Exception as e:
                self.__requests_error_count += 1
                #self.logger.critical('error: %r',e)
                raise e
            
            else:
                if j is not None: break # if all good, break!
            
        return extracted_dt, j
    
    def make_api_call(self,path,**params):
        """This method must be overridden.  It should do any
        post-processing of the data and normally append the
        extracted_dt to each object.
        """
        extracted_dt , j  = self._make_api_call(path,**params) # @UnusedVariable
        return j # default is to ignore the extracted_dt
    ''
    #===========================================================================
    # for paging/batching APIs, override the below functions
    #===========================================================================
    def iter_api_call(self,path,**params):
        """A generator around iter_api_call_pages(...). It 
        iterates each item in each page, for convenience."""
        for page in self.iter_api_call_pages(path,**params):
            for item in page: yield item

    def iter_api_call_pages(self,path,**params):
        """Override: a generator around make_api_call - it iterates each page and
        simple continues to page the API (implementation depends on API)!
        Use page_limit param to limit the items per page."""
        pass
    
    def make_batch_api_call(self,*args,**params):
        """Override this function to wrap batch requests."""
        for page in self.make_batch_api_call_pages(*args,**params):
            for item in page: yield item
                
    def make_batch_api_call_pages(self,path='',**params):
        """Override: to be used to page through batch calls. Most APIs
        batching have a limit to how many you can batch at one time.
        Override this method to abstract that paging. Use the batch_limit
        param if want to use less than the batch limit.
        Use static class vars BATCH_LIMITS and BATCH_LIMIT_DEFAULT = 100
        """
        pass
    ''
    #===========================================================================
    # Generic waiting functions - using if connection issue
    #===========================================================================
    def wait_to_retry(self,try_count,t=None):
        """Called whenever the API needs to wait, usually b/c
        a connection issue or a rate limit was reached."""
        if t is None: t = self.DEFAULT_WAIT_DURATION
        self.logger.debug('waiting {} seconds (attempt {}/{})'.format(
                           t,try_count,self.MAX_TRY_COUNT))
        self.before_sleeping(try_count,t)
        time.sleep(t)
        self.after_sleeping(try_count,t)
        
    def before_sleeping(self,try_count,t):
        """Called in self.wait_to_retry(...) before sleeping """
        self.__waiting_till = time.time()+t
    
    def after_sleeping(self,try_count,t):
        """Called in self.wait_to_retry(...) after sleeping """
        self.__waiting_till = 0
    ''
    #===========================================================================
    # get info summary (override to add more!
    #===========================================================================
    def get_summary_info(self):
        """Add count of API calls, errors, and session open time."""
        a = super(MyAPIBase, self).get_summary_info() 
        a.extend(('get requests: {:,}'.format(self.__requests_get_count),
                  'api requests: {:,}'.format(self.__api_request_count),
                  'err requests: {:,}'.format(self.__requests_error_count),
                  'session open: {:,} min'.format(((self.__end_time if self.__end_time
                                                    else time.clock()) - self.__start_time
                                                   )/60.0 if self.__start_time is not None
                                                  else -1)
                ))
        # add item types pulled (if any)
        a.extend(['{} pulled: {:,}'.format(k,v)
                  for k,v in self.__item_counts.iteritems()])
        return a
''
#===============================================================================
# ERRORS that may be raised
#===============================================================================
class MyError(Exception):
    """ Base error class to capture an optional arguemnt: code and response"""
    code = None
    response = None
    def __init__(self,*args,**keys):
        self.code = keys.pop('code',None)
        self.response = keys.pop('response',None)
        Exception.__init__(self,*args,**keys)

class MaxRetryLimit(MyError):
    """ If re-try count for requests reached """

class TooManyRequests(MyError):
    """ HTTP error 429 - Too Many Requests """
    
class InvalidAuth(MyError):
    """ raise if auth failed has issues """

class ServiceUnavailable(MyError):
    """ Originally from Twitter: Service unavailable due to request timeout;
    please try the request again later """
    
class FeatureNotAvailable(MyError):
    """ Originally from Twitter: The account does not have the feature
    REACH_AND_FREQUENCY_ANALYTICS """

class InvalidRequest(MyError):
    """ First used in InContact: api request failed """
