from __future__ import unicode_literals, absolute_import, print_function
import re
import six
import time
import random
import concurrent.futures
import threading

from requests.packages.urllib3.util.retry import Retry

from facebookads.utils import version
from facebookads.api import FacebookBadObjectError, FacebookSession, \
    FacebookAdsApiBatch, FacebookResponse, FacebookAdsApi, \
    _top_level_param_json_encode

__author__ = 'pasha-r'


max_retries = 5
max_retries_unknown = 3
max_retries_four_hundred = 2
pause_min = 0.5


class FbFutureHolder(object):
    def __init__(self, future, endpoint=None, params=None, limit=1000,
                 method='GET', prev_response=None, extras=None):
        """
        :type future: concurrent.futures.Future
        :type prev_response: AdsAPIResponse
        """
        self.future = future
        self.response = prev_response
        self.extras = extras

        self.endpoint = endpoint
        self.params = {} if params is None else params
        self.method = method

        self.limit = 1000 if limit is None else limit
        self.starting_limit = 1000 if limit is None else limit

        self.last_error_type = None
        self.last_error = None
        self.errors_streak = 0
        self.with_timeout = 0

        self.success_cnt = 0
        self.success_streak = 0
        self.last_yield = time.time()

        self.failed = False
        self.ready = False

        self.tmp_retries = 25
        self.unknown_retries = 30
        self.too_much_data_retries = 14
        self.pause_min = 0.5

    def submit(self, facebook_client):
        """
        :type facebook_client: facebook_client
        """
        if 'limit' not in self.params or not self.params['limit']:
            self.params['limit'] = self.limit
        req_params = json_encode_complex_params(self.params)
        self.future = facebook_client.thread_pool.submit(
            facebook_client.no_throw_wrapper, self.endpoint, method=self.method,
            params=req_params, load_all=False, prev_response=self.response, holder=self)
        """:type: Future"""
        return

    def extract_results(self, facebook_client):
        """
        :type facebook_client: FbApiAsync
        :return: FbFutureHolder
        """
        self.failed = False

        if self.future.done():
            response = self.future.result()
            del self.future
            self.with_timeout = 0

            if isinstance(response, Exception):
                self.on_error(facebook_client, response)
            else:
                self.response = response
                """:type: AdsAPIResponse"""
                self.on_success(facebook_client)

        else:
            if not (self.future.running() or self.future.cancelled()):
                # still not running, just pending in a queue
                self.failed = False
                self.ready = False
                self.last_yield = time.time()
            elif self.future.cancelled():
                # was cancelled
                self.failed = True
                self.with_timeout = 0
                logger.warn("request {} was cancelled, endpoint: {}, params: {}".format(
                    str(self.future), self.endpoint, self.params))
                self.last_error = Exception("request {} was cancelled, endpoint: {}, params: {}".format(
                    str(self.future), self.endpoint, self.params))
                del self.future
            elif int(time.time() - self.last_yield) > 600:
                # running for too long
                self.future_timed_out()
            else:
                # just running
                self.failed = False
                self.ready = False
        return self

    def future_timed_out(self):
        self.failed = True
        self.with_timeout = 0
        logger.warn("request {} stuck, time: {}, endpoint: {}, params: {}".format(
            str(self.future), int(time.time() - self.last_yield),
            self.endpoint, self.params))
        self.last_error = Exception(
            "request {} stuck, time: {}, endpoint: {}, params: {}".format(
                str(self.future), int(time.time() - self.last_yield),
                self.endpoint, self.params))
        self.future.cancel()
        del self.future

    def on_success(self, facebook_client):
        self.success_streak += 1
        self.success_cnt += 1
        if self.response.all_loaded:
            self.ready = True
        else:
            self.ready = False
            if self.success_streak >= 10 and self.last_error_type != "too much data error" \
                    and self.limit < self.starting_limit:
                self.change_the_limit(self.starting_limit, 2)
            self.resubmit(facebook_client)

    def on_error(self, facebook_client, response):
        self.is_exception_fatal(response)
        if not self.failed:
            self.resubmit(facebook_client)
        else:
            self.success_streak = 0

    def resubmit(self, facebook_cient):
        req_params = self.params
        if not self.response:
            req_params = json_encode_complex_params(self.params)
        self.future = facebook_cient.thread_pool.submit(
            facebook_cient.no_throw_wrapper, self.endpoint, method=self.method,
            params=req_params, load_all=False, prev_response=self.response, holder=self)
        return

    # error handling

    def is_exception_fatal(self, exc):
        """
        :type exc: Exception
        """
        if isinstance(exc, GraphAPITemporaryError):
            self.recover_tmp_error(exc)
        elif isinstance(exc, FacebookAPINoJsonError):
            self.recover_tmp_error(exc)
        elif isinstance(exc, GraphAPIUnknownError):
            self.recover_unknown_error(exc)
        elif isinstance(exc, GraphAPITooMuchDataError):
            self.recover_too_much_data_error(exc)
        elif isinstance(exc, GraphAPIRateLimitError):
            self.recover_rate_limit_error(exc)
        elif isinstance(exc, GraphAPIError):
            self.recover_other_graph_error(exc)
        else:
            self.set_fatal_error(exc)
        return self.failed

    def set_fatal_error(self, exc, exception_type="fatal error"):
        self.set_last_error(exception_type)
        self.last_error = exc
        self.failed = True
        logger.error("While loading url: {}, method {} with params: {}. "
                     "Caught an error: {}".format(
            str(self.endpoint), str(self.method), str(self.params), str(exc)))

    def set_non_fatal_error(self, exc, exception_type="temporary error"):
        self.set_last_error(exception_type)
        self.last_error = exc
        logger.warning("While loading url: {}, method {} with params: {}. "
                       "Caught an error: {}".format(
            str(self.endpoint), str(self.method), str(self.params), str(exc)))

    def recover_other_graph_error(self, exc):
        if exc.http_code and 400 <= exc.http_code < 500:
            self.set_fatal_error(exc)
        else:
            self.recover_unknown_error(exc)

    def recover_tmp_error(self, exc):
        err_type = "temporary error"
        if self.errors_streak >= self.tmp_retries:
            self.set_fatal_error(exc, err_type)
        else:
            self.set_non_fatal_error(exc, err_type)
            self.with_timeout = 5 + 5 * self.errors_streak
            self.half_the_limit()

    def recover_too_much_data_error(self, exc):
        err_type = "too much data error"
        if self.errors_streak >= self.too_much_data_retries:
            self.set_fatal_error(exc, err_type)
        else:
            self.set_non_fatal_error(exc, err_type)
            self.half_the_limit(1)

    def recover_unknown_error(self, exc):
        err_type = "unknown error"
        if self.errors_streak >= self.unknown_retries:
            self.set_fatal_error(exc, err_type)
        else:
            self.set_non_fatal_error(exc, err_type)
            self.with_timeout = 5 + self.pause_min * self.errors_streak
            self.half_the_limit()

    def recover_rate_limit_error(self, exc):
        err_type = "rate limit error"
        self.set_non_fatal_error(exc, err_type)
        self.with_timeout = 10 + random.randint(0, 9) + 10 * self.errors_streak

    # error helpers

    def half_the_limit(self, lowest_limit=2):
        self.change_the_limit(int(self.limit / 2), lowest_limit)

    def change_the_limit(self, new_limit, lowest_limit=2):
        self.limit = int(new_limit)
        if self.limit < lowest_limit:
            self.limit = lowest_limit
        elif self.limit < 1:
            self.limit = 1

        if self.response:
            self.response.change_next_page_limit(self.limit, lowest_limit=lowest_limit)
        elif 'limit' in self.params and self.params['limit']:
            self.params['limit'] = self.limit
        else:
            self.params['limit'] = 100

    def set_last_error(self, err_type):
        if self.last_error_type == err_type:
            self.errors_streak += 1
        else:
            self.errors_streak = 1
            self.last_error_type = err_type


class FbFailEarlyFutureHolder(FbFutureHolder):
    def __init__(self, future, endpoint=None, params=None, limit=None, method='GET',
                 prev_response=None, extras=None, repeat_fails=0):
        super(FbFailEarlyFutureHolder, self).__init__(
              future, endpoint=endpoint, params=params, limit=limit,
              method=method, prev_response=prev_response, extras=extras)
        self.tmp_retries = repeat_fails
        self.unknown_retries = repeat_fails
        self.too_much_data_retries = repeat_fails

    def on_success(self, facebook_client):
        super(FbFailEarlyFutureHolder, self).on_success(facebook_client)
        if self.response and not self.response.all_loaded and self.success_streak == 1:
            self.tmp_retries = 25
            self.unknown_retries = 30
            self.too_much_data_retries = 14


class FbPagedStatsFutureHolder(FbFutureHolder):
    def on_success(self, facebook_client):
        super(FbPagedStatsFutureHolder, self).on_success(facebook_client)
        if self.response and not self.response.all_loaded and self.success_streak == 1:
            self.change_the_limit(self.limit, 2)


class FbApiAsync(object):
    def __init__(self, api, threadpool_size=10):
        """
        :type api: facebookads.api.FacebookAdsApi
        """
        self.thread_lock = threading.Lock()
        self.thread_pool = concurrent.futures.ThreadPoolExecutor(threadpool_size)
        self._api = api
        self.futures = []

    def get_request_future_holder(self, endpoint=None, params=None, limit=None,
                                  method='GET', prev_response=None, extras=None,
                                  holder_type=FbFutureHolder):
        """
        :param endpoint:
        :param params:
        :param limit:
        :param method:
        :param prev_response: AdsAPIResponse
        :type holder_type: type
        :rtype: FbFutureHolder
        """
        params = params or {}
        if 'limit' not in params or not params['limit']:
            params['limit'] = limit or self.limit
        req_params = json_encode_complex_params(params)
        holder = holder_type(None, endpoint=endpoint, method=method, params=req_params,
                             limit=limit, prev_response=prev_response, extras=extras)
        holder.submit(self)
        return holder

    def no_throw_wrapper(self, endpoint=None, method='GET', params=None, files=None,
                         load_all=False, prev_response=None, with_timeout=0,
                         holder=None):
        """
        Non-throwing version of _request.

        :param endpoint:
        :param method:
        :param params:
        :param files:
        :param load_all:
        :rtype: AdsAPIResponse
        """
        try:
            if with_timeout:
                time.sleep(with_timeout)
            if holder:
                holder.last_yield = time.time()
            return self._request(endpoint, method, params, files, load_all,
                                 prev_response=prev_response)
        except Exception as exc:
            return exc

    def get_async_results(self):
        """
        :type futures: list[FbFutureHolder]
        :rtype: list[FbFutureHolder]
        """
        time.sleep(0.01)
        cnt = 0
        while True:
            cnt += 1
            try:
                self.thread_lock.acquire()
                future_holder = self.futures.pop(0)
            except IndexError:
                break
            finally:
                self.thread_lock.release()

            future_holder.extract_results(self)

            if future_holder.ready:
                # loaded all the data
                yield future_holder
            else:
                if future_holder.failed:
                    # request failed unrecoverably
                    yield future_holder
                else:

                    # some more loading needs to be done
                    try:
                        self.thread_lock.acquire()
                        self.futures.append(future_holder)
                    finally:
                        self.thread_lock.release()

                    if cnt >= len(self.futures):
                        cnt = 0
                        time.sleep(0.3)

    def __del__(self):
        if self.thread_pool:
            try:
                if not self.thread_pool._shutdown:
                    self.thread_pool.shutdown(False)
            except Exception:
                pass
            del self.thread_pool


# Facebook stuff override

class FacebookAdsAsyncApi(object):

    """Encapsulates session attributes and methods to make API calls.

    Attributes:
        SDK_VERSION (class): indicating sdk version.
        HTTP_METHOD_GET (class): HTTP GET method name.
        HTTP_METHOD_POST (class): HTTP POST method name
        HTTP_METHOD_DELETE (class): HTTP DELETE method name
        HTTP_DEFAULT_HEADERS (class): Default HTTP headers for requests made by
            this sdk.
    """

    SDK_VERSION = version.get_version()

    API_VERSION = 'v' + str(re.sub('^(\d+\.\d+)\.\d+$', '\g<1>', SDK_VERSION))

    HTTP_METHOD_GET = 'GET'

    HTTP_METHOD_POST = 'POST'

    HTTP_METHOD_DELETE = 'DELETE'

    HTTP_DEFAULT_HEADERS = {
        'User-Agent': "fb-python-ads-api-sdk-%s" % SDK_VERSION,
    }

    _default_api = None
    _default_account_id = None

    def __init__(self, session):
        """Initializes the api instance.

        Args:
            session: FacebookSession object that contains a requests interface
                and attribute GRAPH (the Facebook GRAPH API URL).
        """
        self._session = session
        self._num_requests_succeeded = 0
        self._num_requests_attempted = 0
        self.thread_lock = threading.Lock()
        self.thread_pool = concurrent.futures.ThreadPoolExecutor(threadpool_size)
        self._api = api
        self.futures = []

    def get_num_requests_attempted(self):
        """Returns the number of calls attempted."""
        return self._num_requests_attempted

    def get_num_requests_succeeded(self):
        """Returns the number of calls that succeeded."""
        return self._num_requests_succeeded

    @classmethod
    def init(
        cls,
        app_id=None,
        app_secret=None,
        access_token=None,
        account_id=None,
        pool_maxsize=10,
        max_retries=0
    ):
        session = FacebookSession(app_id, app_secret, access_token,
                                  pool_maxsize, max_retries)
        api = cls(session)
        cls.set_default_api(api)

        if account_id:
            cls.set_default_account_id(account_id)

    @classmethod
    def set_default_api(cls, api_instance):
        """Sets the default api instance.

        When making calls to the api, objects will revert to using the default
        api if one is not specified when initializing the objects.

        Args:
            api_instance: The instance which to set as default.
        """
        cls._default_api = api_instance

    @classmethod
    def get_default_api(cls):
        """Returns the default api instance."""
        return cls._default_api

    @classmethod
    def set_default_account_id(cls, account_id):
        account_id = str(account_id)
        if account_id.find('act_') == -1:
            raise ValueError(
                "Account ID provided in FacebookAdsApi.set_default_account_id "
                "expects a string that begins with 'act_'"
            )
        cls._default_account_id = account_id

    @classmethod
    def get_default_account_id(cls):
        return cls._default_account_id

    def call(
        self,
        method,
        path,
        params=None,
        headers=None,
        files=None,
        url_override=None,
        api_version=None,
    ):
        """Makes an API call.

        Args:
            method: The HTTP method name (e.g. 'GET').
            path: A tuple of path tokens or a full URL string. A tuple will
                be translated to a url as follows:
                graph_url/tuple[0]/tuple[1]...
                It will be assumed that if the path is not a string, it will be
                iterable.
            params (optional): A mapping of request parameters where a key
                is the parameter name and its value is a string or an object
                which can be JSON-encoded.
            headers (optional): A mapping of request headers where a key is the
                header name and its value is the header value.
            files (optional): An optional mapping of file names to binary open
                file objects. These files will be attached to the request.

        Returns:
            A FacebookResponse object containing the response body, headers,
            http status, and summary of the call that was made.

        Raises:
            FacebookResponse.error() if the request failed.
        """
        if not params:
            params = {}
        if not headers:
            headers = {}
        if not files:
            files = {}

        if api_version and not re.search('v[0-9]+\.[0-9]+', api_version):
            raise FacebookBadObjectError(
                'Please provide the API version in the following format: %s'
                % self.API_VERSION
            )

        self._num_requests_attempted += 1

        if not isinstance(path, six.string_types):
            # Path is not a full path
            path = "/".join((
                self._session.GRAPH or url_override,
                api_version or self.API_VERSION,
                '/'.join(map(str, path)),
            ))

        # Include api headers in http request
        headers = headers.copy()
        headers.update(FacebookAdsApi.HTTP_DEFAULT_HEADERS)

        if params:
            params = _top_level_param_json_encode(params)

        # Get request response and encapsulate it in a FacebookResponse
        if method in ('GET', 'DELETE'):
            response = self._session.requests.request(
                method,
                path,
                params=params,
                headers=headers,
                files=files,
            )
        else:
            response = self._session.requests.request(
                method,
                path,
                data=params,
                headers=headers,
                files=files,
            )
        fb_response = FacebookResponse(
            body=response.text,
            headers=response.headers,
            http_status=response.status_code,
            call={
                'method': method,
                'path': path,
                'params': params,
                'headers': headers,
                'files': files,
            },
        )

        if fb_response.is_failure():
            raise fb_response.error()

        self._num_requests_succeeded += 1
        return fb_response

    def call_future(
        self,
        method,
        path,
        params=None,
        headers=None,
        files=None,
        url_override=None,
        api_version=None,
    ):
        pass

    def new_batch(self):
        """
        Returns a new FacebookAdsApiBatch, which when executed will go through
        this api.
        """
        return FacebookAdsApiBatch(api=self)


# Helpers

def get_async_api(app_id=None, app_secret=None, access_token=None, pool_maxsize=10):
    """
    Returns FbApiAsync object with thread pool initialized and an associated session
    with appropriately sized connection pool.

    :type app_id: str|int
    :type app_secret: str
    :type access_token: str
    :type pool_maxsize: int
    :rtype: FbApiAsync
    """
    retry_policy = Retry(total=None, connect=3, read=3, redirect=2)
    ads_api = FacebookAdsAsyncApi(FacebookSession(
            app_id=app_id, app_secret=app_secret, access_token=access_token,
            pool_maxsize=pool_maxsize, max_retries=retry_policy))
    return ads_api
