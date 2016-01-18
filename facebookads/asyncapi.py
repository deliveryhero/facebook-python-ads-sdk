from __future__ import unicode_literals, absolute_import, print_function
import re
import six
import time
import random
import logging
import concurrent.futures
import threading

from facebookads.exceptions import FacebookCallFailedError, FacebookBadObjectError
from facebookads.api import FacebookSession, FacebookResponse, \
    FacebookAdsApi, _top_level_param_json_encode

__author__ = 'pasha-r'

logger = logging.getLogger("facebookclient")


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
        req_params = _top_level_param_json_encode(self.params)
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
            req_params = _top_level_param_json_encode(self.params)
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


# Facebook stuff override

class FacebookAsyncResponse(FacebookResponse):
    def __init__(self, body=None, http_status=None, headers=None, call=None, error=None):
        super(FacebookAsyncResponse, self).__init__(body, http_status, headers, call)
        self._error = error

    def is_success(self):
        return not bool(self._error) and super(FacebookAsyncResponse, self).is_success()

    def error(self):
        """
        Returns a FacebookRequestError (located in the exceptions module) with
        an appropriate debug message.
        """
        if self._error:
            return self._error
        return super(FacebookAsyncResponse, self).error()


class FacebookAdsAsyncApi(FacebookAdsApi):
    """Encapsulates session attributes and methods to make API calls.
    Provides an ability to issue several calls at the same time.
    """

    _default_api = None
    _default_account_id = None

    def __init__(self, session, threadpool_size):
        """Initializes the api instance.

        Args:
            session: FacebookSession object that contains a requests interface
                and attribute GRAPH (the Facebook GRAPH API URL).
        """
        super(FacebookAdsAsyncApi, self).__init__(session)
        self._thread_lock = threading.Lock()
        self._thread_pool = concurrent.futures.ThreadPoolExecutor(threadpool_size)
        self._futures = {}
        """:type: dict[int, facebookads.asyncobjects.AsyncEdgeIterator]"""
        self._futures_ordered = []

    @classmethod
    def init(cls, app_id=None, app_secret=None, access_token=None,
             account_id=None, pool_maxsize=10, max_retries=0):
        session = FacebookSession(app_id, app_secret, access_token,
                                  pool_maxsize, max_retries)
        api = cls(session, threadpool_size=pool_maxsize)
        cls.set_default_api(api)

        if account_id:
            cls.set_default_account_id(account_id)

    def prepare_request_params(self, path, params, headers, files,
                               url_override, api_version):
        if not params:
            params = {}
        if not headers:
            headers = {}
        if not files:
            files = {}
        if api_version and not re.search('v[0-9]+\.[0-9]+', api_version):
            raise FacebookBadObjectError(
                    'Please provide the API version in the following format: %s'
                    % self.API_VERSION)
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
        return path, params, headers, files

    def non_throwing_call(self, method, path, params=None, headers=None, files=None,
                          url_override=None, api_version=None):
        """A non-throwing version of call method.
        Returns FacebookAsyncResponse.

        :param method: The HTTP method name (e.g. 'GET').
        :param path: A tuple of path tokens or a full URL string. A tuple will
            be translated to a url as follows:
            graph_url/tuple[0]/tuple[1]...
            It will be assumed that if the path is not a string, it will be
            iterable.
        :param params: (optional) A mapping of request parameters where a key
            is the parameter name and its value is a string or an object
            which can be JSON-encoded.
        :param headers: (optional) A mapping of request headers where a key is the
            header name and its value is the header value.
        :param files: (optional) A mapping of file names to binary open
            file objects. These files will be attached to the request.
        :param url_override:
        :param api_version:
        :rtype: FacebookAsyncResponse
        """
        self._num_requests_attempted += 1
        call_signature = {'method': method, 'path': path, 'params': params,
                          'headers': headers, 'files': files},

        # Get request response and encapsulate it in a FacebookResponse
        try:
            if method in ('GET', 'DELETE'):
                response = self._session.requests.request(
                    method, path, params=params, headers=headers, files=files)
            else:
                response = self._session.requests.request(
                    method, path, data=params, headers=headers, files=files)
        except Exception as exc:
            error = FacebookCallFailedError(call_signature, exc)
            fb_response = FacebookAsyncResponse(call=call_signature, error=error)
        else:
            fb_response = FacebookAsyncResponse(
                body=response.text, headers=response.headers,
                http_status=response.status_code, call=call_signature)

        if fb_response.is_success():
            self._num_requests_succeeded += 1
        return fb_response

    def call_future(self, edge_iter, method, path, params=None, headers=None, files=None,
                    url_override=None, api_version=None):
        """Adds an async API call task to a futures queue.
        Returns a future holder object.

        :param facebookads.asyncobjects.AsyncEdgeIterator edge_iter:
            edge iterator issuing this call
        :param method: The HTTP method name (e.g. 'GET').
        :param path: A tuple of path tokens or a full URL string. A tuple will
            be translated to a url as follows:
            graph_url/tuple[0]/tuple[1]...
            It will be assumed that if the path is not a string, it will be
            iterable.
        :param params: (optional) A mapping of request parameters where a key
            is the parameter name and its value is a string or an object
            which can be JSON-encoded.
        :param headers: (optional) A mapping of request headers where a key is the
            header name and its value is the header value.
        :param files: (optional) A mapping of file names to binary open
            file objects. These files will be attached to the request.
        :param url_override:
        :param api_version:
        :return:
        """
        path, params, headers, files = self.prepare_request_params(
                path, params, headers, files, url_override, api_version)

        future = self._thread_pool.submit(
                self.non_throwing_call, method, path,
                params=params, headers=headers, files=files)

        self.put_in_futures(edge_iter)
        return future

    def put_in_futures(self, edge_iter):
        with self._thread_lock:
            self._futures_ordered.append(id(edge_iter))
            self._futures[id(edge_iter)] = edge_iter

    def remove_from_futures(self, edge_iter):
        with self._thread_lock:
            del self._futures[id(edge_iter)]
            self._futures_ordered.remove(id(edge_iter))

    def get_async_results(self):
        """
        :rtype: list[FbFutureHolder]
        """
        time.sleep(0.01)
        cnt = 0
        while True:
            cnt += 1
            with self._thread_lock:
                try:
                    edge_iter_id = self._futures_ordered.pop(0)
                    if not edge_iter_id in self._futures:
                        continue
                    edge_iter = self._futures.pop(edge_iter_id)
                except IndexError:
                    break

            edge_iter.extract_results(self)

            if edge_iter.ready:
                # loaded all the data
                yield edge_iter
            else:
                if edge_iter.failed:
                    # request failed unrecoverably
                    yield edge_iter
                else:

                    # some more loading needs to be done
                    self.put_in_futures(edge_iter)

                    if cnt >= len(self._futures):
                        cnt = 0
                        time.sleep(0.3)

    def __del__(self):
        if self._thread_pool:
            try:
                if not self._thread_pool._shutdown:
                    self._thread_pool.shutdown(False)
            except Exception:
                pass
            del self._thread_pool
