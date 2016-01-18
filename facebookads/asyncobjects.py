import time
import random
import logging
from facebookads.objects import *
from facebookads.asyncapi import FacebookAdsAsyncApi, FacebookAsyncResponse

logger = logging.getLogger("facebookclient")


class AsyncEdgeIterator(EdgeIterator):

    """Asyncronously retrieves pages of data from object's connections.
    Each page is a list of dicts with the data.
    And it iterates over the data.

    Examples:
        >>> acc = AdAccount('account_id')
        >>> ad_names = []
        >>> for row in acc.get_ads(fields=[Ad.Field.name]):
        >>>     ad_names.append(row["name"])
        >>> ad_names
    """

    def __init__(self, source_object, target_objects_class,
                 fields=None, params=None, include_summary=True,
                 limit=1000):
        """
        Initializes an iterator over the objects to which there is an edge from
        source_object.

        Args:
            source_object: An AbstractObject instance from which to inspect an
                edge. This object should have an id.
            target_objects_class: Objects traverersed over will be initialized
                with this AbstractObject class.
            fields (optional): A list of fields of target_objects_class to
                automatically read in.
            params (optional): A mapping of request parameters where a key
                is the parameter name and its value is a string or an object
                which can be JSON-encoded.
        """
        super(AsyncEdgeIterator, self).__init__(
                source_object, target_objects_class,
                fields=fields, params=params, include_summary=include_summary)
        self._future = None

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

    def get_all_results(self):
        return self._queue

    def load_next_page(self):
        """Queries server for more nodes and loads them into the internal queue.

        Returns:
            True if successful, else False.
        """
        if self._finished_iteration:
            return False
        if not self._future:
            if not self.submit_next_page_async():
                return False

        result = None
        while not result:
            result = self.check_for_the_next_page()

        success = self.read_next_page_result(result) > 0
        self.submit_next_page_async()
        return success

    def submit_next_page_async(self):
        """Puts future request into thread pool queue.

        Returns:
            True if successful, else False.
        """
        if self._finished_iteration or self._future:
            return False

        if self._include_summary:
            if 'summary' not in self.params:
                self.params['summary'] = True

        self._future = self._source_object.get_api_assured().call_future(
                self, 'GET', self._path, params=self.params)
        return True

    def check_for_the_next_page(self):
        if not self._future:
            raise FacebookUnavailablePropertyException("first submit new async call")

        if self._future.done():
            result = self._future.result()
            self._future = None
            try:
                self._source_object.get_api_assured().remove_from_futures(self)
            except ValueError:
                pass
        else:
            # TODO: handle timeouts here (resubmits)
            result = None

        # TODO: handle errors and recovery here (resubmits)
        if result.is_failure():
            return True

        return result

    def read_next_page_result(self, result):
        """

        :type result: facebookads.asyncapi.FacebookAsyncResponse
        :return:
        """
        response = result.json()
        if 'paging' in response and 'next' in response['paging']:
            self._path = response['paging']['next']
        else:
            # Indicate if this was the last page
            self._finished_iteration = True

        if (
            self._include_summary and
            'summary' in response and
            'total_count' in response['summary']
        ):
            self._total_count = response['summary']['total_count']

        return self.build_objects_from_response(response)

    def build_objects_from_response(self, response):
        if 'data' in response and isinstance(response['data'], list):
            new_cnt = len(response['data'])
            self._queue += response['data']

            if new_cnt <= 0:
                # API may return paging.next even for the last page
                self._finished_iteration = True
        else:
            self._finished_iteration = True
            data = response['data'] if 'data' in response else response
            self._queue.append(data)
            new_cnt = 1

        return new_cnt

    # results processing

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
                    str(self.future), str(self._path), self.params))
                self.last_error = Exception("request {} was cancelled, endpoint: {}, params: {}".format(
                    str(self.future), str(self._path), self.params))
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
            str(self._path), self.params))
        self.last_error = Exception(
            "request {} stuck, time: {}, endpoint: {}, params: {}".format(
                str(self.future), int(time.time() - self.last_yield),
                str(self._path), self.params))
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
                self.change_the_next_page_limit(self.starting_limit, 2)
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

    # errors handling

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
        logger.error("While loading url: {}, method GET with params: {}. "
                     "Caught an error: {}".format(
            str(self._path), str(self.params), str(exc)))

    def set_non_fatal_error(self, exc, exception_type="temporary error"):
        self.set_last_error(exception_type)
        self.last_error = exc
        logger.warning("While loading url: {}, method GET with params: {}. "
                       "Caught an error: {}".format(
            str(self._path), str(self.params), str(exc)))

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
            self.change_the_next_page_limit()

    def recover_too_much_data_error(self, exc):
        err_type = "too much data error"
        if self.errors_streak >= self.too_much_data_retries:
            self.set_fatal_error(exc, err_type)
        else:
            self.set_non_fatal_error(exc, err_type)
            self.change_the_next_page_limit(lowest_limit=1)

    def recover_unknown_error(self, exc):
        err_type = "unknown error"
        if self.errors_streak >= self.unknown_retries:
            self.set_fatal_error(exc, err_type)
        else:
            self.set_non_fatal_error(exc, err_type)
            self.with_timeout = 5 + self.pause_min * self.errors_streak
            self.change_the_next_page_limit()

    def recover_rate_limit_error(self, exc):
        err_type = "rate limit error"
        self.set_non_fatal_error(exc, err_type)
        self.with_timeout = 10 + random.randint(0, 9) + 10 * self.errors_streak

    # error helpers

    def change_the_next_page_limit(self, new_limit=None, lowest_limit=2):
        if new_limit is None:
            new_limit = int(self.limit / 2)
        self.limit = int(new_limit)
        if self.limit < lowest_limit:
            self.limit = lowest_limit
        elif self.limit < 1:
            self.limit = 1

        if self.response:
            self.response.change_next_page_limit(self.limit, lowest_limit=lowest_limit)
            if not self._paging or 'next' not in self._paging:
                return

            pr = urlparse(self._paging['next'])
            params = dict(parse_qsl(pr.query, keep_blank_values=True))
            if 'limit' not in params or not params['limit'] or not params['limit'].isdigit():
                old_limit = 2000
            else:
                old_limit = int(params['limit'])

            if new_limit is None:
                new_limit = int(old_limit / 2)
            if new_limit < lowest_limit:
                new_limit = lowest_limit
            params['limit'] = str(new_limit)
            new_url = urlunparse((pr.scheme, pr.netloc, pr.path, pr.params,
                                  urlencode(params), pr.fragment))
            self._paging['next'] = new_url

        elif 'limit' in self.params and self.params['limit']:
            self.params['limit'] = self.limit
        else:
            self.params['limit'] = 100
        return

    def set_last_error(self, err_type):
        if self.last_error_type == err_type:
            self.errors_streak += 1
        else:
            self.errors_streak = 1
            self.last_error_type = err_type


class AbstractCrudAsyncObject(AbstractCrudObject):
    """
    Extends AbstractCrudObject and implements async iter_edge operation.
    """

    @classmethod
    def get_by_ids(cls, ids, params=None, fields=None, api=None):
        """Get objects by id list
        :type ids: list
        :type params: didct
        :type fields: list
        :type api: FacebookAdsAsyncApi
        :rtype: list[AbstractCrudAsyncObject]
        """
        api = api or FacebookAdsAsyncApi.get_default_api()
        params = dict(params or {})
        cls._assign_fields_to_params(fields, params)
        params['ids'] = ','.join(map(str, ids))
        # TODO: check if it's faster to make separate calls for each id, but in parallell
        response = api.call(
            'GET',
            ['/'],
            params=params,
        )
        result = []
        for fbid, data in response.json().items():
            obj = cls(fbid, api=api)
            obj._set_data(data)
            result.append(obj)
        return result

    # Getters

    def get_api(self):
        """
        Returns the api associated with the object. If None, returns the
        default api.
        :rtype: FacebookAdsAsyncApi
        """
        return self._api or FacebookAdsAsyncApi.get_default_api()

    # Helpers

    def iterate_edge(self, target_objects_class, fields=None, params=None,
                     fetch_first_page=True, include_summary=True):
        """
        Returns EdgeIterator with argument self as source_object and
        the rest as given __init__ arguments.

        Note: list(iterate_edge(...)) can prefetch all the objects.
        """
        source_object = self
        iterator = AsyncEdgeIterator(
            source_object,
            target_objects_class,
            fields=fields,
            params=params,
            include_summary=include_summary,
        )
        iterator.submit_next_page_async()
        return iterator

    def iterate_edge_async(self, target_objects_class, fields=None,
                           params=None, async=False, include_summary=True):
        """
        Behaves as iterate_edge(...) if parameter async if False
        (Default value)

        If async is True:
        Returns an AsyncJob which can be checked using remote_read()
        to verify when the job is completed and the result ready to query
        or download using get_result()
        """
        synchronous = not async
        synchronous_iterator = self.iterate_edge(
            target_objects_class,
            fields,
            params,
            fetch_first_page=synchronous,
            include_summary=include_summary,
        )
        if synchronous:
            return synchronous_iterator

        if not params:
            params = {}
        else:
            params = dict(params)
        self.__class__._assign_fields_to_params(fields, params)

        # To force an async response from an edge, do a POST instead of GET.
        # The response comes in the format of an AsyncJob which
        # indicates the progress of the async request.
        response = self.get_api_assured().call(
            'POST',
            (self.get_id_assured(), target_objects_class.get_endpoint()),
            params=params,
        ).json()

        # AsyncJob stores the real iterator
        # for when the result is ready to be queried
        result = AsyncJob(target_objects_class)

        if 'report_run_id' in response:
            response['id'] = response['report_run_id']
        result._set_data(response)
        return result

    def edge_object(self, target_objects_class, fields=None, params=None):
        """
        Returns first object when iterating over EdgeIterator with argument
        self as source_object and the rest as given __init__ arguments.
        """
        params = {} if not params else params.copy()
        params['limit'] = '1'
        for obj in self.iterate_edge(
            target_objects_class,
            fields=fields,
            params=params,
        ):
            return obj

        # if nothing found, return None
        return None


class AdUser(AbstractCrudAsyncObject, AdUser):
    pass


class Page(AbstractCrudAsyncObject, Page):
    pass


class AdAccount(AbstractCrudAsyncObject, AdAccount):
    pass


class AdAccountGroup(AbstractCrudAsyncObject, AdAccountGroup):
    pass


class AdAccountGroupUser(AbstractCrudAsyncObject, AdAccountGroupUser):
    pass


class Campaign(AbstractCrudAsyncObject, Campaign):
    pass


class AdSet(AbstractCrudAsyncObject, AdSet):
    pass


class Ad(AbstractCrudAsyncObject, Ad):
    pass


class AdConversionPixel(AbstractCrudAsyncObject, AdConversionPixel):
    pass


class AdsPixel(AbstractCrudAsyncObject, AdsPixel):
    pass


class AdCreative(AbstractCrudAsyncObject, AdCreative):
    pass


class AdImage(AbstractCrudAsyncObject, AdImage):
    pass


class AdVideo(AbstractCrudAsyncObject, AdVideo):
    pass


class ClickTrackingTag(AbstractCrudAsyncObject, ClickTrackingTag):
    pass


class CustomAudience(AbstractCrudAsyncObject, CustomAudience):
    pass


class LookalikeAudience(AbstractCrudAsyncObject, LookalikeAudience):
    pass


class PartnerCategory(AbstractCrudAsyncObject, PartnerCategory):
    pass


class ReachFrequencyPrediction(AbstractCrudAsyncObject, ReachFrequencyPrediction):
    pass


class Business(AbstractCrudAsyncObject, Business):
    pass


class ProductCatalog(AbstractCrudAsyncObject, ProductCatalog):
    pass


class ProductFeed(AbstractCrudAsyncObject, ProductFeed):
    pass


class ProductFeedUpload(AbstractCrudAsyncObject, ProductFeedUpload):
    pass


class ProductFeedUploadError(AbstractCrudAsyncObject, ProductFeedUploadError):
    pass


class ProductSet(AbstractCrudAsyncObject, ProductSet):
    pass


class ProductGroup(AbstractCrudAsyncObject, ProductGroup):
    pass


class Product(AbstractCrudAsyncObject, Product):
    pass


class ProductAudience(AbstractCrudAsyncObject, ProductAudience):
    pass


class AdLabel(AbstractCrudAsyncObject, AdLabel):
    pass


class Lead(AbstractCrudAsyncObject, Lead):
    pass


class LeadgenForm(AbstractCrudAsyncObject, LeadgenForm):
    pass


class AdPlacePageSet(AbstractCrudAsyncObject, AdPlacePageSet):
    pass


class CustomConversion(AbstractCrudAsyncObject, CustomConversion):
    pass


class Insights(AbstractCrudAsyncObject, Insights):
    # TODO: implement async get method
    pass


class AsyncJob(CannotCreate, AbstractCrudAsyncObject, AbstractCrudObject):

    class Field(object):
        id = 'id'
        async_status = 'async_status'
        async_percent_completion = 'async_percent_completion'

    def __init__(self, target_objects_class):
        AbstractCrudObject.__init__(self, target_objects_class)
        self.target_objects_class = target_objects_class

    def get_result(self, params=None):
        """
        Gets the final result from an async job
        Accepts params such as limit
        """
        return self.iterate_edge(
            self.target_objects_class,
            params=params,
            include_summary=False,
        )

    def __nonzero__(self):
        return self[self.Field.async_percent_completion] == 100
