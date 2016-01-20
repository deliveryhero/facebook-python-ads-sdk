import time
import random
import logging
import facebookads.objects as baseobjects
from facebookads.asyncapi import FacebookAdsAsyncApi
from facebookads.exceptions import FacebookRequestError,\
    FacebookUnavailablePropertyException
from facebookads.utils.fberrcodes import FacebookErrorCodes

try:
    from urlparse import parse_qsl, urlparse, urlunsplit
    from urllib import urlencode
except ImportError:  # python 3 compatibility
    from urllib.parse import parse_qsl, urlparse, urlunsplit, urlencode

logger = logging.getLogger("facebookclient")


class AioEdgeIterator(baseobjects.EdgeIterator):

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
        self.limit = 1000 if limit is None else limit
        self.starting_limit = 1000 if limit is None else limit
        if params is None:
            params = {"limit": self.limit}
        elif not params or "limit" not in params:
            params["limit"] = self.limit

        super(AioEdgeIterator, self).__init__(
                source_object, target_objects_class,
                fields=fields, params=params, include_summary=include_summary)

        # AIO future holder
        self._future = None
        # last loaded response
        self._response = None
        """:type: facebookads.asyncapi.FacebookAsyncResponse"""

        # iterator's state is:
        # self._finished_iteration - True or False
        # self.failed - True or False
        self._request_failed = False
        self._page_ready = False

        self.last_error_type = None
        self.last_error = None
        self.errors_streak = 0
        self.delay_next_call_for = 0

        self.success_cnt = 0
        self.success_streak = 0
        self.last_yield = time.time()

        self.tmp_retries = 25
        self.unknown_retries = 30
        self.too_much_data_retries = 14
        self.pause_min = 0.5

    def get_all_results(self):
        return self._queue

    # AIO-based page loader

    def load_next_page(self):
        """Queries server for more nodes and loads them into the internal queue.

        Returns:
            True if successful, else False.
        """
        if self._finished_iteration:
            return False
        if not self._future:
            if not self.submit_next_page_aio():
                return False

        while not self._page_ready or self._request_failed:
            self.extract_results()
            time.sleep(0.2)
        return self._page_ready and not self._request_failed and self._queue

    # AIO methods

    def submit_next_page_aio(self):
        """Puts future request into thread pool queue.

        Returns:
            True if successful, else False.
        """
        if self._finished_iteration or self._future:
            return False

        if self._include_summary:
            if 'summary' not in self.params:
                self.params['summary'] = True

        self._request_failed = False
        self._page_ready = False
        self._future = self._source_object.get_api_assured().call_future(
                self, 'GET', self._path, params=self.params,
                delay_next_call_for=self.delay_next_call_for)
        return True

    def read_next_page_aio(self, response):
        """

        :type response: facebookads.asyncapi.FacebookAsyncResponse
        :return:
        """
        jresp = response.json()
        if not jresp:
            self._finished_iteration = True
            return 0

        if 'paging' in jresp and 'next' in jresp['paging']:
            self._path = jresp['paging']['next']
            pr = urlparse(jresp['paging']['next'])
            self.params = dict(parse_qsl(pr.query, keep_blank_values=True))

            # url components: scheme, netloc, url, query, fragment
            self._path = urlunsplit((pr.scheme, pr.netloc, pr.path, None, None))
        else:
            # Indicate if this was the last page
            self._finished_iteration = True

        if (
            self._include_summary and
            'summary' in jresp and
            'total_count' in jresp['summary']
        ):
            self._total_count = jresp['summary']['total_count']

        return self.build_objects_from_response(jresp)

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
        self._response = None

        return new_cnt

    # results processing and state changes

    def extract_results(self):
        """
        :return: FbFutureHolder
        """
        self._request_failed = False
        if not self._future:
            raise FacebookUnavailablePropertyException("first submit new async call")

        if self._future.done():
            result = self._future.result()
            self._source_object.get_api_assured().remove_from_futures(self)
            del self._future
            self._future = None

            self.delay_next_call_for = 0

            if result.is_failure():
                self.on_error(result)
            else:
                self.on_success(result)

        else:
            if not self._future.running() and not self._future.cancelled():
                # still not running, just pending in a queue
                self._request_failed = False
                self._page_ready = False
                self.last_yield = time.time()
            elif self._future.cancelled():
                # was cancelled
                self._request_failed = True
                self.delay_next_call_for = 0
                logger.warn("request {} was cancelled, endpoint: {}, params: {}".format(
                    str(self._future), str(self._path), self.params))
                self.last_error = Exception("request {} was cancelled, endpoint: {}, params: {}".format(
                    str(self._future), str(self._path), self.params))
                self._source_object.get_api_assured().remove_from_futures(self)
                del self._future
                self._future = None
            elif int(time.time() - self.last_yield) > 600:
                # running for too long
                self.future_timed_out()
            else:
                # just running
                self._request_failed = False
                self._page_ready = False
        return self

    def future_timed_out(self):
        self._request_failed = True
        self.delay_next_call_for = 0
        logger.warn("request {} stuck, time: {}, endpoint: {}, params: {}".format(
            str(self._future), int(time.time() - self.last_yield),
            str(self._path), self.params))
        self.last_error = Exception(
            "request {} stuck, time: {}, endpoint: {}, params: {}".format(
                str(self._future), int(time.time() - self.last_yield),
                str(self._path), self.params))
        self._future.cancel()
        del self._future
        self._future = None

    def on_success(self, response):
        self._response = response
        """:type: facebookads.asyncapi.FacebookAsyncResponse"""
        self.success_streak += 1
        self.success_cnt += 1
        self._page_ready = True
        self.read_next_page_aio(self._response)
        if not self._finished_iteration:
            if self.success_streak >= 10 and self.last_error_type != "too much data error" \
                    and self.limit < self.starting_limit:
                self.change_the_next_page_limit(self.starting_limit, 2)
            self.submit_next_page_aio()

    def on_error(self, response):
        self.is_exception_fatal(response)
        if not self._request_failed:
            self.submit_next_page_aio()
        else:
            self.success_streak = 0

    # errors handling

    def is_exception_fatal(self, resp):
        exc = resp.error()
        if isinstance(exc, FacebookRequestError):
            if exc._api_error_code == FacebookErrorCodes.temporary:
                self.recover_tmp_error(exc)
            elif exc._api_error_code == FacebookErrorCodes.unknown:
                self.recover_unknown_error(exc)
            elif exc._api_error_code == FacebookErrorCodes.too_much_data:
                self.recover_too_much_data_error(exc)
            elif exc._api_error_code == FacebookErrorCodes.rate_limit:
                self.recover_rate_limit_error(exc)
            else:
                self.recover_other_graph_error(exc)
        else:
            if resp.body() and not resp.json():
                self.recover_tmp_error(exc)
            else:
                self.set_fatal_error(exc)
        return self._request_failed

    def set_fatal_error(self, exc, exception_type="fatal error"):
        self.set_last_error(exception_type)
        self.last_error = exc
        self._request_failed = True
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
        if exc._http_status and 400 <= exc._http_status < 500:
            self.set_fatal_error(exc)
        else:
            self.recover_unknown_error(exc)

    def recover_tmp_error(self, exc):
        err_type = "temporary error"
        if self.errors_streak >= self.tmp_retries:
            self.set_fatal_error(exc, err_type)
        else:
            self.set_non_fatal_error(exc, err_type)
            self.delay_next_call_for = 5 + 5 * self.errors_streak
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
            self.delay_next_call_for = 5 + self.pause_min * self.errors_streak
            self.change_the_next_page_limit()

    def recover_rate_limit_error(self, exc):
        err_type = "rate limit error"
        self.set_non_fatal_error(exc, err_type)
        self.delay_next_call_for = 10 + random.randint(0, 9) + 10 * self.errors_streak

    # error helpers

    def change_the_next_page_limit(self, new_limit=None, lowest_limit=2):
        if self._finished_iteration:
            return

        if new_limit is None:
            new_limit = int(self.limit / 2)
        self.limit = int(new_limit)
        if self.limit < lowest_limit:
            self.limit = lowest_limit
        elif self.limit < 1:
            self.limit = 1

        if 'limit' in self.params and self.params['limit']:
            self.params['limit'] = self.limit
        else:
            self.params['limit'] = self.starting_limit
        return

    def set_last_error(self, err_type):
        if self.last_error_type == err_type:
            self.errors_streak += 1
        else:
            self.errors_streak = 1
            self.last_error_type = err_type


class AbstractCrudAioObject(baseobjects.AbstractCrudObject):
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
        :rtype: list[AbstractCrudAioObject]
        """
        api = api or FacebookAdsAsyncApi.get_default_api()
        params = dict(params or {})
        cls._assign_fields_to_params(fields, params)
        params['ids'] = ','.join(map(str, ids))
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

    def iterate_edge_aio(self, target_objects_class, fields=None, params=None,
                         include_summary=True, limit=1000):
        """
        Returns EdgeIterator with argument self as source_object and
        the rest as given __init__ arguments.

        Note: list(iterate_edge_aio(...)) can prefetch all the objects.
        """
        source_object = self
        iterator = AioEdgeIterator(
            source_object,
            target_objects_class,
            fields=fields,
            params=params,
            include_summary=include_summary,
            limit=limit
        )
        iterator.submit_next_page_aio()
        return iterator

    def iterate_edge_async_aio(self, target_objects_class, fields=None, params=None):
        """
        Returns an AsyncAioJob which can be checked using remote_read()
        to verify when the job is completed and the result ready to query
        or download using get_result()
        """
        if not params:
            params = {}
        else:
            params = dict(params)
        self.__class__._assign_fields_to_params(fields, params)

        # To force an async response from an edge, do a POST instead of GET.
        # The response comes in the format of an AsyncAioJob which
        # indicates the progress of the async request.
        response = self.get_api_assured().call(
            'POST',
            (self.get_id_assured(), target_objects_class.get_endpoint()),
            params=params,
        ).json()

        # AsyncAioJob stores the real iterator
        # for when the result is ready to be queried
        result = AsyncAioJob(target_objects_class)

        if 'report_run_id' in response:
            response['id'] = response['report_run_id']
        result._set_data(response)
        return result


class AdUser(AbstractCrudAioObject, baseobjects.AdUser):
    pass


class Page(AbstractCrudAioObject, baseobjects.Page):
    pass


class AdAccount(AbstractCrudAioObject, baseobjects.AdAccount):
    def get_activities_aio(self, fields=None, params=None):
        """Returns iterator over Activity's associated with this account."""
        return self.iterate_edge_aio(baseobjects.Activity, fields, params)

    def get_ad_users_aio(self, fields=None, params=None):
        """Returns iterator over AdUser's associated with this account."""
        return self.iterate_edge_aio(baseobjects.AdUser, fields, params)

    def get_campaigns_aio(self, fields=None, params=None):
        """Returns iterator over Campaign's associated with this account."""
        return self.iterate_edge_aio(baseobjects.Campaign, fields, params)

    def get_ad_sets_aio(self, fields=None, params=None):
        """Returns iterator over AdSet's associated with this account."""
        return self.iterate_edge_aio(baseobjects.AdSet, fields, params)

    def get_ads_aio(self, fields=None, params=None):
        """Returns iterator over Ad's associated with this account."""
        return self.iterate_edge_aio(baseobjects.Ad, fields, params)

    def get_ad_conversion_pixels_aio(self, fields=None, params=None):
        """
        Returns iterator over AdConversionPixels associated with this account.
        """
        return self.iterate_edge_aio(baseobjects.AdConversionPixel, fields, params)

    def get_ad_creatives_aio(self, fields=None, params=None):
        """Returns iterator over AdCreative's associated with this account."""
        return self.iterate_edge_aio(baseobjects.AdCreative, fields, params)

    def get_ad_images_aio(self, fields=None, params=None):
        """Returns iterator over AdImage's associated with this account."""
        return self.iterate_edge_aio(baseobjects.AdImage, fields, params)

    def get_insights_aio(self, fields=None, params=None, async=False):
        if async:
            return self.iterate_edge_async_aio(
                Insights,
                fields,
                params
            )
        return self.iterate_edge_aio(
            Insights,
            fields,
            params,
            include_summary=False,
        )

    def get_broad_category_targeting_aio(self, fields=None, params=None):
        """
        Returns iterator over BroadCategoryTargeting's associated with this
        account.
        """
        return self.iterate_edge_aio(baseobjects.BroadCategoryTargeting, fields, params)

    def get_connection_objects_aio(self, fields=None, params=None):
        """
        Returns iterator over ConnectionObject's associated with this account.
        """
        return self.iterate_edge_aio(baseobjects.ConnectionObject, fields, params)

    def get_custom_audiences_aio(self, fields=None, params=None):
        """
        Returns iterator over CustomAudience's associated with this account.
        """
        return self.iterate_edge_aio(baseobjects.CustomAudience, fields, params)

    def get_partner_categories_aio(self, fields=None, params=None):
        """
        Returns iterator over PartnerCategory's associated with this account.
        """
        return self.iterate_edge_aio(baseobjects.PartnerCategory, fields, params)

    def get_rate_cards_aio(self, fields=None, params=None):
        """Returns iterator over RateCard's associated with this account."""
        return self.iterate_edge_aio(baseobjects.RateCard, fields, params)

    def get_reach_estimate_aio(self, fields=None, params=None):
        """
        Returns iterator over ReachEstimate's associated with this account.
        """
        return self.iterate_edge_aio(baseobjects.ReachEstimate, fields, params)

    def get_transactions_aio(self, fields=None, params=None):
        """Returns iterator over Transaction's associated with this account."""
        return self.iterate_edge_aio(baseobjects.Transaction, fields, params)

    def get_ad_preview_aio(self, fields=None, params=None):
        """Returns iterator over previews generated under this account."""
        return self.iterate_edge_aio(baseobjects.GeneratePreview, fields, params)

    def get_ad_labels_aio(self, fields=None, params=None):
        """
        Returns all the ad labels associated with the ad account
        """
        return self.iterate_edge_aio(baseobjects.AdLabel, fields, params)

    def get_ad_creatives_by_labels_aio(self, fields=None, params=None):
        """
        Returns the ad creatives associated with the ad AdLabel
        """
        return self.iterate_edge_aio(baseobjects.AdCreativesByLabels, fields, params)

    def get_ads_by_labels_aio(self, fields=None, params=None):
        """
        Returns the ad Groups associated with the ad AdLabel
        """
        return self.iterate_edge_aio(baseobjects.AdsByLabels, fields, params)

    def get_adsets_by_labels_aio(self, fields=None, params=None):
        """
        Returns the ad sets associated with the ad AdLabel
        """
        return self.iterate_edge_aio(baseobjects.AdSetsByLabels, fields, params)

    def get_campaigns_by_labels_aio(self, fields=None, params=None):
        """
        Returns the ad campaigns associated with the ad AdLabel
        """
        return self.iterate_edge_aio(baseobjects.CampaignsByLabels, fields, params)

    def get_minimum_budgets_aio(self, fields=None, params=None, limit=1000):
        """
        Returns the minimum budget associated with the AdAccount
        """
        return self.iterate_edge_aio(baseobjects.MinimumBudget, fields, params,
                                     limit=limit)

    def get_ad_place_page_sets_aio(self, fields=None, params=None):
        """
        Returns the ad place page sets associated with the AdAccount
        """
        return self.iterate_edge_aio(baseobjects.AdPlacePageSet, fields, params)

    def get_custom_conversions_aio(self, fields=None, params=None):
        """
        Returns the custom conversions associated with the AdAccount
        """
        return self.iterate_edge_aio(baseobjects.CustomConversion, fields, params)


class AdAccountGroup(AbstractCrudAioObject, baseobjects.AdAccountGroup):
    pass


class AdAccountGroupUser(AbstractCrudAioObject, baseobjects.AdAccountGroupUser):
    pass


class Campaign(AbstractCrudAioObject, baseobjects.Campaign):
    pass


class AdSet(AbstractCrudAioObject, baseobjects.AdSet):
    pass


class Ad(AbstractCrudAioObject, baseobjects.Ad):
    pass


class AdConversionPixel(AbstractCrudAioObject, baseobjects.AdConversionPixel):
    pass


class AdsPixel(AbstractCrudAioObject, baseobjects.AdsPixel):
    pass


class AdCreative(AbstractCrudAioObject, baseobjects.AdCreative):
    pass


class AdImage(AbstractCrudAioObject, baseobjects.AdImage):
    pass


class AdVideo(AbstractCrudAioObject, baseobjects.AdVideo):
    pass


class ClickTrackingTag(AbstractCrudAioObject, baseobjects.ClickTrackingTag):
    pass


class CustomAudience(AbstractCrudAioObject, baseobjects.CustomAudience):
    pass


class LookalikeAudience(AbstractCrudAioObject, baseobjects.LookalikeAudience):
    pass


class PartnerCategory(AbstractCrudAioObject, baseobjects.PartnerCategory):
    pass


class ReachFrequencyPrediction(AbstractCrudAioObject, baseobjects.ReachFrequencyPrediction):
    pass


class Business(AbstractCrudAioObject, baseobjects.Business):
    pass


class ProductCatalog(AbstractCrudAioObject, baseobjects.ProductCatalog):
    pass


class ProductFeed(AbstractCrudAioObject, baseobjects.ProductFeed):
    pass


class ProductFeedUpload(AbstractCrudAioObject, baseobjects.ProductFeedUpload):
    pass


class ProductFeedUploadError(AbstractCrudAioObject, baseobjects.ProductFeedUploadError):
    pass


class ProductSet(AbstractCrudAioObject, baseobjects.ProductSet):
    pass


class ProductGroup(AbstractCrudAioObject, baseobjects.ProductGroup):
    pass


class Product(AbstractCrudAioObject, baseobjects.Product):
    pass


class ProductAudience(AbstractCrudAioObject, baseobjects.ProductAudience):
    pass


class AdLabel(AbstractCrudAioObject, baseobjects.AdLabel):
    pass


class Lead(AbstractCrudAioObject, baseobjects.Lead):
    pass


class LeadgenForm(AbstractCrudAioObject, baseobjects.LeadgenForm):
    pass


class AdPlacePageSet(AbstractCrudAioObject, baseobjects.AdPlacePageSet):
    pass


class CustomConversion(AbstractCrudAioObject, baseobjects.CustomConversion):
    pass


class Insights(AbstractCrudAioObject, baseobjects.Insights):
    # TODO: implement async get method
    pass


class AsyncAioJob(AbstractCrudAioObject, baseobjects.AsyncJob):

    def get_result(self, params=None):
        """
        Gets the final result from an async job
        Accepts params such as limit
        """
        return self.iterate_edge_aio(
            self.target_objects_class,
            params=params,
            include_summary=False,
        )
