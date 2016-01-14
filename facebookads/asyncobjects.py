from facebookads.objects import *
from facebookads.asyncapi import FacebookAdsAsyncApi


class AsyncEdgeIterator(EdgeIterator):

    """AsyncEdgeIterator is an iterator over an object's connections
    that doesn't generate new objects, it holds a list of dicts with the data.

    Examples:
        >>> me = AdUser('me')
        >>> my_accounts = [act for act in EdgeIterator(me, AdAccount)]
        >>> my_accounts
        [<AdAccount act_abc>, <AdAccount act_xyz>]
    """

    def __init__(
        self,
        source_object,
        target_objects_class,
        fields=None,
        params=None,
        include_summary=True,
    ):
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
        self.params = dict(params or {})
        target_objects_class._assign_fields_to_params(fields, self.params)
        self._source_object = source_object
        self._target_objects_class = target_objects_class
        self._path = (
            source_object.get_id_assured(),
            target_objects_class.get_endpoint(),
        )
        self._queue = []
        self._finished_iteration = False
        self._total_count = None
        self._include_summary = include_summary
        self._futures = []

    def __repr__(self):
        return str(self._queue)

    def __len__(self):
        return len(self._queue)

    def __iter__(self):
        return self

    def __next__(self):
        # Load next page at end.
        # If load_next_page returns False, raise StopIteration exception
        if not self._queue and not self.load_next_page():
            raise StopIteration()

        return self._queue.pop(0)

    # Python 2 compatibility.
    next = __next__

    def __getitem__(self, index):
        return self._queue[index]

    def total(self):
        if self._total_count is None:
            raise FacebookUnavailablePropertyException(
                "Couldn't retrieve the object total count for that type "
                "of request.",
            )
        return self._total_count

    def load_next_page(self):
        """Queries server for more nodes and loads them into the internal queue.

        Returns:
            True if successful, else False.
        """
        if self._finished_iteration:
            return False

        if self._include_summary:
            if 'summary' not in self.params:
                self.params['summary'] = True

        response = self._source_object.get_api_assured().call(
            'GET',
            self._path,
            params=self.params,
        ).json()

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

        new_cnt = self.build_objects_from_response(response)
        return new_cnt > 0

    def build_objects_from_response(self, response):
        if 'data' in response and isinstance(response['data'], list):
            new_cnt = len(response['data'])
            self._queue += response['data']
        else:
            data = response['data'] if 'data' in response else response
            self._queue.append(data)
            new_cnt = 1

        return new_cnt


class AbstractCrudAsyncObject(AbstractCrudObject):
    """
    Extends AbstractObject and implements async iter_edge operation.

    Attributes:
        parent_id: The object's parent's id. (default None)
        api: The api instance associated with this object. (default None)
    """

    def __init__(self, fbid=None, parent_id=None, api=None):
        """Initializes a CRUD object.

        Args:
            fbid (optional): The id of the object ont the Graph.
            parent_id (optional): The id of the object's parent.
            api (optional): An api object which all calls will go through. If
                an api object is not specified, api calls will revert to going
                through the default api.
        """
        super(AbstractCrudAsyncObject, self).__init__(
                fbid=fbid, parent_id=parent_id, api=api)

    @classmethod
    def get_by_ids(cls, ids, params=None, fields=None, api=None):
        # TODO: do we need this to be async?
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
        # FIXME: implement submit
        iterator.submit()
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

        Example:
        >>> job = object.iterate_edge_async(
                TargetClass, fields, params, async=True)
        >>> time.sleep(10)
        >>> job.remote_read()
        >>> if job:
                result = job.read_result()
                print result
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
        result = AsyncJob(target_objects_class, api=self.get_api())

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


class AdUser(CannotCreate, CannotDelete, CannotUpdate, AbstractCrudAsyncObject, AdUser):
    pass


class Page(CannotCreate, CannotDelete, CannotUpdate, AbstractCrudAsyncObject, Page):
    pass


class AdAccount(CannotCreate, CannotDelete, HasAdLabels, AbstractCrudAsyncObject, AdAccount):
    pass


class Campaign(CanValidate, HasStatus, HasObjective, HasAdLabels, CanArchive,
               AbstractCrudAsyncObject, Campaign):
    pass


class AdSet(CanValidate, HasStatus, CanArchive, HasAdLabels,
            AbstractCrudAsyncObject, AdSet):
    pass


class Ad(HasStatus, CanArchive, HasAdLabels, AbstractCrudAsyncObject, Ad):
    pass


class AdConversionPixel(AbstractCrudAsyncObject, AdConversionPixel):
    pass


class AdsPixel(CannotUpdate, CannotDelete, AbstractCrudAsyncObject, AdsPixel):
    pass


class AdCreative(HasAdLabels, AbstractCrudAsyncObject, AdCreative):
    pass


class CustomAudience(AbstractCrudAsyncObject, CustomAudience):
    pass


class LookalikeAudience(AbstractCrudAsyncObject, LookalikeAudience):
    pass


class Business(CannotCreate, CannotDelete, AbstractCrudAsyncObject, Business):
    pass


class ProductCatalog(AbstractCrudAsyncObject, ProductCatalog):
    pass


class ProductFeed(AbstractCrudAsyncObject, ProductFeed):
    pass


class ProductSet(AbstractCrudAsyncObject, ProductSet):
    pass


class ProductGroup(AbstractCrudAsyncObject, ProductGroup):
    pass


class Product(AbstractCrudAsyncObject, Product):
    pass


class ProductAudience(AbstractCrudAsyncObject, ProductAudience):
    pass


class Insights(CannotCreate, CannotDelete, CannotUpdate, AbstractCrudAsyncObject, Insights):
    # TODO: implement async get method
    pass


class AsyncJob(CannotCreate, AbstractCrudAsyncObject, AsyncJob):

    class Field(object):
        id = 'id'
        async_status = 'async_status'
        async_percent_completion = 'async_percent_completion'

    def __init__(self, target_objects_class, api=None):
        AbstractCrudObject.__init__(self, api=api)
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
