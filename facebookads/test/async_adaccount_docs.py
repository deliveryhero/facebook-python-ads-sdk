'''
Unit tests for the Python Facebook Ads API SDK.

How to run:
    python -m facebookads.test.async_adaccount_docs
'''

import sys
import json
import unittest

from facebookads.utils.httpretries import retry_policy
from facebookads.test.async_docs_utils import AsyncDocsTestCase, AsyncDocsDataStore, \
    AdAccount, Insights, AdLabel, FacebookAdsAsyncApi


class AdAccountAsyncDocsTestCase(AsyncDocsTestCase):
    def setUp(self):
        # Create Campaigns
        campaign = self.create_campaign(1)
        self.create_campaign(2)
        # Create AdSets
        adset = self.create_adset(1, campaign)
        self.create_adset(2, campaign)
        # Create Creatives
        creative1 = self.create_creative(1)
        creative2 = self.create_creative(2)
        # Create AdGroups
        ad = self.create_ad(1, adset, creative1)
        self.create_ad(2, adset, creative2)
        AsyncDocsDataStore.set('ad_id', ad.get_id())
        # Create Ad Labels
        adlabel = self.create_adlabel()
        AsyncDocsDataStore.set('adlabel_id', adlabel['id'])
        # Create AdImage
        image = self.create_image()
        AsyncDocsDataStore.set('ad_account_image_hash', image['hash'])

    def test_get_insights(self):
        account = AdAccount(AsyncDocsDataStore.get('adaccount_id'))
        insights = account.get_insights(fields=[
            Insights.Field.campaign_id,
            Insights.Field.unique_clicks,
            Insights.Field.impressions,
        ], params={
            'level': Insights.Level.campaign,
            'date_preset': Insights.Preset.last_week,
        })
        self.store_response(insights)

    def atest_get_campaigns_by_labels(self):
        account = AdAccount(AsyncDocsDataStore.get('adaccount_id'))
        adlabel_id = AsyncDocsDataStore.get('adlabel_id')
        params = {'ad_label_ids': [adlabel_id], 'operator': 'ALL'}
        campaigns = account.get_campaigns_by_labels(fields=[
            AdLabel.Field.name,
            AdLabel.Field.id,
        ], params=params)
        self.store_response(campaigns)

    def atest_get_minimum_budgets(self):
        account = AdAccount(AsyncDocsDataStore.get('adaccount_id'))
        min_budgets = account.get_minimum_budgets()
        self.store_response(min_budgets)


if __name__ == '__main__':
    handle = open(AsyncDocsDataStore.get('filename'), 'w')
    handle.write('')
    handle.close()

    try:
        config_file = open('./autogen_docs_config.json')
    except IOError:
        print("No config file found, skipping docs tests")
        sys.exit()
    config = json.load(config_file)
    config_file.close()

    FacebookAdsAsyncApi.init(
        config['app_id'],
        config['app_secret'],
        config['access_token'],
        config['adaccount_id'],
        pool_maxsize=10,
        max_retries=retry_policy()
    )

    AsyncDocsDataStore.set('adaccount_id', config['adaccount_id'])
    AsyncDocsDataStore.set('adaccount_id_int', config['adaccount_id_int'])
    AsyncDocsDataStore.set('business_id', config['business_id'])
    AsyncDocsDataStore.set('ca_id', config['ca_id'])
    AsyncDocsDataStore.set('dpa_catalog_id', config['dpa_catalog_id'])
    AsyncDocsDataStore.set('dpa_set_id', config['dpa_set_id'])
    AsyncDocsDataStore.set('dpa_feed_id', config['dpa_feed_id'])
    AsyncDocsDataStore.set('dpa_upload_id', config['dpa_upload_id'])
    AsyncDocsDataStore.set('as_user_id', config['as_user_id'])
    AsyncDocsDataStore.set('page_id', config['page_id'])
    AsyncDocsDataStore.set('pixel_id', config['pixel_id'])

    unittest.main()
