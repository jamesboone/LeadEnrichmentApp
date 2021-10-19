import logging
import ipdb
import requests
import urllib
import re
logger = logging.getLogger(__name__)


class WalkScore(object):

    def __init__(self):
        logger.debug('walkscore init')
        self.base_url = 'http://api.walkscore.com'
        self.token_ix = 0
        self.quota_errors = 0

    def get_walkscore_data(self, data):
        logger.debug('get_walkscore_data')

        self.api_keys = [
            '2d6d11cb1589331c1595cc42fa315',
            'dd7b1932e7a579c8779a9094d337e',
            'ffd6f9abcf84872116b4cc2dfcf31',
            'b728763203418d081f140357696e'
        ]

        self.token_ix = (self.token_ix + 1) % len(self.api_keys)

        address = urllib.urlencode(
            {'address': '%s %s %s %s' % (data.get('Street'),
                                         data.get('City'),
                                         data.get('State'),
                                         data.get('PostalCode'))})

        api_url = "%s/score?format=json&%s&lat=%s&lon=%s&wsapikey=%s" % (
            self.base_url, address, data.get('Latitude'), data.get('Longitude'), self.api_keys[self.token_ix])

        try:
            response = requests.get(api_url)
        except:
            logger.error("exception during requests.get(api_url)")

        if response.status_code == 400 and "quota" in response.text.lower().encode('utf-8'):
            self.quota_errors += 1
            if self.quota_errors > 10:
                self.api_keys.pop(self.token_ix)
            return self.get_walkscore_data(data)

        elif response.status_code != 200:
            logger.error("Received HTTP status code %s - %s" % (response.status_code, response.text, ))

        return self._get_object_data(response.json(), data)

    def _get_object_data(self, walkscore_data, data):
        logger.debug('_get_object_data')
        return {
            'Walkscore_Score__c': walkscore_data.get('walkscore') if walkscore_data.get('walkscore') else 0,
            'Walkscore_Desc__c': walkscore_data.get(
                'description').encode('utf-8') if walkscore_data.get('description') else None
        }
        return data


class TransitScore(object):

    def __init__(self):
        logger.debug('transitscore init')
        self.base_url = 'http://transit.walkscore.com/transit/score/?'
        self.token_ix = 0
        self.quota_errors = 0

    def get_transitscore_data(self, data):
        logger.debug('get_transitscore_data')

        self.api_keys = [
            '2d6d116bbcb19331c1595cc42fa315',
            'dd7b1932ee7a579c8779a9094d337e',
            'ffd1c56fbcf84872116b4cc2dfcf31',
            'b72221763203418d081f140357696e'
        ]

        self.token_ix = (self.token_ix + 1) % len(self.api_keys)

        url_params = urllib.urlencode({
            'research': 'yes',
            'lat': data.get('Latitude'),
            'lon': data.get('Longitude'),
            'city': data.get('City'),
            'state': data.get('State'),
            'wsapikey': self.api_keys[self.token_ix]
        })

        api_url = "%s%s" % (self.base_url, url_params)

        try:
            response = requests.get(api_url)
        except:
            logger.error("exception during requests.get(api_url)")
            return data

        if response.status_code == 400 and "quota" in response.text.lower().encode('utf-8'):
            self.quota_errors += 1
            if self.quota_errors > 10:
                self.api_keys.pop(self.token_ix)
            return self.get_transitscore_data(data)

        elif response.status_code != 200:
            logger.error("Received HTTP status code %s - %s" % (response.status_code, response.text, ))
            return data

        return self._get_object_data(response.json(), data)

    def _get_object_data(self, transit_data, data):
        logger.debug('get_transit_data')
        Transitscore_Nearby_Routes__c = int(self._get_transit_mode_int(transit_data.get('summary'), 'nearby routes'))
        Transitscore_Bus_Routes__c = int(self._get_transit_mode_int(transit_data.get('summary'), 'bus'))
        Transitscore_Rail_Routes__c = int(self._get_transit_mode_int(transit_data.get('summary'), 'rail'))
        Transitscore_Other_Routes__c = int(self._get_transit_mode_int(transit_data.get('summary'), 'other'))

        return {
            'Transitscore_Score__c': transit_data.get('walkscore') if transit_data.get('walkscore') else 0,
            'Transitscore_Desc__c': transit_data.get('description').encode('utf-8'),
            'Transitscore_Nearby_Routes__c': Transitscore_Nearby_Routes__c if Transitscore_Nearby_Routes__c else 0,
            'Transitscore_Bus_Routes__c': Transitscore_Bus_Routes__c if Transitscore_Bus_Routes__c else 0,
            'Transitscore_Rail_Routes__c': Transitscore_Rail_Routes__c if Transitscore_Rail_Routes__c else 0,
            'Transitscore_Other_Routes__c': Transitscore_Other_Routes__c if Transitscore_Other_Routes__c else 0
        }

    def _get_transit_mode_int(self, summary, mode):
        logger.debug('_get_transit_mode_int')
        regex = '(\d*).%s' % (mode)
        results = re.search(regex, summary)

        if results:
            return results.groups()[0]
        else:
            return 0
