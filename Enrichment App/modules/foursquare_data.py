import logging
import foursquare
from fuzzywuzzy import fuzz
from collections import Counter
import ipdb
logger = logging.getLogger(__name__)


class FourSquare(object):

    def __init__(self):
        logger.debug('foursquare init')
        self.client = foursquare.Foursquare(
            client_id='DFY41EXZJMYTFE0NPTTQCTIBGI4LUSMONCPMTKC',
            client_secret='NU0YLJ5YNLFT0PUXUQPCSLHMFI4F00ABNUXTB')

    def get_foursquare_data(self, data, raidus=500, intent='browse'):
        logger.debug('get_foursquare_data')

        company = data['Company']
        lat = data['Latitude']
        lng = data['Longitude']

        if lat == 0 or lng == 0:
            data['Foursquare_Verified__c'] = False
            return data

        coordinates = '%s, %s' % (lat, lng)

        params={'query': company, 'radius': raidus, 'intent': intent, 'll': coordinates}

        search = self.client.venues.search(params)

        venue = None
        if search.get('venues'):
            venue = self._identify_correct_company(search['venues'], data)

        if venue:
            return self._get_venue_object_data(venue, data)

        return data

    def _identify_correct_company(self, venues, data):
        logger.debug('_identify_correct_company')
        our_name = data.get('Company')
        fuzz_calcs = []
        if len(venues) > 1 and our_name:
            for venue in venues:
                their_name = venue['name']
                fuzz_calcs.append(fuzz.ratio(our_name, their_name))
        checkincount = []
        if len(fuzz_calcs) and Counter(fuzz_calcs)[100] > 1:
            for venue in venues:
                venue_count = venue.get('stats').get('checkinsCount') if venue.get('stats') else 0
                checkincount.append(venue_count)
                return venues[checkincount.index(max(checkincount))]
        return venues[fuzz_calcs.index(max(fuzz_calcs))] if len(venues) > 1 else venues[0]

    def _get_venue_object_data(self, venue, data):
        logger.debug('_get_venue_object_data')

        return {
            'Foursquare_ID__c': venue.get('id', '').encode('utf-8'),
            'Foursquare_Category_Name__c': self._get_primary_category_name(venue.get('categories')),
            'Foursquare_Phone__c': venue.get('phone'),
            'Foursquare_Twitter_Handle__c': venue.get('twitter', '').encode('utf-8'),
            'Foursquare_Name__c': venue.get('name', '').encode('utf-8'),
            'Foursquare_Specials_Count__c': venue.get('specials').get('count') if venue.get('specials').get('count') else 0,
            'Foursquare_Specials_Items__c': len(venue.get('specials').get('items')) if venue.get('specials').get('items') else 0,
            'Foursquare_Stats_Checkins__c': venue.get('stats').get('checkinsCount') if venue.get('stats').get('checkinsCount') else 0,
            'Foursquare_Stats_Tips__c': venue.get('stats').get('tipCount') if venue.get('stats').get('tipCount') else 0,
            'Foursquare_Stats_Users__c': venue.get('stats').get('usersCount') if venue.get('stats').get('usersCount') else 0,
            'Foursquare_Verified__c': 1 if venue.get('verified') else 0,
            'Foursquare_Collected_Website__c': venue.get('url', '').encode('utf-8')
        }

    def _get_primary_category_name(self, categories):
        logger.debug('_get_primary_category_name')
        if categories and len(categories) > 1:
            for cat in categories:
                if cat['primary']:
                    return cat['name']
        else:
            return categories[0]['name'].encode('utf-8') if categories else None
