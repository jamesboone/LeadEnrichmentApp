import ipdb
import logging
import gspread
import json
from oauth2client.client import SignedJwtAssertionCredentials
logger = logging.getLogger(__name__)


class ZipCodeUtils(object):
    def __init__(self):
        logger.debug('zipcodeutils init')
        scope = ['https://spreadsheets.google.com/feeds']
        json_key = json.load(open('modules/ZipCodeSheet-7e89399c0453.json'))
        credentials = SignedJwtAssertionCredentials(json_key['client_email'], json_key['private_key'].encode(), scope)
        gspreadclient = gspread.authorize(credentials)
        spreadsheet = gspreadclient.open_by_key('1qf4xfXGtaFdp4soX7c2taIiS5oxnofLm5iCOzOA')
        worksheet = spreadsheet.worksheet('Zip Codes')
        self.zipcode_data = worksheet.get_all_values()

    def get_owner_id(self, owner_id, rep_zip, rekindle=False):
        ''' Uses the zip code doc that outside sales reps use to identify their sales territory.
            If the lead zip code matches a zip in the zip code doc then it doesn't get reassigned,
            if the lead zip does not match, then it gets assigned to inside sales
        '''
        logger.debug('get_owner_id')
        for zipcode in self.zipcode_data:
            if zipcode[0] == rep_zip:
                if '00GE000000369DZ' in owner_id:
                    return zipcode[6]
                if owner_id and not rekindle:
                    return owner_id
                elif not rekindle:
                    return zipcode[6]
        return '00GE0000003QWrg'
