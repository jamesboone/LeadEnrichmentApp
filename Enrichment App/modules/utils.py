import logging
import ipdb
import googlemaps
from datetime import datetime
from modules.realvalidation import get_real_validation


logger = logging.getLogger(__name__)


def needs_update(data, days_since_enrich=30, real_validation=False):
    ''' saves time and money by not checking geo and real validation if it has been done recently
    '''
    logger.debug('needs_update')
    # logger.debug("FB_Enriched_Date__c: %s" % (datetime.strptime(data.get('FB_Enriched_Date__c')), ))
    try:
        # if geo:
        #     if (data.get('FB_Enriched_Date__c') and data.get('Latutude')) and (
        #         abs((datetime.today() - datetime.strptime(str(data.get(
        #             'FB_Enriched_Date__c')), '%Y-%m-%d')).days) < abs((
        #                 datetime.today() - datetime.strptime('4/15/2016', '%d/%m/%Y')).days)):
        #             return False
        # elif real_validation:
        if (data.get('FB_Enriched_Date__c') and data.get('Phone_Disposition__c')) and (
            abs((datetime.today() - datetime.strptime(str(data.get(
                'FB_Enriched_Date__c')), '%Y-%m-%d')).days) < days_since_enrich):
            return False
    except:
        logger.error("error during def needs_update")
        pass
    return True


def get_best_phone(data, phone, other_phone):
    ''' if another phone number is found, we use the one that is connected giving
        priority to the original phone number
    '''
    logger.debug('get_best_phone')
    if needs_update(data, real_validation=True):
        data = get_real_validation(data, phone)

    if data.get('Phone_Disposition__c') not in [
            "connected", "connected-75", "busy"] and other_phone:
        data = get_real_validation(data, other_phone)
        if data['Phone_Disposition__c'] in ["connected", "connected-75", "busy"]:
            data['Phone'] = other_phone
    return data


def set_name(name):
    ''' formats the name for our lead
    '''
    logger.debug('set_name')
    if not name or name == 'Unknown':
        return '?'
    else:
        return name


def get_geo(data):
    ''' collects the lat and lng of our leads with google places api
    '''
    logger.debug('get_geo')
    key = "AIzaSyA9jguf6ptHOG_3wrDzm1tHEnpnLcdPVjs"
    gmaps = googlemaps.Client(key)
    lat = 0
    lng = 0
    try:
        address = "%s, %s, %s" % (data.get("Street"), data.get('City'), data.get('State'),)
        geocode_result = gmaps.geocode(address)
        if geocode_result:
            lat = geocode_result[0]['geometry']['location']['lat']
            lng = geocode_result[0]['geometry']['location']['lng']
    except:
        lat = data.get("Latitude")
        lng = data.get("Longitude")
    return (lat, lng)


def map_rekindle_fields(data, initiative):
    logger.debug('map_fields')
    company = None
    if data[14] and '-' in data[14]:
        company = data[14][:-1]
    return {'Acct_Desc': data[0],
            'Campaign_Category__c': data[1],
            'campaign_name': data[2],
            'City': data[3],
            'Campaign_Comp_Level__c': data[4],
            'Date_DM_Reached__c': data[5],
            'Date_Lost__c': data[6],
            'Date_Meeting_Held__c': data[7],
            'Date_Meeting_Scheduled__c': data[8],
            'Description': data[9],
            'Id': data[10],
            'lead_Source': data[11],
            'merchant_name': data[12],
            'mobile_phone': data[13],
            'Company': company if company else data[14],
            'Owner_ID__c': data[15],
            'Owner_Role__c': data[16],
            'Phone': data[17],
            'PostalCode': data[18],
            'Reason_Lost__c': data[19],
            'FirstName': data[20].split()[0],
            'LastName': data[20].split()[1],
            'State': data[21],
            'Street': data[22],
            'Store_Desc': data[23],
            'Yelp_Page__c': data[24],
            'contact_id': data[25],
            'rekindle': True,
            'marketing_initiative': initiative}


def fb_yelp_claimed_status(data, fb_data):
    return {
        'FB_and_yelp_not_claimed__c': 1 if fb_data.get('page_data', {}).get('is_unclaimed', None) and not data.get('Yelp_Is_Claimed__c') else 0,
        'FB_and_yelp_claimed__c': 1 if not fb_data.get('page_data', {}).get('is_unclaimed', None) and data.get('Yelp_Is_Claimed__c') else 0
    }


def intWithCommas(x):
    if type(x) not in [type(0), type(0L)]:
        raise TypeError("Parameter must be an integer.")
    if x < 0:
        return '-' + intWithCommas(-x)
    result = ''
    while x >= 1000:
        x, r = divmod(x, 1000)
        result = ",%03d%s" % (r, result)
    return "%d%s" % (x, result)
