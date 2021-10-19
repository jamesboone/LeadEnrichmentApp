import logging
import json
from pprint import pprint
from simple_salesforce import Salesforce, SalesforceGeneralError
import datetime
import ipdb
import pytz
import requests
import time

from modules.data_cleaner import DataCleaner
from modules.facebook_tool import Facebook as fb
from modules.foursquare_data import FourSquare
from modules import utils
from modules.walk_transit_score import WalkScore, TransitScore

logger = logging.getLogger(__name__)


class SalesForceAPI(object):
    def __init__(self, validator):
        logger.debug('salesforce api init')
        self.instance = self.authenticate()
        self.validator = validator

    def authenticate(self):
        logger.debug('salesforce authentication')
        with open('config.json', 'r') as f:
            config = json.load(f)
        username = config.get("salesforce").get("username")
        password = config.get("salesforce").get("password")
        token = config.get("salesforce").get("token")

        return Salesforce(username=username, password=password, security_token=token)

    def create_rekindle_inbound_opp_1(self, data, lead_info):
        logger.debug('create_rekindle_inbound_opp_1')
        logger.debug('processing opp: %s - %s' % (data['Id'], data.get('Company'),))
        try:
            leads_associated_to_opp, rekindled_lead_date = self.check_for_leads(
                data.get('Company'), data.get('Phone'), data.get('mobile_phone'))

            opp_data, rekindled_opp_date = self.get_opp(data['Id'], data.get('campaign_name'))

            logger.info('previously rekindled opp: %s' % (bool(rekindled_opp_date), ))
            logger.info('previously rekindled lead: %s\n' % (bool(rekindled_lead_date), ))
            if not rekindled_opp_date and not rekindled_lead_date:
                if not data.get('Phone_Disposition__c'):
                    data.update(utils.get_best_phone(data, data.get('Phone'), data.get('mobile_phone')))
                # recent_lead_info = None
                # ipdb.set_trace()
                if leads_associated_to_opp:
                    lead_ids = self.get_lead_ids(leads_associated_to_opp[0])
                    self.flag_for_deletion(lead_ids)
                    # if recent_lead_info:
                    #     lead_info = self.map_lead_data(lead_info, recent_lead_info)

                Campaign_Category__c = data.get(
                    'Campaign_Category__c') if data.get('Campaign_Category__c') else 'Rekindle'
                Campaign_Name = data.get('campaign_name')
                Reason_Lost__c = opp_data.get('Reason_Lost__c')
                Lost_Subexplanation__c = opp_data.get('Lost_Subexplanation__c')
                Meeting_Missed__c = opp_data.get('Meeting_Missed__c')
                Stage_Notes__c = opp_data.get('Stage_Notes__c')
                Description = opp_data.get('Description')
                Acct_Desc = opp_data.get('Acct_Desc')
                Store_Desc = opp_data.get('Store_Desc')
                Desc = '%s | %s | %s | %s' % (Store_Desc, Acct_Desc, Description, Stage_Notes__c,)
                Sub_Industry__c = opp_data.get('Sub_Industry__c')
                Acct_Last_Contact_Attempt__c = opp_data.get('Acct_Last_Contact_Attempt__c')
                # FromLead__c = data['Id']
                Detail_Explanation__c = opp_data.get('Detail_Explanation__c')
                Date_DM_Reached__c = opp_data.get('Date_DM_Reached__c')
                Date_Lost__c = opp_data.get('Date_Lost__c')
                Date_Meeting_Held__c = opp_data.get('Date_Meeting_Held__c')
                Date_Meeting_Scheduled__c = opp_data.get('Date_Meeting_Scheduled__c')

                opp_url = 'https://na9.salesforce.com/%s' % (data['Id'],)

                description = '''RekinINFO: %s %s | Campaign Cat: %s | Campaign Name: %s | \
                Reas. Lost: %s | SubExpl: %s | Meet. Miss: %s | Notes: %s | Industry: %s | Sub Ind: %s | \
                Last Attempt: %s | Opp: %s | Detail Expl: %s | Date DM: %s | Date Set: %s | \
                Date Held: %s | Date Lost: %s''' % (
                    data.get('FirstName'), data.get('LastName'), Campaign_Category__c, Campaign_Name,
                    Reason_Lost__c, Lost_Subexplanation__c, Meeting_Missed__c, Desc,
                    opp_data['Industry__c'], Sub_Industry__c, Acct_Last_Contact_Attempt__c, opp_url,
                    Detail_Explanation__c, Date_DM_Reached__c, Date_Meeting_Scheduled__c, Date_Meeting_Held__c,
                    Date_Lost__c,)

                lead_info['Rekindle_Date_1__c'] = datetime.datetime.now(tz=pytz.utc).astimezone(
                    pytz.timezone('US/Pacific')).strftime('%Y-%m-%d')
                lead_info['Lead_Description__c'] = description
                lead_info['FirstName'] = data.get('merchant_name').split()[0] if data.get('merchant_name') else '?'
                lead_info['LastName'] = data.get('merchant_name').split()[1] if data.get(
                    'merchant_name') and len(data.get('merchant_name').split()) == 2 else '?'
                lead_info['Yelp_Page__c'] = opp_data.get(
                    'Yelp_Page__c').ecode('utf-8') if opp_data.get('Yelp_Page__c') else ''
                lead_info['PostalCode'] = data.get('PostalCode')
                lead_info['Company'] = data.get('Company')
                lead_info['Street'] = data.get('Street')
                lead_info['City'] = data.get('City')
                lead_info['State'] = data.get('State')
                lead_info['Phone_Disposition__c'] = data['Phone_Disposition__c']
                lead_info['Industry'] = opp_data.get('Industry__c')
                lead_info['Lead_Source_Detail__c'] = opp_data.get('Lead_Source_Detail__c')
                lead_info['Is_Yelp_Advertiser__c'] = opp_data.get('Is_Yelp_Advertiser__c')
                lead_info['Company_owned_locations__c'] = opp_data.get('of_Locations__c')
                lead_info['Next_Step__c'] = opp_data.get('Next_Step__c')
                lead_info['Campaign_Comp_Level__c'] = 'Cold'
                lead_info['Website'] = opp_data.get('Website__c')
                lead_info['Phone'] = data.get('Phone')
                lead_info['FB_Enriched_Date__c'] = (
                    datetime.datetime.today().date() - datetime.timedelta(hours=7)).strftime('%Y-%m-%d')
                lead_info['Marketing_Approved_First_Date__c'] = (
                    datetime.datetime.today().date() - datetime.timedelta(hours=7)).strftime('%Y-%m-%d')


                if 'Cold' in data.get('Campaign_Comp_Level__c'):
                    logger.debug('\nCreating Rekindled Cold Opp 1')
                    lead_info['Campaign_Category__c'] = 'Rekindled Cold Opp 1'
                    lead_info['Campaign_ID__c'] = '701E0000000BWPj'  # Rekindled Cold Opportunities - Inside
                else:
                    logger.debug('\nCreating Rekindled Inbound Opp 1')
                    lead_info['Campaign_Category__c'] = 'Rekindled Inbound Opp 1'
                    lead_info['Campaign_ID__c'] = '701E0000000BWPK'  # Rekindled Inbound Opportunities - Inside

                if lead_info.get('Lead_Invalid_Reason__c'):  # means it has successfully gone through enrich.py
                    if lead_info['Lead_Invalid_Reason__c'] == 'No FB Data':
                        lead_info['FB_Valid_Lead__c'], lead_info['Lead_Invalid_Reason__c'] = self.validator.validate_lead(data, None, True)
                    # this method is currently only for inside so it isn't that important to run zipcode logic
                    # lead_info['OwnerId'] = zip_data.get_owner_id(data['Owner_ID__c'], data['PostalCode'], rekindle=True)  # NOQA
                    lead_info['OwnerId'] = '00GE0000003QWrg'
                    lead_info['Marketing_Initiative__c'] = data.get('marketing_initiative')
                    lead_info['Marketing_Approved_Last_Date__c'] = (
                        datetime.datetime.today().date() - datetime.timedelta(hours=7)).strftime('%Y-%m-%d')
                    lead_info['MobilePhone'] = data.get('mobile_phone')
                else:
                    logger.debug("rekindle didn't go through enrichment")
                    ipdb.set_trace()

                lead_id = self.create_lead(lead_info)
                logger.debug('Created lead with id: %s from opp: %s as a %s' % (
                    lead_id, data['Id'], lead_info['Campaign_Category__c']))
                self.update_opp(data['Id'], lead_info=lead_info)
                self.update_acct(opp_data['AccountId'], lead_info['Phone'], lead_info['MobilePhone'])
                self.update_contact(data.get('contact_id'), lead_info['Phone'], lead_info['MobilePhone'])
                self.update_campaign_history(lead_info['Campaign_ID__c'], lead_id)
                logger.info('Rekindle Processed: opp: %s -> lead: %s' % (data['Id'], lead_id))
                return

            if rekindled_lead_date and not rekindled_opp_date:
                logger.debug('updating rekindled opp because lead has already been rekindled')
                self.update_opp(data['Id'], lead_info=lead_info, rekin_date=rekindled_opp_date)

        except:
            logger.exception("Error during create_rekindle_inbound_opp_1")
            ipdb.set_trace()

    def check_for_leads(self, company, phone, mobile):
        logger.debug('check_for_leads')
        quick_search = []
        search = '"%s" %s' % (company, phone,)
        try:
            quick_search = self.instance.quick_search(search)
        except SalesforceGeneralError:
            pass

        if not quick_search:
            try:
                search = '%s' % (phone,)
                quick_search = self.instance.quick_search(search)
            except SalesforceGeneralError:
                pass

        if not quick_search:
            try:
                search = '%s' % (mobile,)
                quick_search = self.instance.quick_search(search)
            except SalesforceGeneralError:
                pass

        leads_to_update = []

        if quick_search:
            for obj in quick_search:
                if str(obj['attributes']['type']) == 'Lead':
                    lead_obj = self.get_lead(obj['Id'])
                    leads_to_update.append((obj['Id'], lead_obj))

        rekindled_date = None
        for lead in leads_to_update:
            if lead[1]['Campaign_Category__c'] and 'rekindle' in lead[1]['Campaign_Category__c'].lower():
                rekindled_date = lead[1]['CreatedDate']
                return (leads_to_update, rekindled_date)
            else:
                lead_id = str(lead[0])
                url = 'https://na9.salesforce.com/%s' % (lead_id,)
                sid = self.instance.session_id
                response = requests.get(url, headers=self.instance.headers, cookies={'sid': sid})
                content = response.content
                for rekin_name in ['Rekindled Cold Opportunities - Inside',
                                   'Rekindled Inbound Opportunities - Inside']:
                    if rekin_name in content:
                        rekindled_date = datetime.datetime.now(
                            tz=pytz.utc).astimezone(pytz.timezone('US/Pacific')).strftime('%Y-%m-%d')
                        break
        return (leads_to_update, rekindled_date)

    def update_opp(self, opp_id, lead_info=None, phone=None, rekin_date=None):
        """
        Update Opportunity
        """
        logger.debug('update_opp')
        if not rekin_date:
            rekin_date = datetime.datetime.now(
                tz=pytz.utc).astimezone(pytz.timezone('US/Pacific')).strftime('%Y-%m-%d')
        opp_update = {}
        opp_update['Rekindle_Date__c'] = rekin_date
        opp_update['phone__c'] = phone
        opp_update['Rekindle_Invalid_Reason__c'] = lead_info.get('Lead_Invalid_Reason__c')
        self.instance.opportunity.update(opp_id, opp_update)

    def update_acct(self, acct_id, phone, mobile):
        """
        Update Account
        """
        logger.debug('update_acct')
        acct_update = {}
        # acct_id = '001E0000019FEpA'
        acct_update['phone'] = phone
        acct_update['mobile__c'] = mobile
        self.instance.account.update(acct_id, acct_update)

    def update_contact(self, contact_id, phone, mobile):
        """
        Update Contact
        """
        if contact_id:
            logger.debug('update_contact')
            contact_update = {}
            # acct_id = '001E0000019FEpA'
            contact_update['phone'] = phone
            contact_update['MobilePhone'] = mobile
            self.instance.contact.update(contact_id, contact_update)

    def update(self, data, lead_info, try_again=False):
        """
        Updates the lead with the lead Id and Dict of info to update.
        lead_info could be something like: {'FirstName': 'booner',
                                            'LastName':'sooner',
                                            'Email':'example@example.com',
                                            'Company': 'Boonerville'}
        """
        try:
            if lead_info:
                result = self.instance.Lead.update(data['Id'].encode('utf-8'), lead_info)
                if try_again:
                    return result
            else:
                logger.warning('\n\n no lead info on: %s\n\n' % (data['Id'], ))
        except SalesforceGeneralError:
            logger.error(SalesforceGeneralError)
            result = self.update(data, lead_info, try_again=True)
        except:
            logger.exception("error in lead update method in salesforce.py")
            # ipdb.set_trace()
            return None
            # self.instance.Lead.update(data['Id'], lead_info)
        return result

    def update_campaign_history(self, campaign_id, lead_id):
        """
            Create campaign member history
        """
        logger.debug('update_campaign_history')
        campaign_member = {}
        campaign_member['campaignid'] = campaign_id
        campaign_member['leadid'] = lead_id
        self.instance.campaignmember.create(campaign_member)

    def create_lead(self, lead_info):
        """
            Create lead
        """
        logger.debug('create_lead')
        response = self.instance.Lead.create(lead_info)
        return response.get('id')

    def get_lead_ids(self, leads_associated_to_opp):
        """
            get_most_recent_lead
        """
        logger.debug('get_lead_ids')
        lead_obj = []
        for lead in leads_associated_to_opp:
            if isinstance(lead, unicode):
                lead_obj.append(lead)
        return lead_obj

    def get_opp(self, opp_id, campaign_name):
        """
        Get opp info
        """
        logger.debug('get_opp')
        rekindled = False
        if 'rekindle' in campaign_name:
            rekindled = True
        data = self.instance.opportunity.get(opp_id)
        if data.get('Rekindle_Date__c') and data.get('Rekindle_Invalid_Reason__c') != 'No FB Data':
            return (None, data['Rekindle_Date__c'])
        elif rekindled:
            return (None, datetime.datetime.now(tz=pytz.utc).astimezone(
                    pytz.timezone('US/Pacific')).strftime('%Y-%m-%d'))
        return (data, None)

    def get_lead(self, lead_id):
        """
        Get lead info
        """
        logger.debug('get_lead')
        return self.instance.Lead.get(lead_id)

    def query(self, query, limit):
        """
        Query for leads
        Ex: "SELECT Id, Email FROM Lead WHERE not Lead.Owner_ID__c like '00GE0000003QWrg%' limit 10"
        """
        logger.debug('query')
        if not query:
            raise Exception("You must provide a query file to enrich data")

        try:
            if limit:
                results = [id for id in self.instance.query(query)['records']]
            else:
                results = [id for id in self.instance.query_all(query)['records']]
            logger.info('Leads to process: %s' % (len(results),))
        except:
            logger.error("error in salesforce:query")
            ipdb.set_trace()
            time.sleep(300)
            self.query(query, limit)
            # self.instance.query_all(query)['records'][0]
        return results

    def flag_for_deletion(self, leads):
        """
        Update lead for future deletion because we are creating a new lead in its place
        """
        logger.debug('flag_for_deletion')

        for lead_id in leads:
            flag = {}
            flag['flagged_for_deletion__c'] = True
            self.instance.lead.update(str(lead_id), flag)

    def invalid_lead(self, data, lead_info):
        ''' appends invalid lead data to a lead that has been identified as invalid
        '''
        # ipdb.set_trace()
        logger.debug('invalid_lead')
        lead_info['FB_Valid_Lead__c'] = 0
        lead_info['FB_Enriched_Date__c'] = (
            datetime.datetime.today().date() - datetime.timedelta(hours=7)).strftime('%Y-%m-%d')
        lead_info['Bad_Data__c'] = 1
        lead_info['Street'] = data['Street'].encode('utf=8') if data['Street'] else None
        lead_info['City'] = data['City'].encode('utf=8') if data['City'] else None
        lead_info['State'] = data['State'].encode('utf=8') if data['State'] else None
        # logger.info('Invalid lead - Reason: %s' % (lead_info['Lead_Invalid_Reason__c'],))
        return self.update(data, lead_info)


class SalesForceBuilder(object):
    def __init__(self, zip_data, sfdc_api, yelp, validator, facebook, marketing_initiative, nearby):
        logger.debug('SalesForceBuilder Init')
        self.zip_data = zip_data
        self.sfdc_api = sfdc_api
        self.yelp = yelp
        self.validator = validator
        self.foursquare = FourSquare()
        self.walkscore = WalkScore()
        self.transitscore = TransitScore()
        self.marketing_initiative = marketing_initiative
        self.nearby = nearby
        self.fb = facebook

    def aggregate_data(self, data, fb_data, lead_info):
        logger.debug('aggregate_data')
        data['fb_phone'] = DataCleaner.clean_new_phone(fb_data.get('page_data'), data.get('Phone'))
        data.update(utils.get_best_phone(data, data.get('Phone'), data.get('fb_phone')))
        data.update(self.yelp.get_yelp_data(data))

        if data.get('Yelp_Phone__c') and data['Yelp_Phone__c'] != data.get('Phone'):
            data.update(utils.get_best_phone(data, data['Phone'], data['Yelp_Phone__c']))

        if not data.get('Phone') and not data.get('fb_phone') and not data.get('Yelp_Phone__c'):
            lead_info['Lead_Invalid_Reason__c'] = 'No Phone'
            return (data, lead_info)
        data['valid_lead'], lead_info['Lead_Invalid_Reason__c'] = self.validator.validate_lead(data, fb_data)

        data.update(self.foursquare.get_foursquare_data(data))
        # data.update(self.walkscore.get_walkscore_data(data))
        # data.update(self.transitscore.get_transitscore_data(data))

        lead_info.update(self.build_sf_obj(data, fb_data))

        if data.get('Nearby_Version__c') != 2:
            distance = 5
            if self.zip_data.get_owner_id(data['Owner_ID__c'], data['PostalCode']) == '00GE0000003QWrg':
                distance = 50
            data, lead_info = self.nearby.get_nearby_data(data, lead_info, distance)

        if data.get('Phone_Disposition__c') not in ["connected", "connected-75", "busy"]:
            lead_info['Lead_Invalid_Reason__c'] = 'phone dispo - ' + data['Phone_Disposition__c']

        if ((fb_data.get('page_data', {}).get('is_permanently_closed', None)) or (
                data.get('Yelp_Is_Permanently_Closed__c'))):
            lead_info['Lead_Invalid_Reason__c'] = 'FB / Yelp - out of biz'

        return (data, lead_info)

    def build_sf_obj(self, data, fb_data):
        ''' This method creates a salesforce object that can be used to update a salesforce lead object.
        '''
        logger.debug('create_sf_obj')
        try:
            if not fb_data.get('page_data'):
                print 'no page data'
                ipdb.set_trace()
                print 'no page data'

            last_owner_post, oldest_owner_post, count_of_owner_posts, last_owner_post_message, owner_deal_count_post = self.fb.get_post_info(fb_data.get('page_data'))
            last_post, oldest_post, count_of_posts = self.fb.get_feed_info(fb_data.get('page_data'))
            return {
                'FirstName': utils.set_name(data['FirstName']).encode('utf-8'),
                'LastName': utils.set_name(data['LastName']).encode('utf-8'),
                'Phone': data['Phone'],
                'Status': 'Pending',
                'OwnerId': self.zip_data.get_owner_id(data['Owner_ID__c'], data['PostalCode']),
                'Campaign_Comp_Level__c': 'Cold',
                'Campaign_Category__c': data.get(
                    'Campaign_Category__c').encode('utf-8') if data.get('Campaign_Category__c') else "No Category",
                'FB_Match_State__c': fb_data.get('fuzzy_score'),
                'FB_Days_Since_Last_Post__c': last_post if last_post else 0,
                'FB_Days_Since_Oldest_Post__c': oldest_post if oldest_post else 0,
                'FB_Count_of_Post__c': count_of_posts if count_of_posts else 0,
                'FB_Post_Engagement__c': self.fb.post_engagement(
                    count_of_posts, oldest_post) if self.fb.post_engagement(count_of_posts, oldest_post) else 0,
                'FB_Likes__c': fb_data.get(
                    'page_data', {}).get(self.fb.likes) if fb_data.get('page_data', {}).get(self.fb.likes) else 0,
                'FB_Checkins__c': fb_data.get(
                    'page_data', {}).get('checkins', ''),
                'FB_Talking_About__c': fb_data.get(
                    'page_data', {}).get('talking_about_count', ""),
                'FB_Is_Permanently_Closed__c': 1 if fb_data.get(
                    'page_data', {}).get('is_permanently_closed', None) else 0,
                'FB_Is_Claimed__c': 0 if fb_data.get(
                    'page_data', {}).get('is_unclaimed', None) else 1,
                'FB_Price_Range__c': self.fb.get_price_range(fb_data) if self.fb.get_price_range(fb_data) else "",
                'FB_Can_Post__c': 1 if fb_data.get('page_data', {}).get('can_post', None) else 0,
                'FB_Can_Checkin__c': 1 if fb_data.get(
                    'page_data', {}).get('can_checkin', None) else 0,
                'FB_Place_Type__c': fb_data.get('page_data', {}).get('place_type', '').encode('utf-8'),
                'FB_Category__c': fb_data.get('page_data', {}).get('category', '').encode('utf-8'),
                'FB_Category_List__c': self.validator.get_category_list(fb_data.get('page_data')),
                'FB_Valid_Lead__c': 1 if data['valid_lead'] else 0,
                'Phone_Disposition__c': data['Phone_Disposition__c'],
                'RV_Co_Name__c': data['RV_Co_Name__c'],
                'Number_Is_Cell__c': 1 if data['Number_is_Cell__c'] else 0,
                'Phone_Carrier__c': data['Phone_Carrier__c'],
                'Latitude': data['Latitude'],
                'Longitude': data['Longitude'],
                'FB_and_yelp_not_claimed__c': data.get(
                    'FB_and_yelp_not_claimed__c') if data.get('FB_and_yelp_not_claimed__c') else 0,
                'FB_and_yelp_claimed__c': data.get(
                    'FB_and_yelp_claimed__c') if data.get('FB_and_yelp_claimed__c') else 0,
                'FB_Restaurant_Services__c': self.fb.get_restaurant_services(fb_data.get('page_data')),
                'FB_Restaurant_Specialities__c': self.fb.get_restaurant_specialties(
                    fb_data.get('page_data')),
                'FB_Food_Style__c': self.fb.get_food_styles(fb_data),
                'FB_Page__c': fb_data.get('page_data', {}).get('link', '').encode('utf-8'),
                'FB_Website__c': fb_data.get(
                    'page_data', {}).get('website', '').replace('\n', ';')[:250].encode('utf-8'),
                'FB_Phone__c': data.get('fb_phone'),
                'Yelp_Phone__c': data.get('Yelp_Phone__c'),
                'Yelp_Is_Permanently_Closed__c': 1 if data.get('Yelp_Is_Permanently_Closed__c') else 0,
                'Yelp_Review_Count__c': data.get('Yelp_Review_Count__c') if data.get('Yelp_Review_Count__c') else 0,
                'Yelp_Page__c': data.get('Yelp_Page__c').encode('utf-8') if data.get('Yelp_Page__c') else '',
                'Yelp_Deals__c': data.get('Yelp_Deals__c'),
                'Yelp_Category__c': data.get('Yelp_Categories__c'),
                'Yelp_eat24_url__c': data.get('Yelp_eat24_url__c'),
                'Yelp_Gift_Certificate__c': data.get(
                    'Yelp_Gift_Certificate__c') if data.get('Yelp_Gift_Certificate__c') else 0,
                'Yelp_Is_Claimed__c': 1 if data.get('Yelp_Is_Claimed__c') else 0,
                'Yelp_Menu_Updated_Date__c': data.get('Yelp_Menu_Updated_Date__c'),
                'Web_Rating__c': data.get('Yelp_Rating__c') if data.get('Yelp_Rating__c') else 0,
                'Yelp_Reservation_url__c': data.get('Yelp_Reservation_url__c'),
                'YellowPages_Phone__c': data.get('yellow_pages_phone'),
                'FB_Page_ID__c': self.fb.get_page_id(fb_data.get('page_data')).encode('utf-8'),
                'FB_Email__c': self.fb.get_emails(fb_data.get(
                    'page_data', {}).get('emails', None))[:190],
                'Marketing_Initiative__c': self.marketing_initiative,
                'FB_Enriched_Date__c': (
                    datetime.datetime.today().date() - datetime.timedelta(hours=7)).strftime('%Y-%m-%d'),
                'Marketing_Approved_Last_Date__c': (
                    datetime.datetime.today().date() - datetime.timedelta(hours=7)).strftime('%Y-%m-%d'),
                'Foursquare_ID__c': data.get('Foursquare_ID__c'),
                'Foursquare_Category_Name__c': data.get('Foursquare_Category_Name__c'),
                'Foursquare_Phone__c': data.get('Foursquare_Phone__c'),
                'Foursquare_Twitter_Handle__c': data.get('Foursquare_Twitter_Handle__c'),
                'Foursquare_Name__c': data.get('Foursquare_Name__c'),
                'Foursquare_Specials_Count__c': data.get(
                    'Foursquare_Specials_Count__c') if data.get('Foursquare_Specials_Count__c') else 0,
                'Foursquare_Specials_Items__c': data.get('Foursquare_Specials_Items__c'),
                'Foursquare_Stats_Checkins__c': data.get(
                    'Foursquare_Stats_Checkins__c') if data.get('Foursquare_Stats_Checkins__c') else 0,
                'Foursquare_Stats_Tips__c': data.get(
                    'Foursquare_Stats_Tips__c') if data.get('Foursquare_Stats_Tips__c') else 0,
                'Foursquare_Stats_Users__c': data.get(
                    'Foursquare_Stats_Users__c') if data.get('Foursquare_Stats_Users__c') else 0,
                'Foursquare_Verified__c': 1 if data.get(
                    'Foursquare_Verified__c') else 0,
                'Foursquare_Collected_Website__c': data.get('Foursquare_Collected_Website__c'),
                'Walkscore_Score__c': data.get('Walkscore_Score__c') if data.get('Walkscore_Score__c') else 0,
                'Walkscore_Desc__c': data.get('Walkscore_Desc__c'),
                'Transitscore_Score__c': data.get('Transitscore_Score__c') if data.get('Transitscore_Score__c') else 0,
                'Transitscore_Desc__c': data.get('Transitscore_Desc__c'),
                'Transitscore_Nearby_Routes__c': data.get(
                    'Transitscore_Nearby_Routes__c') if data.get('Transitscore_Nearby_Routes__c') else 0,
                'Transitscore_Bus_Routes__c': data.get(
                    'Transitscore_Bus_Routes__c') if data.get('Transitscore_Bus_Routes__c') else 0,
                'Transitscore_Rail_Routes__c': data.get(
                    'Transitscore_Rail_Routes__c') if data.get('Transitscore_Rail_Routes__c') else 0,
                'Transitscore_Other_Routes__c': data.get(
                    'Transitscore_Other_Routes__c') if data.get('Transitscore_Other_Routes__c') else 0
            }

        except:
            logger.exception("Update SF Lead Error")
            return
