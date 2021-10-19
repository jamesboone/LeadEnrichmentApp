import logging
import pandas
import json
import ipdb
from pprint import pprint
from geopy.distance import vincenty
import time
import heapq
import numpy as np
import psycopg2
from modules import utils
from pyzipcode import ZipCodeDatabase, ZipNotFoundException
from modules.data_cleaner import DataCleaner
from fuzzywuzzy import fuzz

logger = logging.getLogger(__name__)


class Nearby(object):

    def __init__(self, version):
        logger.debug('nearby init')

        self.version = version
        self.zcdb = ZipCodeDatabase()
        with open('config.json', 'r') as f:
            config = json.load(f)
        host = config.get("redshift").get("host")
        dbname = config.get("redshift").get("dbname")
        user = config.get("redshift").get("user")
        password = config.get("redshift").get("password")
        port = config.get("redshift").get("port")

        conn_string = """
            host=%s
            dbname=%s
            user=%s
            password=%s
            port=%s
        """ % (host, dbname, user, password, port)

        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        nearby_query = "rollback; select * from leads_nearby_fs_businesses;"
        pandas.set_option('display.max_rows', 10)
        pandas.set_option('display.max_columns', 20)
        pandas.set_option('display.width', 120)
        try:
            cursor.execute(nearby_query)
            names = [x[0] for x in cursor.description]
            rows = cursor.fetchall()
            self.pd_all = pandas.DataFrame(rows, columns=names)
        except Exception, psycopg2.InternalError:
            logger.error('sleeing for 2 min: %s' % (psycopg2.InternalError, ))
            time.sleep(120)
            cursor.execute(nearby_query)
            names = [x[0] for x in cursor.description]
            rows = cursor.fetchall()
            self.pd_all = pandas.DataFrame(rows, columns=names)
        finally:
            if cursor is not None:
                cursor.close()

    def query(self, zips):
        logger.debug('nearby query')
        return self.pd_all[self.pd_all['postal_code'].isin(zips)]

    def narrow_distance(self, data, lead_info, distance):
        logger.debug('narrow_distance')
        try:
            zipcode = DataCleaner.clean_zip_code(str(data['PostalCode']))
            zips = [str(z.zip) for z in self.zcdb.get_zipcodes_around_radius(zipcode, distance)]
        except ZipNotFoundException:
            raise
        return self.query(zips)

    def get_nearby_data(self, data, lead_info, distance):
        logger.debug('get_nearby_data')

        if data.get('PostalCode') and len(data.get('PostalCode')) == 4:
            data['PostalCode'] = '0' + data['PostalCode']

        try:
            data['Latitude'], data['Longitude'] = utils.get_geo(data)
        except:
            ipdb.set_trace()
            print 'pause'
            return (data, lead_info)
        try:
            locations = self.narrow_distance(data, lead_info, distance)
            if len(locations) > 100:
                distance = 2 if distance == 5 else 25
                locations = self.narrow_distance(data, lead_info, distance)
            lead_info['Nearby_Distance__c'] = str(distance) + ' miles'
        except:
            lead_info['Nearby_Version__c'] = self.version
            return (data, lead_info)

        industry_info = self.get_user_counts(locations, data['Latitude'], data['Longitude'], distance)
        nearby = []
        member_count = []
        biz_names = []
        try:
            for loc_idx in range(len(locations)):

                loc = {
                    'business_id': int(locations.iloc[loc_idx][0]),
                    'business_group_id': int(locations.iloc[loc_idx][1]),
                    'biz_name': locations.iloc[loc_idx][2],
                    'member_count': int(locations.iloc[loc_idx][3]),
                    'industry': locations.iloc[loc_idx][4],
                    'sub_industry': locations.iloc[loc_idx][5],
                    'how_to_earn': locations.iloc[loc_idx][6],
                    'postal_code': locations.iloc[loc_idx][7],
                    'address': locations.iloc[loc_idx][8],
                    'city': locations.iloc[loc_idx][9],
                    'sf_account': locations.iloc[loc_idx][10],
                    'lat': locations.iloc[loc_idx][11],
                    'lng': locations.iloc[loc_idx][12],
                    'owner_name': locations.iloc[loc_idx][13],
                    'ap': locations.iloc[loc_idx][14],
                    'up': locations.iloc[loc_idx][15]
                }
                biz_names.append((loc['biz_name'], loc['postal_code']))
                loc_distance = round(vincenty((data['Latitude'], data['Longitude']), (loc['lat'], loc['lng'])).miles, 1)
                if loc_distance <= distance and loc.get('member_count'):
                    account_url = "<a href='https://na9.salesforce.com/%s'>Account</a>" % (loc['sf_account'],)
                    loc['dashboard'] = "<a href='http://www.fivestars.com/login_as_owner/%s'>Dash.</a>" % (
                        loc['business_id'],)
                    if loc.get('owner_name'):
                        owner_name = "%s %s" % ("|", loc['owner_name'].split()[0],)
                    else:
                        owner_name = ''
                    # Vertical, Biz name, Biz Owner Name, User count, City
                    loc['info'] = '%s | %s %s | %s | %s <br> Sub:%s | %smi | How2Earn: %s | AP:%s | UP:%s | %s | %s' % (
                        loc['industry'], loc['biz_name'], owner_name,  # line 1 of salesforce view
                        utils.intWithCommas(loc['member_count']), loc['city'],  # line 1 of view
                        loc['sub_industry'], float(loc_distance), loc['how_to_earn'],  # line 2 of view
                        str(bool(loc['ap']))[:1], str(bool(loc['up']))[:1], account_url,  # line 2 of view
                        loc['dashboard'],)  # line 2 of view
                    member_count.append((loc['member_count'], loc['business_id']))
                    nearby.append(loc)
                del (loc['business_group_id'],
                     loc['ap'],
                     loc['up'],
                     loc['owner_name'],
                     loc['biz_name'],
                     loc['how_to_earn'],
                     loc['lng'],
                     loc['lat'],
                     loc['city'],
                     loc['sf_account'],
                     loc['address'],
                     loc['postal_code'],
                     loc['member_count'])
            best_accounts = heapq.nlargest(100, ((x[0], x[1]) for x in member_count))
            best_list = self.get_best_list(best_accounts, nearby)
            for biz_id in best_list:
                del (biz_id['business_id'],
                     biz_id['industry'],
                     biz_id['sub_industry'])
            results = {
                'nearby': nearby,
                'industry_info': industry_info,
                'best_businesses': best_list
            }
            lead_info.update(self.update_nearby_data(results, data, lead_info, biz_names))
        except Exception, err:
            print err
            ipdb.set_trace()
            print err

        return (data, lead_info)

    def get_user_counts(self, locations, lat, lng, distance):
        logger.debug('get_user_counts')
        industries = {
            'Active Life': (0, 0),
            'Home Service': (0, 0),
            'Food': (0, 0),
            'Automotive': (0, 0),
            'Beauty & Spa': (0, 0),
            'Nightlife': (0, 0),
            'Hotel & Travel': (0, 0),
            'Pets': (0, 0),
            'Health & Medical': (0, 0),
            'Arts & Entertainment': (0, 0),
            'Shopping (Retail)': (0, 0),
            'Local Service': (0, 0),
            'Total': (0, 0)
        }
        try:
            for biz_idx in range(len(locations)):
                if not industries.get(locations.iloc[biz_idx][4]):  # if the industry doesn't exist, add it
                    industries[locations.iloc[biz_idx][4].split(',')[0]] = (0, 0)  # use the first word in the industry
                if float(str(vincenty((lat, lng), (locations.iloc[biz_idx][11], locations.iloc[biz_idx][12])).miles)[:5]) <= distance:
                    num_locations = 0
                    num_users = 0
                    if isinstance(locations.iloc[biz_idx][3], float) or isinstance(locations.iloc[biz_idx][3], int) or isinstance(locations.iloc[biz_idx][3], np.int64):
                        num_locations += industries[locations.iloc[biz_idx][4]][0] + 1
                        num_users += industries[locations.iloc[biz_idx][4]][1] + int(locations.iloc[biz_idx][3])
                        industries[locations.iloc[biz_idx][4]] = (num_locations, num_users)
            # for biz in locations:
            #     if not industries.get(biz[4]):
            #         industries[biz[4].split(',')[0]] = (0, 0)
            #     if float(str(vincenty((lat, lng), (biz[11], biz[12])).miles)[:5]) <= distance:
            #         num_locations = 0
            #         num_users = 0
            #         if biz[3]:
            #             num_locations += industries[biz[4]][0] + 1
            #             num_users += industries[biz[4]][1] + int(biz[3])
            #             industries[biz[4]] = (num_locations, num_users)

            industries_with_users = {}
            industries_total_cnt = 0
            industries_total_users = 0

            for ind, cnt in industries.iteritems():
                if cnt[0]:
                    industries_with_users[ind] = cnt
                industries_total_cnt += cnt[0]
                industries_total_users += cnt[1]
            industries_with_users['Total'] = (industries_total_cnt, industries_total_users)

            industry_list = []
            for indust, counts in industries_with_users.iteritems():
                industry_list.append("%s(%s)-%s" % (indust, counts[0], utils.intWithCommas(counts[1])))
            industry_list.sort(reverse=True)

            return str(industry_list).replace('[', '').replace(']', '').replace("'", '').replace(", ", " | ")
        except Exception, err:
            print err
            ipdb.set_trace()
            print err

    def get_best_list(self, best_accounts, nearby):
        logger.debug('get_best_list')
        top_accounts = []
        industry = []
        sub_industry = []
        for biz in best_accounts:
            for account in nearby:
                if biz[1] == account['business_id'] and (account['industry'] not in industry or
                                                         account['sub_industry'] not in sub_industry):
                    top_accounts.append(account)
                    industry.append(account['industry'])
                    sub_industry.append(account['sub_industry'])

        if len(top_accounts) < 10 and (len(nearby) > len(top_accounts)):
            for account in nearby:
                if account not in top_accounts:
                    top_accounts.append(account)
                    if len(top_accounts) == 10:
                        break
            return top_accounts

        return top_accounts[:10]

    def update_nearby_data(self, nearby_data, lead, lead_info, biz_names):
        logger.debug('update_nearby_data')
        counter = 1
        for biz in nearby_data['best_businesses']:
            nearby_info = "Nearby_Info_%s__c" % (counter, )
            counter += 1
            for biz_name, zipcode in biz_names:
                fuzzy_score = fuzz.ratio(lead['Company'], biz_name)
                if fuzzy_score > 75 and lead['PostalCode'] == zipcode:
                    lead_info['Lead_Invalid_Reason__c'] = 'Duplicate Lead to FS Account'
                    return lead_info
            lead_info[nearby_info] = biz['info']

        lead_info['Nearby_Industry_Info__c'] = nearby_data['industry_info']
        lead_info['FS_Nearby__c'] = len(nearby_data['nearby'])
        lead_info['Nearby_Version__c'] = self.version
        return lead_info
        # return self.sf_api.update(lead['Id'], lead_info)

    # Commented out because this can really screw up data, only use if you know why to use it
    # def clear_nearby_data(self, nearby_data, lead):
    #     logger.debug('clear nearby data')
    #     lead_info = {}
    #     counter = 1
    #     for biz in nearby_data['best_businesses']:
    #         nearby_info = "Nearby_Info_%s__c" % (counter, )
    #         counter += 1
    #         lead_info[nearby_info] = None
    #     lead_info['Nearby_Industry_Info__c'] = None
    #     lead_info['FS_Nearby__c'] = None
    #     lead_info['Nearby_Version__c'] = None
    #     lead_info['Latitude'] = lead['Latitude']
    #     lead_info['longitude'] = lead['Longitude']
    #     logger.debug("lead['Id']: %s" % (lead['Id'],))
    #     return self.sf_api.update(lead['Id'], lead_info)

    def close(self):
        logger.debug('nearby close db conn')
        self.cursor.close()
        self.conn.close()
