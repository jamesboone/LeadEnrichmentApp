''' Lead enrichment tool '''
from __future__ import division

import logging
import json
import ipdb
import sys
import time
import urllib2
from datetime import datetime

from selenium import webdriver
from fuzzywuzzy import fuzz
from BeautifulSoup import BeautifulSoup
from facebook import GraphAPI, GraphAPIError
from pprint import pprint

logger = logging.getLogger(__name__)
reload(sys)
sys.setdefaultencoding('utf8')


class Facebook(object):
    FACEBOOK_APP_TOKENS = []

    def __init__(self, sf_api, validator, initiative, testing, rekindle=None, days_since_enrich=None):
        ''' Initialize the facebook searches
        '''
        logger.debug('facebook init')
        self.testing = testing
        self.marketing_initiative = initiative
        logger.info("Marketing Initiative: %s" % (self.marketing_initiative),)
        self.days_since_enrich = days_since_enrich or 30
        self.request_counter = 0
        self.consec_failures = 0
        self.token_ix = 0
        self.throttled = 0

        with open('config.json', 'r') as f:
            config = json.load(f)
        self.username = config.get("facebook").get("username")
        self.password = config.get("facebook").get("password")
        self.likes = config.get("facebook").get("likes")

        if self.testing:
            logger.warning(
                "\n%s\nIn Testing Mode don't use '-t' flag when running in production\n%s" % ("+" * 80, "+" * 80,))
            self.FACEBOOK_APP_TOKENS = config.get("facebook").get("keys")
        else:
            self.get_facebook_tokens()

        self.token_ix = (self.token_ix + 1) % len(self.FACEBOOK_APP_TOKENS)
        try:
            self.graph = GraphAPI(self.FACEBOOK_APP_TOKENS[self.token_ix])
        except GraphAPIError:
            logger.error('go to https://developers.facebook.com/tools/access_token/ to refresh tokens')
            self.get_facebook_tokens()
        self.validator = validator
        self.sf_api = sf_api

    def get_facebook_tokens(self):
        ''' Opens browser and captures extended key good for 1 months
        '''
        logger.info('get_facebook_tokens, can take a few min and requires firefox browser on device')
        try:
            self.request_counter += 1
            if self.request_counter > 3:
                raise Exception("Requested 'get refreshed token' too many consecutive times'")
                logger.warning('Sleeping for 15 min to let the Facebook API cool down.')
                time.sleep(900)
                self.request_counter = 0
                self.throttled += 1
                logger.info("You have been throttled %s times" % (self.throttled))
                return
            url = "https://developers.facebook.com/tools/access_token/"

            driver = webdriver.Firefox()
            driver.get(url)
            UN = driver.find_element_by_id('email')
            UN.send_keys(self.username)
            PS = driver.find_element_by_id('pass')
            PS.send_keys(self.password)
            LI = driver.find_element_by_id('loginbutton')
            LI.click()

            html = driver.page_source
            soup = BeautifulSoup(html)

            debug_buttons = soup.findAll('a', {'class': '_42ft _4jy0 rfloat _ohf _4jy3 _517h _51sy'})
            sites = []
            for button in debug_buttons:
                sites.append(button.get('href'))
            self.FACEBOOK_APP_TOKENS = []
            for site in sites:
                driver.get(site)
                try:
                    time.sleep(2)
                    driver.find_elements_by_xpath(
                        "//button[contains(.,'Extend Access Token')]")[0].click()
                    time.sleep(2)
                    self.FACEBOOK_APP_TOKENS.append(driver.find_elements_by_xpath(
                        '//div[@class="_ohe lfloat"]')[0].text)
                    if self.testing:
                        logger.warning("\nReminder: In Testing Mode don't use '-t' flag when running in production\n")
                        break
                except:
                    pass
            logger.debug("\nNew facebook tokens\n")
            pprint(self.FACEBOOK_APP_TOKENS)
            driver.close()
        except:
            logger.exception("error during selenium usage")

    def change_throttled_token_to_new_token(self):
        ''' Cycle through fb graph tokens. Just stop if all tokens fail
        '''
        logger.debug('change_throttled_token_to_new_token')

        self.consec_failures += 1
        if self.consec_failures > len(self.FACEBOOK_APP_TOKENS):
            logger.info("All tokens failed... try again later")
            self.timedeltas = []
            time.sleep(900)
            self.consec_failures = 0
            self.get_facebook_tokens()
            self.change_throttled_token_to_new_token()

        self.token_ix = (self.token_ix + 1) % len(self.FACEBOOK_APP_TOKENS)
        logger.info("Using refreshed token: %s" % (self.FACEBOOK_APP_TOKENS[self.token_ix]))
        self.graph = GraphAPI(self.FACEBOOK_APP_TOKENS[self.token_ix])

    def control_request_speed(self):
        ''' Throttles requests so that facebook doesn't block us.
        '''
        logger.debug('control_request_speed')
        time.sleep(20)

    def search(self, data, search_method='geo_search', distance=300, fb_page=None):
        ''' Method that uses the facebook graph api to identify business by lat & lng coords
        '''
        logger.debug('search')
        try:
            self.control_request_speed()
            result = None
            name = data['Company']
            geo = "%s, %s" % (data['Latitude'], data['Longitude'])
            geo_search = {"q": name, 'center': geo, "distance": distance, "type": "place"}
            fql = '%s in %s' % (name, data['City'],)
            fql_search = {"q": fql, "type": "place"}

            fb_query = {
                'geo_search': geo_search,
                'fql_search': fql_search
            }
            if not data['Latitude']:
                search_method = 'fql_search'
                distance = 1000
            try:
                result = self.graph.request("search", fb_query[search_method])
                result = self.check_search(result, name)

                if result.get('data'):
                    result['page_data'] = self.get_object_data(result["data"][0]["id"])
                    result['fuzzy_score'] = self.validator.validate_company(name, result['page_data']["name"])
                    if result['fuzzy_score'] == 'Wrong':
                        result['data'] = None
                if fb_page:
                    name = data['FB_Page__c'].split('/')[-1]
                    fb_id = None
                    try:
                        fb_obj = self.graph.get_object(name)
                        if fb_obj:
                            fb_id = fb_obj.get('id')
                    except GraphAPIError:
                        result['bad_fb_page'] = True
                    if fb_id:
                        result['page_data'] = self.get_object_data(fb_id)
                        result['fuzzy_score'] = self.validator.validate_company(data['Company'], result['page_data']["name"])
                        if result['fuzzy_score'] == 'Wrong':
                            result['data'] = None

                if not result.get('data') and distance == 300 and not fb_page:
                    return self.search(data, search_method='geo_search', distance=1000)

                if (not result.get('data') and distance == 1000 and
                        not search_method == 'fql_search' and not fb_page):
                    return self.search(data, search_method='fql_search', distance=1000)

                if (not result.get('data') and data.get('FB_Page__c') and not fb_page):
                    name = data['FB_Page__c'].split('/')[-1]
                    if name:
                        return self.search(data, search_method='geo_search', distance=300, fb_page=name)

            except GraphAPIError:
                logger.exception('Error while searching for fb page, possibly throttled sleeping for 15 min')
                time.sleep(900)
                self.get_facebook_tokens()
                self.change_throttled_token_to_new_token()
                return self.search(data, search_method, distance)
            except:
                logger.exception('unknown error during fb page search')
                time.sleep(900)
                self.get_facebook_tokens()
                self.change_throttled_token_to_new_token()
                return self.search(data, search_method, distance)
            return result if result else None
        except:
            logger.exception('error in def search')

    def get_object_data(self, object_id):
        ''' Method to collect facbook data using page object id
        '''
        logger.debug('get_object_data')
        fields = ",".join(['id',
                           'link',
                           'website',
                           'emails',
                           'checkins',
                           'is_community_page',
                           'is_permanently_closed',
                           'is_unclaimed',
                           'phone',
                           'can_post',
                           'can_checkin',
                           'category',
                           'category_list',
                           'attire',
                           'contact_address',
                           'food_styles',
                           'hours',
                           'is_always_open',
                           'is_verified',
                           self.likes,
                           'location',
                           'name',
                           'parent_page',
                           'parking',
                           'payment_options',
                           'place_type',
                           'price_range',
                           'public_transit',
                           'restaurant_services',
                           'restaurant_specialties',
                           'start_info',
                           'talking_about_count'])
        fields += ",feed.limit(25),posts.limit(25){shares,comments.limit(0).summary(true),message,story,created_time}"
        try:
            result = self.graph.request(object_id, args={"fields": fields})
            self.consec_failures = 0
        except GraphAPIError:
            # ipdb.set_trace()
            fields = fields.replace(',feed.limit(25),posts.limit(25){shares,comments.limit(0).summary(true),message,story,created_time}', '')
            result = self.graph.request(object_id, args={"fields": fields})
            print '\n\n failed to get feed or posts'
        except:
            # ipdb.set_trace()
            print '\n\n failed for some other reason than feed or post'
            logger.exception("Graph API Error(3)")
            self.get_facebook_tokens()
            self.change_throttled_token_to_new_token()
            return self.get_object_data(object_id)
        return result

    def get_food_styles(self, fb_data):
        ''' returns a comma delimited string of the food styles that the fb page listed
        '''
        logger.debug('get_food_styles')
        if fb_data:
            styles = fb_data.get('page_data', {}).get('food_styles', '')
            if styles:
                return ', '.join(styles).encode('utf-8')[:100]
        return None

    def check_search(self, search_result, name):
        ''' Identifies which facebook returned result is the best fit
        '''
        logger.debug('check_search')
        if search_result.get("data") and len(search_result["data"]) > 1:
            fuzz_calcs = []
            for record in search_result["data"]:
                fuzz_calcs.append(fuzz.ratio(record['name'], name))
            search_result["data"] = [search_result["data"][fuzz_calcs.index(max(fuzz_calcs))]]
            return search_result
        else:
            return search_result

    def set_street(self, street, fb_data):
        ''' gets street from fb data
        '''
        logger.debug('set_street')
        if fb_data:
            if fb_data.get('location').get('street'):
                if street != fb_data.get('location').get('street'):
                    return fb_data.get('location').get('street')
        return street

    def get_last_post_msg(self, posts):
        for post in posts:
            if post.get('story') or post.get('message'):
                story = post.get('story') if post.get('story') else ''
                message = post.get('message') if post.get('message') else ''
                shares = post.get('shares').get('count') if post.get('shares') else ''
                post_message = '%s%s%s' % (story + ': ' if story else '', message, 'shares: ' + shares if shares else '')
                return post_message
        return ''

    def get_post_info(self, fb_data):
        ''' gets the first page of posts info then pages to the last post using recursive
            method "get oldest post date"
        '''
        logger.debug('get_post_info')
        if fb_data:
            self.deal_count = 0
            self.number_of_posts = 0
            try:
                posts = fb_data.get('posts', None)
                paging = posts.get('paging') if posts else None
                if paging:
                    previous = paging.get('previous')
                else:
                    previous = None
                len_of_posts = len(posts.get('data'))
                if len_of_posts:
                    last_post = posts.get('data', None)[0]
                    if last_post:
                        last_post_message = self.get_last_post_msg(posts.get('data'))
                        last_post = datetime.strptime(
                            last_post.get('created_time', None), "%Y-%m-%dT%H:%M:%S+%f")
                    else:
                        raise
                if len_of_posts < 25:
                    oldest_post_date = self.get_oldest_post_date(posts, previous, pages=False)
                    self.number_of_posts += len_of_posts
                else:
                    self.number_of_posts = len_of_posts
                    oldest_post_date = self.get_oldest_post_date(posts, previous, pages=True)
                return (abs((datetime.now() - last_post).days),
                        abs((datetime.now() - oldest_post_date).days),
                        self.number_of_posts, last_post_message, self.deal_count)
            except:
                return (None, None, None, None, None)
        else:
            return (None, None, None, None, None)

    def get_oldest_post_date(self, feed, previous, pages=False):
        ''' recursive method paging back to first post to get oldest date and count of total
        '''
        previous_page = previous
        logger.debug('get_oldest_post_date')
        if pages:
            paging = feed.get('paging')
            try:
                if paging:
                    self.deal_count = self.check_for_deal_data(feed.get('data'))
                    next_page = paging.get('next')
                    next_page = next_page.replace('limit=25', 'limit=100')
                    next_page = json.loads(urllib2.urlopen(next_page).read())
                    paging = next_page.get('paging')
                    previous = paging.get('previous')
                    if len(next_page.get('data')) < 100:
                        self.number_of_posts += len(next_page.get('data'))
                        return self.get_oldest_post_date(next_page, previous)
                    else:
                        self.number_of_posts += len(next_page.get('data'))
                        return self.get_oldest_post_date(next_page, previous, pages=True)
                else:
                    return self.get_oldest_post_date(previous_page, previous=None)
            except:
                logger.exception('error(post error)')
        else:
            oldest_post = feed.get('data', None)[-1]
            self.deal_count = self.check_for_deal_data(feed.get('data'))
            if oldest_post:
                oldest_post = datetime.strptime(
                    oldest_post.get('created_time', None), "%Y-%m-%dT%H:%M:%S+%f")
                return oldest_post

    def check_for_deal_data(self, feed, biz_post_id=None):
        deal_keywords = ['bargin', 'buy', 'deal', 'giveaway', 'closeout', 'markdown', 'deduction',
                         'decrease', 'coupon', 'promo', 'promotion', 'groupon', 'dailydeal',
                         'daily-deal', 'gift card', 'giftcard', 'reward', 'special ', 'limited',
                         'free', 'valpack', 'val-pack', 'event', 'celebrat', 'sale', 'sales',
                         'blowout', 'last chance', '% off', 'event']
        negative_keywords = ['feel free']
        for record in feed:
            for keyword in deal_keywords:
                message = record.get('message').lower() if record.get('message') else None
                if not message:
                    break
                elif keyword in message:
                    self.deal_count += 1
                    break
        return self.deal_count

    def get_feed_info(self, fb_data):
        ''' gets the first page of feed info then pages to the last post using recursive
            method "get oldest post date"
        '''
        logger.debug('get_feed_info')
        if fb_data:
            try:
                feed = fb_data.get('feed', None)
                paging = feed.get('paging')
                if paging:
                    previous = paging.get('previous')
                else:
                    previous = None
                len_of_feed = len(feed.get('data'))
                if len_of_feed:
                    last_feed_post = feed.get('data', None)[0]
                    if last_feed_post:
                        last_feed_post = datetime.strptime(
                            last_feed_post.get('created_time', None), "%Y-%m-%dT%H:%M:%S+%f")
                    else:
                        raise

                if len_of_feed < 25:
                    oldest_feed_post_date = self.get_oldest_feed_post_date(feed, previous, pages=False)
                    self.number_of_feed_posts += len_of_feed
                else:
                    self.number_of_feed_posts = len_of_feed
                    oldest_feed_post_date = self.get_oldest_feed_post_date(feed, previous, pages=True)
                result =  (abs((datetime.now() - last_feed_post).days),
                           abs((datetime.now() - oldest_feed_post_date).days),
                           self.number_of_feed_posts)
                return result
            except:
                return (None, None, None)
        else:
            return (None, None, None)


    def get_oldest_feed_post_date(self, feed, previous, pages=False):
        ''' recursive method paging back to first post to get oldest date and count of total
        '''
        previous_page = previous
        logger.debug('get_oldest_post_date')
        if pages:
            paging = feed.get('paging')
            try:
                if paging:
                    next_page = paging.get('next')
                    next_page = next_page.replace('limit=25', 'limit=100')
                    next_page = json.loads(urllib2.urlopen(next_page).read())
                    paging = next_page.get('paging')
                    previous = paging.get('previous')
                    if len(next_page.get('data')) < 100:
                        self.number_of_feed_posts += len(next_page.get('data'))
                        return self.get_oldest_feed_post_date(next_page, previous)
                    else:
                        self.number_of_feed_posts += len(next_page.get('data'))
                        return self.get_oldest_feed_post_date(next_page, previous, pages=True)
                else:
                    return self.get_oldest_feed_post_date(previous_page, previous=None)
            except:
                logger.exception('error(post error)')
        else:
            oldest_post = feed.get('data', None)[-1]
            if oldest_post:
                oldest_post = datetime.strptime(
                    oldest_post.get('created_time', None), "%Y-%m-%dT%H:%M:%S+%f")
                return oldest_post

    def post_engagement(self, count_of, oldest):
        ''' count of posts divided by oldest post in days from today, improvments can be made here
        '''
        logger.debug('post_engagement')
        if count_of and oldest:
            return float(str(count_of / oldest)[:6])
        else:
            return None

    def get_page_id(self, fb_data):
        ''' get the page id of the facebook page
        '''
        logger.debug('get_page_id')
        if fb_data:
            page_id = fb_data.get('id', None)
            if page_id:
                page_id = "www.facebook.com/" + page_id
                return page_id
        return ''

    def get_parent_page_id(self, fb_data):
        ''' get the parent facebook page id of the current lead's facebook page if it exists
        '''
        logger.debug('get_parent_page_id')
        if fb_data:
            page = fb_data.get('parent_page', None)
            if page:
                page_id = "www.facebook.com/" + page.get('id')
                return page_id
        return None

    def get_price_range(self, fb_data):
        ''' normalize price range of fb price data
        '''
        logger.debug('get_price_range')
        if fb_data:
            price_range = fb_data.get('price_range', "").strip("$ \()")
            if price_range:
                price_range = "$ %s" % (price_range, )
                return price_range
        return None

    def get_payment_options(self, fb_data):
        ''' normalize payment data
        '''
        logger.debug('get_payment_options')
        if fb_data:
            options = [options for options, boolean in fb_data.get(
                'payment_options', {}).iteritems() if boolean]
            if options:
                return options
        return None

    def get_restaurant_services(self, fb_data):
        ''' normalize restaurant services for sf
        '''
        logger.debug('get_restaurant_services')
        if fb_data:
            services = [services for services, boolean in fb_data.get(
                'restaurant_services', {}).iteritems() if boolean]
            if services:
                return ', '.join(map(lambda services: services.encode('utf-8'), services))
        return None

    def get_restaurant_specialties(self, fb_data):
        ''' normalize for Salesforce
        '''
        logger.debug('get_restaurant_specialties')
        if fb_data:
            specialties = [specialty for specialty, boolean in fb_data.get(
                'restaurant_specialties', {}).iteritems() if boolean]
            if specialties:
                return ', '.join(map(lambda specialties: specialties.encode('utf-8'), specialties))
        return None

    def get_parking_info(self, fb_data):
        ''' normalize for Salesforce
        '''
        logger.debug('get_parking_info')
        if fb_data:
            parking_info = [parking_type for parking_type, boolean in fb_data.get(
                'parking', {}).iteritems() if boolean]
            if parking_info:
                return parking_info
        return None

    def get_start_date(self, fb_data):
        ''' normalize for Salesforce
        '''
        logger.debug('get_start_date')
        data = None
        if fb_data:
            data = fb_data.get('start_info', None)
            if data.get('date'):
                date_info = data['date']
                day = date_info.get('day', 01)
                month = date_info.get('month', 01)
                year = date_info.get('year', 1900)
                data = "%s/%s/%s" % (day, month, year)
        return data

    def get_emails(self, emails):
        ''' normalize for Salesforce
        '''
        logger.debug('get_emails')
        email_list = ''
        try:
            if len(emails):
                for i in emails:
                    if not email_list:
                        email_list = i.encode('utf8')
                    else:
                        email_list += ', ' + i.encode('utf8')
                return email_list
            else:
                return ''
        except:
            return ''
