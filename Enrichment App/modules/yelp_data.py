import ipdb
import logging
from yelp.errors import BusinessUnavailable
from modules.send_email import send
from yelp.client import Client
from yelp.oauth1_authenticator import Oauth1Authenticator
logger = logging.getLogger(__name__)


class Yelp_Data(object):
    def __init__(self, validator, req_proxy, email):
        ''' yelp class to get page data '''
        logger.debug('yelp data init')
        auth = Oauth1Authenticator(
            consumer_key="wJQvTDVid5w6q3fVmvuw",
            consumer_secret="hFrqqK3GCGcKCvHICpWnWZnvM",
            token="F2Fel6odVvmKMnzSpfaz-IyJFi7EBi",
            token_secret="dXLRCMBnhYTlvIrTbpgFQBNds"
        )

        self.client = Client(auth)
        self.req_proxy = req_proxy
        self.validator = validator
        self.email = email

    def get_yelp_data(self, data):
        logger.debug('get_yelp_data')
        self.yelp_object = self._get_yelp_object(data)
        is_closed = self._is_closed(data)
        if hasattr(self.yelp_object, 'business'):
            self.yelp_object = self.yelp_object.business
        elif hasattr(self.yelp_object, 'businesses'):
            self._best_yelp_biz_idx(data)
            if isinstance(self.biz_index, int):
                try:
                    self.yelp_object = self.yelp_object.businesses[self.biz_index]
                except Exception, err:
                    print err
                    ipdb.set_trace()
                    print err
            else:
                data['Yelp_Is_Permanently_Closed__c'] = is_closed
                return data

        if not self.yelp_object:
            data['Yelp_Is_Permanently_Closed__c'] = is_closed
            return data

        yelp_categories = self._get_category_obj(data)
        deals = self._get_deals(data)
        if deals:
            data.update(deals)

        return {
            'Yelp_Is_Permanently_Closed__c': is_closed,
            'Yelp_Review_Count__c': self._get_review_count(data) if self._get_review_count(data) else 0,
            'Yelp_Phone__c': self._get_phone(data),
            'Yelp_Page__c': self._get_yelp_url(data),
            'Yelp_Categories__c': self._get_categories(yelp_categories),
            'Yelp_eat24_url__c': self._get_eat24_url(data),
            'Yelp_Gift_Certificate__c': self._get_gift_certificate(data),
            'Yelp_Is_Claimed__c': self._get_page_claimed_status(data),
            'Yelp_Menu_Updated_Date__c': self._get_menu_updated_date(data),
            'Yelp_Rating__c': self._get_rating(data) if self._get_rating(data) else 0,
            'Yelp_Reservation_url__c': self._get_reservation_url(data),
            'invalid_reason': 'yelp biz closed' if is_closed else None
        }

    def _get_yelp_object(self, data):
        logger.debug('_get_yelp_object')
        yelp_page = data.get('Yelp_Page__c')

        yelp_id = None
        try:
            # Ex - yelp_page = self.client.get_business('buffalo-stamps-and-stuff-buffalo')
            # Ex - yelp_biz_list = [u'http:', u'', u'www.yelp.com', u'biz', u'buffalo-stamps-and-stuff-buffalo']
            yelp_biz_list = yelp_page.split('/')
            if yelp_biz_list[3] == 'biz' and yelp_biz_list[4]:
                yelp_id = yelp_biz_list[4]
            if yelp_id:
                request = self.client.get_business(yelp_id)
                if request:
                    return request
        except BusinessUnavailable:
            logger.debug("BusinessUnavailable")
            pass

        except:
            logger.debug("Error collecting yelp object")
            pass

        if data['Phone'] and len(data['Phone']) in [10, 11]:
            try:
                request = self.client.phone_search(phone=data['Phone'])
                if request:
                    return request
            except:
                logger.debug("error getting yelp object")

    def _best_yelp_biz_idx(self, data):
        logger.debug('_best_yelp_biz_idx')
        self.biz_index = None
        biz_scores = []
        for business in self.yelp_object.businesses:
            fuzzy_score = self.validator.validate_company(data['Company'], business.name, yelp=True)
            biz_scores.append(fuzzy_score)
        for score in biz_scores:
            if score not in range(50, 101, 1):
                idx = biz_scores.index(score)
                biz_scores.pop(idx)
                self.yelp_object.businesses.pop(idx)
        if biz_scores:
            self.biz_index = biz_scores.index(max(biz_scores))

    def _is_closed(self, data):
        logger.debug('_is_closed')
        yelp_page = data.get('Yelp_Page__c')
        if hasattr(self.yelp_object, 'business'):
            return self.yelp_object.business.is_closed
        elif hasattr(self.yelp_object, 'businesses'):
            self._best_yelp_biz_idx(data)
            if isinstance(self.biz_index, int):
                self.yelp_object.businesses[self.biz_index].is_closed
        elif yelp_page:
            counter = 0
            req = self.req_proxy.generate_proxied_request(yelp_page)
            while req in ['timeout', 'unreachable', 'auth']:
                counter += 1
                req = self.req_proxy.generate_proxied_request(yelp_page)
                if counter > 50:
                    subject = "Yelp Probably Banned EC2 IP."
                    send(subject, toaddr=[self.email])
                    raise "Yelp Probably Blocked Us"

            if req and req.text and 'Yelpers report this location has closed.' in req.text:
                return True
            else:
                return False
        else:
            return True

    def _get_review_count(self, data):
        logger.debug('_get_review_count')
        return self.yelp_object.review_count

    def _get_phone(self, data):
        logger.debug('_get_phone')
        phone = str(self.yelp_object.phone)
        if phone:
            if '+' in phone:
                if '+1' in phone:
                    phone = phone.replace('+1', '')
                else:
                    phone = phone.replace('+', '')
            return phone

    def _get_yelp_url(self, data):
        logger.debug('_get_yelp_url')
        if data.get('Yelp_Page__c'):
            return data.get('Yelp_Page__c')
        return self.yelp_object.url.encode('utf-8') if self.yelp_object.url else None

    def _get_deals(self, data):
        logger.debug('_get_deals')
        if self.yelp_object.deals:
            deal = self.yelp_object.deals[-1]
            return {
                'Yelp_Number_of_Deals__c': len(self.yelp_object.deals) if self.yelp_object.deals else 0,
                'Yelp_Deal_url__c': deal.url.encode('utf-8') if deal.url else None,
                'Yelp_Deal_Expiration_Date__c': deal.time_end.encode('utf-8') if deal.time_end else None,
                'Yelp_Deal_What_You_Get__c': deal.what_you_get.encode('utf-8') if deal.what_you_get else None,
                'Yelp_Deal_Important_Restriction__c': deal.important_restriction.encode(
                    'utf-8') if deal.important_restriction else None,
                'Yelp_Deal_Is_Poplular__c': 1 if deal.is_popular else 0,
                'Yelp_Deal_Title__c': deal.title.encode('utf-8') if deal.title else None
            }
        else:
            return data

    def _get_category_obj(self, data):
        logger.debug('_get_category_obj')
        if data.get('Yelp_Category__c'):
            return str(data.get('Yelp_Category__c'))
        return self.yelp_object.categories

    def _get_categories(self, data):
        logger.debug('_get_categories')
        if data and not isinstance(data, list):
            if isinstance(data, str):
                return data
            elif data.get('Yelp_Category__c'):
                return str(data.get('Yelp_Category__c'))

        if data:
            if len(data) > 1:
                categories = []
                for cat in data:
                    categories.append(cat.name.encode('utf-8'))
                    categories.append(cat.alias.encode('utf-8'))
                if categories:
                    cat_list = str(categories).strip("[]").replace("'", "")
                    return cat_list
            else:
                name = data[0].name.encode('utf-8') if data[0].name else None
                alias = data[0].alias.encode('utf-8') if data[0].alias else None
                separator = None
                if name and alias:
                    separator = ', '
                cat_list = '%s%s%s' % (name if name else "", separator if separator else "", alias if alias else "")

    def _get_eat24_url(self, data):
        logger.debug('_get_eat24_url')
        return self.yelp_object.eat24_url.encode(
            'utf-8') if self.yelp_object.eat24_url else None

    def _get_gift_certificate(self, data):
        logger.debug('_get_gift_certificate')
        prices = []
        if self.yelp_object.gift_certificates:
            for gift in self.yelp_object.gift_certificates[0].options:
                prices.append(gift.price / 100)

        if prices:
            return str(prices).strip("[]") if prices else ''

    def _get_page_claimed_status(self, data):
        logger.debug('_get_page_claimed_status')
        return self.yelp_object.is_claimed

    def _get_menu_updated_date(self, data):
        logger.debug('_get_menu_updated_date')
        return self.yelp_object.menu_date_updated

    def _get_menu_provider(self, data):
        logger.debug('_get_menu_provider')
        return self.yelp_object.menu_provider.encode(
            'utf-8') if self.yelp_object.menu_provider else None

    def _get_rating(self, data):
        logger.debug('_get_rating')
        return self.yelp_object.rating

    def _get_reservation_url(self, data):
        logger.debug('_get_reservation_url')
        return self.yelp_object.reservation_url.encode(
            'utf-8') if self.yelp_object.reservation_url else None
