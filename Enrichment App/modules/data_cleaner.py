'''Module that cleans any lead so that it can be enriched'''
import logging
import ipdb
import phonenumbers
logger = logging.getLogger(__name__)


class DataCleaner(object):
    def __init__(self, data, lead_info):
        logger.debug('data cleaner init')
        self.lead_info = lead_info
        self.data = data

    def _phone(self):
        logger.debug('_phone')
        if self.data.get('Phone') and not len(self.data.get('Phone')) >= 10:
            self.lead_info['Lead_Invalid_Reason__c'] = 'phone - too few digits'
        elif not self.data.get('Phone'):
            self.lead_info['Lead_Invalid_Reason__c'] = 'no phone number'
        else:
            self.data['Phone'] = str(phonenumbers.parse(self.data['Phone'], 'US').national_number)
            if not len(self.data['Phone']) in [10, 11]:
                self.lead_info['Lead_Invalid_Reason__c'] = 'phone - too many digits'
            elif len(self.data['Phone']) == 11 and self.data['Phone'][:1] != 1:
                self.lead_info['Lead_Invalid_Reason__c'] = 'phone - wrong first digit'

    def _street(self):
        logger.debug('_street')
        if not self.data.get('Street'):
            self.lead_info['Lead_Invalid_Reason__c'] = 'no street'
        else:
            self.data['Street'] = self.data['Street'].split(',')[0]

    def _company(self):
        logger.debug('_company')
        if not self.data.get('Company'):
            self.lead_info['Lead_Invalid_Reason__c'] = 'no company name'
        else:
            opp = str(self.data['Company']).strip()[-1]
            if opp == '-':
                self.data['Company'] = str(self.data['Company']).strip()[:-1]
            elif '-' in self.data['Company']:
                self.data['Company'] = str(self.data['Company']).split('-')[0].strip()
            else:
                self.data['Company'] = str(self.data['Company'])

    def clean(self):
        logger.debug('clean')
        self._phone()
        self._street()
        self._company()

        return (self.data, self.lead_info)

    @classmethod
    def clean_zip_code(cls, zipcode):
        if len(zipcode) > 5:
            zipcode = zipcode.decode('unicode_escape').encode('ascii', 'ignore')

        if len(zipcode) < 5:
            zipcode = '%s%s' % ('0', str(zipcode))
        return zipcode

    @classmethod
    def clean_new_phone(cls, new_phone, existing_phone):
        ''' get the fb phone number from the fb data
        '''
        logger.debug('clean_new_phone')
        phone = new_phone.get('Phone')
        if phone:
            cleaned_phone = str(phonenumbers.parse(phone, 'US').national_number)
            if not len(cleaned_phone) in [10, 11]:
                return None
            elif len(cleaned_phone) == 11 and cleaned_phone[:1] != 1:
                return None
            elif cleaned_phone and existing_phone != cleaned_phone:
                return cleaned_phone
        return None

    @classmethod
    def name_normalizer(cls, name):
        ''' normalizes provided name
        '''
        logger.debug('name_normalizer')
        replace_words = [
            ("&", "and"),
            ("\"", ""),
            ("\'", ""),
            ("st.", "st"),
            ("-", ""),
            (",", ""),
        ]
        name = name.lower().strip()
        for x, y in replace_words:
            name = name.replace(x, y)
        return name

    # @staticmethod
    # def street_normalizer(street):
    #     ''' normalizes provided street
    #     '''
    #     replace_words = [
    #         ("&", "and"),
    #         ("\"", ""),
    #         ("\'", ""),
    #         (".", ""),
    #         ("-", ""),
    #         (" ste", " suite"),
    #     ]
    #     street = street.lower().strip()
    #     for x, y in replace_words:
    #         street = street.replace(x, y)
    #     street = (street + " ")[:street.find("suite")]
    #     return street
