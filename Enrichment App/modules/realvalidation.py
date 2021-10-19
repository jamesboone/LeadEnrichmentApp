import logging
import ipdb
import xml.etree.ElementTree as et
from modules.xml_to_dict import XmlDictConfig
import requests
logger = logging.getLogger(__name__)


def _real_validation(phone):
    logger.debug('real validation lib')
    good_to_go = ["connected", "busy", "disconnected", "unreachable", "Invalid Phone", "restricted",
                  "ERROR", "invalid-format", "invalid-phone"]
    no_bueno = ["connected-75", "disconnected-85", "disconnected-70", "disconnected-50", "unknown",
                "phone-zip-invalid"]
    url = "https://api.realvalidation.com/rpvWebService/RealPhoneValidationScrub.php?zip=00000&phone=%s&token=EBAB161C-3AE6-DFFE-035640587F23" % (phone, )  # NOQA
    result = requests.get(url)
    if result.status_code == 200:
        root = et.XML(result.content)
        xmldict = XmlDictConfig(root)
        if xmldict.get('status', None) in good_to_go:
            carrier = xmldict.get('carrier', None)
            co_name = xmldict.get('cnam', None)
            is_cell = 1 if xmldict.get('iscell', None) == "Y" else 0
            status = xmldict.get('status', None)
            return (carrier, is_cell, status, co_name)
        elif xmldict.get('status', None) in no_bueno:
            url = "https://api.realvalidation.com/rpvWebService/RealPhoneValidationSimple.php?zip=00000&Phone=%s&token=EBAB161C-3AE6-DFFE-035640587F23" % (phone, )  # NOQA
            result = requests.get(url)
            if result.status_code == 200:
                root = et.XML(result.content)
                xmldict = XmlDictConfig(root)
                carrier = xmldict.get('carrier', None)
                co_name = xmldict.get('cnam', None)
                is_cell = 1 if xmldict.get('iscell', None) == "Y" else 0
                status = xmldict.get('status', None)
                return (carrier, is_cell, status, co_name)
        # elif xmldict.get('status', None) in error:
        else:
            logger.exception("RealValidation Error: %s - Text: %s" % (
                xmldict.get('status', None), xmldict.get('error_text', None), ))
    else:
        logger.exception("Failed url request with status code: %s" % (result.status_code, ))


def get_real_validation(data, phone):
    ''' gets the validation data of the phone number usig realvalidation library
    '''
    logger.debug('get_real_validation')
    try:
        if phone:
            data['Phone_Carrier__c'], \
                data['Number_is_Cell__c'], \
                data['Phone_Disposition__c'], \
                data['RV_Co_Name__c'] = _real_validation(phone)
    except:
        logger.exception('Real Validation Error')
    return data
