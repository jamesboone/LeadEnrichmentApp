# refresh salesforce leads

"""
-------------------------------------------------------------------------------
Usage:
    app.py [FILE] [PURPOSE] [DAYS_SINCE_ENRICHMENT] [--limit=N] [-hvdaerlts] [EMAIL] [--split=N] [--speed=N] [--machines=N]

ipython ex: ipython app.py -- rekindle.py "rekindle for outside" --limit=20 -d -v
    Above processes 20 rekindle leads in decending order with verbose mode on
    the marketing initiative will be "rekindle for outside <today's date>"

python ex: same as above but you can exclude the two hyphens '--'

Help ex: ipython app.py -- -h or python app.py -h

Arguments:
    EMAIL                  your email if you want to be notified about success/fail
                           currently if you add an email you need to also add the
                           DAYS_SINCE_ENRICHMENT value before it, just use 30
    FILE                   file that contains the query to salesforce leads located in
                           ~app/enrichments/
    PURPOSE                purpose for enrichment mapped to "marketing initative" field
                           you can use this field to string search in salesforce
                           (field can be overwritten, typically used for temp reasons)
    DAYS_SINCE_ENRICHMENT  how many days to delay from hitting realvalidation
                           default is 30 days (must be greater than 0)

Options:
    -h                  help
    -v                  verbose mode
    -l                  rebuild the learning model pkl
    -d                  decending query order by company name
    -a                  ascending query order by company name
    -e                  error troubleshooting mode, set's processing to 1 record and
                        query limit to 5 records
    -s                  score enriched leads
    -r                  indicates script to run in rekindle mode
    --limit=N           limit followed by number of records to limit in query
    -t                  This is for testing purposes so that the whole setup process runs faster,
                        it will first use the pre-set facebook tokens then if they have expired it
                        will grab only one from facebook
    --split=N           this allows you to split the query into --machines=N and process the Nth segment
    --speed=N           the is the number of records to process in async. Should not exceed 7 unless
                        new api endpoints have been added that slow down the enrichments (default=7)
    --machines=N        the number of machines you are using to enrich leads (default=5) 
-------------------------------------------------------------------------------
"""
from twisted.internet import defer
from twisted.internet.threads import deferToThread


from twisted.internet import reactor
import importlib
from modules import logger
import os
import pytz
import atexit
import time
from sklearn.externals import joblib
import pandas as pd

from datetime import datetime

import ipdb
from docopt import docopt
from copy import copy
from modules import requestProxy
from modules.redshift import Redshift
from modules.data_cleaner import DataCleaner
from modules.facebook_tool import Facebook
from modules.nearby import Nearby
from modules.salesforce import SalesForceAPI, SalesForceBuilder
from modules.send_email import send
from modules import utils
from modules.validator import Validator
from enrichments import base_query
from modules.yelp_data import Yelp_Data
from modules.zipcode import ZipCodeUtils
from modules.ClassificationModel.model_analysis import ModelAnalysis, SalesForceAPISession

logger = logger.initialize(os.path.join("log"))
logger.info('Logger Initialized')


class Enrichment(object):
    def __init__(self, args):
        verbose = args.get('-v')
        if verbose:
            logger.getLogger().setLevel(logger.DEBUG)
        logger.debug('Enrichment Init')
        self.machines = int(args.get('--machines')) if args.get('--machines') else 5
        self.speed = int(args.get('--speed')) if args.get('--speed') else 7
        self.limit = args.get('--limit')
        self.orderby = 'desc' if args.get('-d') else None
        self.orderby = 'asc' if args.get('-a') else self.orderby
        self.rekindle = args.get('-r')
        self.purpose = args.get('PURPOSE')
        self.days_since_enrich = args.get('DAYS_SINCE_ENRICHMENT')
        self.filename = args.get('FILE')
        self.email = args.get('EMAIL')
        self.testing = args.get('-t')
        self.debug = args.get('-e')
        self.score = args.get('-s')
        self.split = int(args.get('--split')) if args.get('--split') else None
        print 'split', self.split
        logger.info('')
        for k, v in args.iteritems():
            logger.info("Arg Value '%s' == %s" % (k, v,))
        logger.info('')

        self.invalid_reasons = [
            "bad co name", "bad fb cat", "No FB Data", "phone dispo", "Bad Co or Cat", "no street",
            "Bad Lead Source", "bad yelp cat", "wrong fb page match", "Invalid Phone Format",
            "no phone number", "phone - too many digits", "phone - lib error", "manually invalidated",
            "Duplicate Lead to FS Account", "Invalid - Manual", "phone - wrong first digit", "Valid",
            "phone - too few digits", "FB / Yelp - out of biz"
        ]

        self.enrichment_summary = {
            "bad co name": 0,
            "bad fb cat": 0,
            "No FB Data": 0,
            "phone dispo": 0,
            "phone - too few digits": 0,
            "Bad Co or Cat": 0,
            "no street": 0,
            "Bad Lead Source": 0,
            "bad yelp cat": 0,
            "wrong fb page match": 0,
            "Invalid Phone Format": 0,
            "no phone number": 0,
            "phone - too many digits": 0,
            "phone - lib error": 0,
            "manually invalidated": 0,
            "Duplicate Lead to FS Account": 0,
            "Invalid - Manual": 0,
            "phone - wrong first digit": 0,
            "FB / Yelp - out of biz": 0,
            "Valid": 0
        }

    def process_arguments(self):
        logger.debug('process_arguments')

        if self.filename[-3:] != '.py':
            exception_message = """\n
                **Must choose a file to query salesforce/redshift: %s**
                  Please refer to the app help documentation by typing "python app.py -h"
                  and read the "FILE" argument instructions
            """ % (self.filename if self.filename else 'File not provided',)
            raise Exception(exception_message)

        if not self.purpose:
            raise Exception(
                "You must provide a purose to this enrichment was: %s" % (self.purpose,))

        if self.days_since_enrich:
            if not self.days_since_enrich.isdigit() or not self.days_since_enrich > 0:
                raise Exception(
                    "valueError in days since enrich, must be positive int: was %s" % (self.days_since_enrich,))

        if self.limit and self.limit.isdigit() and int(self.limit) > 0:
            self.limit = int(self.limit)
        elif self.limit and not self.limit.isdigit():
            raise Exception(
                "Error in Limit argument value, must be a positive integer, was %s" % (self.limit))

        if self.debug:
            self.limit = 100
            self.speed = 1

        if self.split and self.orderby == 'asc':
            self.orderby = 'desc'
        elif self.split:
            self.orderby = 'desc'

        if self.score:
            self.speed = 1

        if 'rekin' in self.filename.lower() and not self.rekindle:
            # logger.warning("File name indicates rekindle enrichment but no '-r' option found")
            logger.exception("File name indicates rekindle enrichment but no '-r' option found")

        if self.filename[-3:] == '.py':
            module = self.filename[:-3]
        else:
            raise Exception("wrong file type provided, must be '.py'")

        if not self.email:
            self.email = 'james.boone@fivestars.com'

        self.module = importlib.import_module('enrichments.%s' % (module,))

        date = datetime.now(tz=pytz.utc).astimezone(pytz.timezone('US/Pacific')).strftime('%m/%d/%Y')
        self.marketing_initiative = "%s %s" % (self.purpose, date,)

    def query_data(self):
        logger.debug('query_salesforce')
        """
            Initialization of SFDC done here because we don't want to waste time if there
            are zero records to process
        """
        query_string = base_query.query_string(self.module.query, self.limit, self.orderby)
        self.validator = Validator()
        self.sf_api = SalesForceAPI(self.validator)
        if self.module.__db__ == 'salesforce':
            return self.sf_api.query(query_string, self.limit)
        elif self.module.__db__ == 'redshift':
            self.redshift_db = Redshift()
            return self.redshift_db.query(self.module.query)

    def initialize_modules(self):
        logger.debug('initialize_modules')
        sfdc = SalesForceAPISession()
        if self.score:
            self.rf = joblib.load('./modules/ClassificationModel/model/ensemble_model.pkl')
            self.train = pd.read_pickle('./modules/ClassificationModel/train_set_for_lead_scoring.pkl')
            self.model = ModelAnalysis(sfdc, True)
            # self.model.learn()  # Deletes old model and rebuilds a new one
            # self.model.get_model_results(False)
            # self.model.get_model_results(True)  # kicks off gridsearchcv to search for classifier hyperparams
        else:
            zip_data = ZipCodeUtils()
            req_proxy = requestProxy.RequestProxy(
                web_proxy_list=[
                    'cm-58-9-99-41.revip16.asianet.co.th', '183.91.33.75', '101.96.10.40',
                    '61.135.217.16', '101.96.10.38', '202.119.199.147', '183.91.33.44',
                    '202.100.167.144', '51.255.197.171', '183.91.33.42', '58.9.99.41'])
            yelp = Yelp_Data(self.validator, req_proxy, self.email)
            self.fb = Facebook(
                self.sf_api, self.validator, self.marketing_initiative, self.testing, self.rekindle, self.days_since_enrich)
            self.nearby = Nearby(version=2)
            self.sfdc_builder = SalesForceBuilder(
                zip_data, self.sf_api, yelp, self.validator, self.fb, self.marketing_initiative, self.nearby)

    def update_sfdc_objects(self, sfdc_data):
        self.sfdc_data = sfdc_data
        self.record_count = 0
        if self.score:
            d = deferToThread(self.score_leads, sfdc_data)

        else:
            d = deferToThread(self._process, self.sfdc_data)
            d.addCallback(self._update_or_create_sfdc_obj)
            d.addErrback(self.deferred_error)
        return d

    def score_leads(self, data):
        logger.info('scoring lead')
        lead_info = {}
        lead_model = self.model.format_lead(data)
        lead_info['BSC__c'] = self.model.test_data(self.rf, test=lead_model, train=self.train, lead=True)[:, 1][0]
        lead_info['BSC_version__c'] = 1.002
        lead_info['Marketing_Initiative__c'] = self.marketing_initiative
        print 'lead score: ', lead_info['BSC__c'], 'id: ', data['Id']
        return self.sf_api.update(data, lead_info)

    def deferred_error(self, failure):
        logger.error("\n\n\nError in Deferred: %s\n\n\n" % (failure,))
        self.send_report(failure)
        import sys
        sys.exit()
        raise

    def _process(self, data):
        ''' cleans the lead, then if it passes all lead validation checks, searches for facebook page
        '''
        if self.rekindle:
            data = utils.map_rekindle_fields(data, self.marketing_initiative)

        lead_info = {}
        data, lead_info = DataCleaner(data, lead_info).clean()
        if lead_info.get('Lead_Invalid_Reason__c') and not self.rekindle:
            logger.debug('Lead_Invalid_Reason__c: %s and not self.rekindle: %s' % (
                lead_info.get('Lead_Invalid_Reason__c'), self.rekindle,))
            for invalid_reason in self.invalid_reasons:
                if invalid_reason in lead_info.get('Lead_Invalid_Reason__c'):
                    self.enrichment_summary[invalid_reason] += 1

            return (data, lead_info)

        try:
            search_result = {}
            data['Latitude'], data['Longitude'] = utils.get_geo(data)

            search_result = self.fb.search(data, search_method='geo_search', distance=300)
            if not search_result.get('page_data'):
                lead_info['Lead_Invalid_Reason__c'] = 'No FB Data'
                for invalid_reason in self.invalid_reasons:
                    if invalid_reason in lead_info.get('Lead_Invalid_Reason__c'):
                        self.enrichment_summary[invalid_reason] += 1
                        break
                return (data, lead_info)

            data, lead_info = self.sfdc_builder.aggregate_data(data, search_result, lead_info)
            for invalid_reason in self.invalid_reasons:
                if invalid_reason in lead_info.get('Lead_Invalid_Reason__c'):
                    self.enrichment_summary[invalid_reason] += 1
                    break
            return (data, lead_info)
        except:
            logger.exception('Error to debug(1)')

        return (None, None)


    def _update_or_create_sfdc_obj(self, args):
        logger.debug('update_or_create_sfdc_obj')
        self.record_count += 1
        data = args[0]
        lead_info = args[1]
        if not data or not lead_info:
            logger.error("not data or not lead_info")
            return
        if data.get('rekindle'):
            # print 'Rekindle invalid reason: ', lead_info.get('Lead_Invalid_Reason__c')
            if lead_info.get('Lead_Invalid_Reason__c') not in ["Valid", "No FB Data"]:
                return self.sf_api.update_opp(data['Id'], lead_info=lead_info)
            else:
                return self.sf_api.create_rekindle_inbound_opp_1(data, lead_info)
        if lead_info['Lead_Invalid_Reason__c'] != 'Valid':
            logger.info('invalid - %s -> id: %s\n' % (
                lead_info['Lead_Invalid_Reason__c'], data['Id'], ))
            return self.sf_api.invalid_lead(data, lead_info)
        else:
            logger.info('VALID: %s\n' % (data['Id'], ))
            # lead_model = self.model.format_lead(lead_info, data)
            # lead_info['BSC__c'] = self.model.test_data(self.rf, test=lead_model, train=self.train, lead=True)[:, 1][0]
            # lead_info['BSC_version__c'] = 1.002
            lead_info['Marketing_Initiative__c'] = self.marketing_initiative
            # print 'lead score: ', lead_info['BSC__c'], 'id: ', data['Id']
            return self.sf_api.update(data, lead_info)

    def send_report(self, error):
        email_report = copy(self.enrichment_summary)
        for summary, amount in self.enrichment_summary.iteritems():
            if amount == 0:
                del email_report[summary]

        message = """
            <html><head><style>caption, table {border: 1px solid black;}</style></head><body>
            <table><tr><caption><h3>Record Summaries</h3></caption></tr><tr><th>Reasons</th>
            <th>Counts</th><th>Percent Of Total</th></tr>"""
        total = 0
        for reason, counts in email_report.iteritems():
            total += counts

        for reason, counts in email_report.iteritems():
            percent_of_total = '%.2f' % ((float(counts) / total) * 100)
            message += """
                <tr><td>%s</td><td style="text-align: center">%s</td><td style="text-align: center">%s</td></tr>
                """ % (reason, str(counts), str(percent_of_total),)

        message += '<tr><td>Total Records</td><td style="text-align: center">%s</td></tr>' % (str(total),)

        message += '</table></body></html>'

        if self.sfdc_data and len(self.sfdc_data) == self.record_count:
            logger.info('\nAll %s records processed, emailing that status is complete.' % (len(self.sfdc_data),))
            subject = "%s records processed for %s" % (len(self.sfdc_data), self.marketing_initiative, )
        else:
            logger.info('\nFailure during processing, sending email.')
            subject = "Error during processing records %s" % (self.marketing_initiative, )
        send(subject, toaddr=[self.email], message=message, error=error)

    def stop_reactor(self, args=None):
        if reactor.running:
            # self.send_report("All Good")
            reactor.stop()
    
    def chunks(self, l, n):
        for i in range(0, len(l), n):
            yield l[i:i + n]

def main():
    app = Enrichment(docopt(__doc__))
    app.process_arguments()
    sem = defer.DeferredSemaphore(app.speed)
    data = app.query_data()
    if app.split >= 0:
        nn = len(data) / app.machines
        data = [data[chunk:chunk+nn] for chunk in xrange(0, len(data), nn)][(app.split if app.split != 0 else 1) - 1]
        logger.info("Processing %s records after query split %s ways." % (len(data), app.machines,))
    app.initialize_modules()

    deferreds = []
    for record in data:
        deferreds.append(sem.run(app.update_sfdc_objects, record))
    d = defer.gatherResults(deferreds)
    d.addCallback(app.stop_reactor)
    atexit.register(app.stop_reactor)
    reactor.run()

if __name__ == '__main__':
    main()
