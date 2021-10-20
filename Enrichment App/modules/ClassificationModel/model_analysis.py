
from io import StringIO
import os
import pandas as pd
import numpy as np
import requests
from datetime import date
from docopt import docopt
from simple_salesforce import SalesforceLogin
import ipdb
import phonenumbers
from sklearn.metrics import roc_auc_score
from sklearn.grid_search import GridSearchCV
from sklearn import ensemble, cross_validation
from sklearn.tree import DecisionTreeClassifier
from sklearn.externals import joblib
from weighted_majoirty import EnsembleClassifier
from sklearn.svm import SVC
from sklearn.linear_model import LogisticRegression
from sklearn.naive_bayes import GaussianNB

import logging
logger = logging.getLogger(__name__)


class SalesForceAPISession(object):
    # Export SFDC reports: Setup
    def __init__(self):
        logger.debug("model analysis salesforce init")
        today_date = date.today().strftime('%m_%d')

        self.training_leads = 'training_leads-' + today_date + '.csv'
        self.instance_url = None
        self.access_token = None

    def export_report(self, rep_id, filename):
        logger.debug("export report")
        session_id, instance = SalesforceLogin(
            username='',
            password='',
            security_token='')

        url = 'http://' + instance + '/' + rep_id + '?export=1&enc=UTF-8&xf=csv'
        cookies = {"sid": session_id}
        r = requests.get(url,
                         allow_redirects=True,
                         data={"download_open": "Download", "format_open": ".csv"}, cookies=cookies)
        return StringIO(unicode(r.content))


def clean_phone(phone):
    if not phone:
        return '000'
    if phone and not len(phone) >= 10:
        phone_dig = 10 - len(phone)
        phone = phone + ('0' * phone_dig)
    else:
        phone = str(phonenumbers.parse(phone, 'US').national_number)
        if len(phone) < 3:
            phone += '0' * (3 - len(phone))
    return phone[:3]


class ModelAnalysis(object):
    def __init__(self, sfdc, learn=False):
        logger.debug("ModelAnalysis Init")
        pd.set_option('display.max_rows', 20)
        pd.set_option('display.max_columns', 30)
        pd.set_option('display.width', 190)
        if learn:
            self.learning = True
        else:
            self.learning = False

        self.sfdc = sfdc

    # def format_lead(self, lead_info, data):
    #     logger.debug("format_lead")
    #     lm = {'LeadID': [0],
    #           'Phone': [data.get('Phone')],
    #           'DateLost': [False],
    #           'DateLost.1': [False],
    #           'DateDMReached': [False],
    #           'DateAppointmentSet': [False],
    #           'DateMeetingHeld': [False],
    #           'DateContractSigned': [False],
    #           'FSNearby': [lead_info.get('FS_Nearby__c', 0)],
    #           # 'FBWebsite': [lead_info.get('FB_Website__c', np.nan)],
    #           'CampaignCategory': [data.get('Campaign_Category__c')],
    #           'FBCategory': [lead_info.get('FB_Category__c')],
    #           'YelpCategory': [lead_info.get('Yelp_Category__c')],
    #           'LeadSource': [data.get('LeadSource')],
    #           # 'PardotScoreatLastTransferDate': [data.get('Pardot_Score_at_Last_Transfer_Date__c')],
    #           'FBPostEngagement': [lead_info.get('FB_Post_Engagement__c')],
    #           'FBDaysSinceOldestPost': [lead_info.get('FB_Days_Since_Oldest_Post__c')],
    #           'FBDaysSinceLastPost': [lead_info.get('FB_Days_Since_Last_Post__c')],
    #           'FBCheckins': [lead_info.get('FB_Checkins__c')],
    #           'FBLikes': [lead_info.get('FB_Likes__c')],
    #           'FBCountofPost': [lead_info.get('FB_Count_of_Post__c')],
    #           'NumberisCell': [lead_info.get('Number_is_Cell__c')]}
    #     try:
    #         lead = pd.DataFrame(lm)
    #         lead['AreaCode'] = lead['Phone']
    #         lead['LastStageofRecord'] = 'New lead'
    #         logger.debug('Cleaning Phone Numbers')
    #         for i, row in lead.iterrows():
    #             lead.loc[i, 'AreaCode'] = clean_phone(str(row['AreaCode']))
    #         logger.debug('Phone Numbers Cleaned')
    #         lead['LastStageofRecord'] = pd.Categorical(
    #             lead.LastStageofRecord, categories=['New Lead'], ordered=True)
    #         # lead['fb_domain_com'] = 0
    #         # lead['fb_domain_org'] = 0
    #         # lead['fb_domain_net'] = 0
    #         # lead['fb_domain_other'] = self.get_domain(lead_info.get('FB_Website__c'))
    #         # lead['fb_domain_none'] = 0
    #         # lead.FBWebsite.where(lead.FBWebsite==None, 0)
    #         # lead.loc[lead.FBWebsite.isnull(), 'fb_domain_none'] = 1
    #         # lead.loc[lead.FBWebsite.str.contains('com'), 'fb_domain_com'] = 1
    #         # lead.loc[lead.FBWebsite.str.contains('org'), 'fb_domain_org'] = 1
    #         # lead.loc[lead.FBWebsite.str.contains('net'), 'fb_domain_net'] = 1
    #         lead = lead.drop([
    #             'DateContractSigned', 'DateMeetingHeld', 'DateAppointmentSet', 'DateLost',
    #             'Phone', 'DateLost.1', 'DateDMReached'], 1)
    #         return lead
    #     except Exception, err:
    #         print err
    #         ipdb.set_trace()
    #         print err
    def format_lead(self, data, lead_info=None):
        logger.debug("format_lead")
        lm = {'LeadID': [0],
              'Phone': [data.get('Phone')],
              'DateLost': [False],
              'DateLost.1': [False],
              'DateDMReached': [False],
              'DateAppointmentSet': [False],
              'DateMeetingHeld': [False],
              'DateContractSigned': [False],
              'FSNearby': [data.get('FS_Nearby__c', 0)],
              # 'FBWebsite': [data.get('FB_Website__c', np.nan)],
              'CampaignCategory': [data.get('Campaign_Category__c')],
              'FBCategory': [data.get('FB_Category__c')],
              'YelpCategory': [data.get('Yelp_Category__c')],
              'LeadSource': [data.get('LeadSource')],
              # 'PardotScoreatLastTransferDate': [data.get('Pardot_Score_at_Last_Transfer_Date__c')],
              'FBPostEngagement': [data.get('FB_Post_Engagement__c')],
              'FBDaysSinceOldestPost': [data.get('FB_Days_Since_Oldest_Post__c')],
              'FBDaysSinceLastPost': [data.get('FB_Days_Since_Last_Post__c')],
              'FBCheckins': [data.get('FB_Checkins__c')],
              'FBLikes': [data.get('FB_Likes__c')],
              'FBCountofPost': [data.get('FB_Count_of_Post__c')],
              'NumberisCell': [data.get('Number_is_Cell__c')]}
        try:
            lead = pd.DataFrame(lm)
            lead['AreaCode'] = lead['Phone']
            lead['LastStageofRecord'] = 'New lead'
            logger.debug('Cleaning Phone Numbers')
            for i, row in lead.iterrows():
                lead.loc[i, 'AreaCode'] = clean_phone(str(row['AreaCode']))
            logger.debug('Phone Numbers Cleaned')
            lead['LastStageofRecord'] = pd.Categorical(
                lead.LastStageofRecord, categories=['New Lead'], ordered=True)
            # lead['fb_domain_com'] = 0
            # lead['fb_domain_org'] = 0
            # lead['fb_domain_net'] = 0
            # lead['fb_domain_other'] = self.get_domain(lead_info.get('FB_Website__c'))
            # lead['fb_domain_none'] = 0
            # lead.FBWebsite.where(lead.FBWebsite==None, 0)
            # lead.loc[lead.FBWebsite.isnull(), 'fb_domain_none'] = 1
            # lead.loc[lead.FBWebsite.str.contains('com'), 'fb_domain_com'] = 1
            # lead.loc[lead.FBWebsite.str.contains('org'), 'fb_domain_org'] = 1
            # lead.loc[lead.FBWebsite.str.contains('net'), 'fb_domain_net'] = 1
            lead = lead.drop([
                'DateContractSigned', 'DateMeetingHeld', 'DateAppointmentSet', 'DateLost',
                'Phone', 'DateLost.1', 'DateDMReached'], 1)
            return lead
        except Exception, err:
            print err
            ipdb.set_trace()
            print err
    
    def get_domain(self, website):
        import re
        if re.search('.*\.([\w.]+)', website if website else ''):
            domains = ['com', 'org', 'net']
            if re.search('.*\.([\w.]+)', website).groups()[0] not in domains:
                return 1
        return 0

    def learn(self):
        logger.debug('learn')

        try:
            leads = './modules/ClassificationModel/leads.pkl'
            train = './modules/ClassificationModel/train.pkl'
            test = './modules/ClassificationModel/test.pkl'
            os.remove(leads)
            os.remove(train)
            os.remove(test)
        except OSError:
            pass
        try:
            csv_file = self.sfdc.export_report('00OE0000002xmBD', self.sfdc.training_leads)
            leads = pd.read_csv(csv_file, skipfooter=6, engine='python')
            leads.columns = [col.replace(' ', '') for col in leads.columns.values]
            leads['DateLeadLost'] = leads['DateLost'].isnull()
            leads['DateOppLost'] = leads["DateLost.1"].isnull()
            leads['DateDMReached'] = leads['DateDMReached'].isnull()
            leads['DateAppointmentSet'] = leads['DateAppointmentSet'].isnull()
            leads['DateMeetingHeld'] = leads['DateMeetingHeld'].isnull()
            leads['DateContractSigned'] = leads['DateContractSigned'].isnull()
            leads = leads.replace({True: False, False: True})
            leads['AreaCode'] = leads['Phone']

            for i, row in leads.iterrows():
                if (row['DateLeadLost'] == True) & (row['DateDMReached'] == False):
                    leads.loc[i, 'LastStageofRecord'] = 'Lost During Call'
                elif (row['DateDMReached'] == True) & (row['DateAppointmentSet'] == False):
                    leads.loc[i, 'LastStageofRecord'] = 'Lost At DM'
                elif (row['DateAppointmentSet']) == True & (row['DateMeetingHeld'] == False):
                    leads.loc[i, 'LastStageofRecord'] = 'Lost At Set'
                elif (row['DateMeetingHeld'] == True) & (row['DateContractSigned'] == False):
                    leads.loc[i, 'LastStageofRecord'] = 'Lost At Held'
                elif row['DateContractSigned'] == True:
                    leads.loc[i, 'LastStageofRecord'] = 'Contract Signed'
                leads.loc[i, 'AreaCode'] = clean_phone(str(row['AreaCode']))
            leads = leads.drop(['DateContractSigned', 'DateMeetingHeld', 'DateAppointmentSet', 'DateLeadLost',
                'DateLost', 'DateOppLost', 'Phone', 'DateLost.1', 'DateDMReached'], 1)

            leads['LastStageofRecord'] = pd.Categorical(leads.LastStageofRecord,
                                                        categories=['Lost During Call', 'Lost At DM',
                                                                    'Lost At Set', 'Lost At Held',
                                                                    'Contract Signed'],
                                                        ordered=True)
            # domains = ['com', 'org', 'net']
            # leads['fb_domain_com'] = 0
            # leads['fb_domain_org'] = 0
            # leads['fb_domain_net'] = 0
            # leads['fb_domain_other'] = self.get_domain(leads.FBWebsite.str)
            # leads['fb_domain_none'] = 0
            # leads.loc[leads.FBWebsite.isnull(), 'fb_domain_none'] = 1
            # leads.loc[leads.FBWebsite.str.contains('com', na=False), 'fb_domain_com'] = 1
            # leads.loc[leads.FBWebsite.str.contains('org', na=False), 'fb_domain_org'] = 1
            # leads.loc[leads.FBWebsite.str.contains('net', na=False), 'fb_domain_net'] = 1
            # ===============================================================================================
            # Generate 70/30 train/test split
            train_ids = leads.LeadID.sample(int(leads.shape[0] * .7))
            train = leads[leads.LeadID.isin(train_ids)].copy()
            test = leads[~leads.LeadID.isin(train_ids)].copy()

            # ===============================================================================================
            # Write the data to disc
            leads.to_pickle('./modules/ClassificationModel/leads.pkl')
            train.to_pickle('./modules/ClassificationModel/train.pkl')
            test.to_pickle('./modules/ClassificationModel/test.pkl')
        except Exception, err:
            print err
            ipdb.set_trace()
            print err


    def transform_test(self, train, test, lead=None, cat_count_thresh=30):
        try:
            logger.debug('transform_test')
            # Transforms the given test set based on values in the train set

            self.train1 = train.copy()
            self.test1 = test.copy()
            # --------------------------------------------------
            # Conversion rate by CampaignCategory
            self.train1['CampaignCategoryCount'] = self.train1.groupby('CampaignCategory')['CampaignCategory'].transform(lambda x: len(x))
            self.train1.loc[self.train1.CampaignCategoryCount < cat_count_thresh, 'CampaignCategory'] = 'OtherCampaign'
            campaignMap = self.train1.groupby('CampaignCategory').agg({'CampaignCategory': 'count', 'Target': 'sum'})
            campaignMap.rename(columns={'CampaignCategory': 'CampaignCategoryCount'}, inplace=True)
            campaignMap.reset_index(inplace=True)
            campaignMap['CampaignConversionRate'] = campaignMap.Target / campaignMap.CampaignCategoryCount

            # Map to the test set
            self.test1.loc[~self.test1.CampaignCategory.isin(campaignMap.CampaignCategory), 'CampaignCategory'] = 'OtherCampaign'
            self.test1 = self.test1.merge(campaignMap[['CampaignCategory', 'CampaignConversionRate']], how='left', on='CampaignCategory')

            # --------------------------------------------------
            # Conversion rate by FBCategory
            self.train1['FBCategoryCount'] = self.train1.groupby('FBCategory')['FBCategory'].transform(lambda x: len(x))
            self.train1.loc[self.train1.FBCategoryCount < cat_count_thresh, 'FBCategory'] = 'OtherFB'
            fbMap = self.train1.groupby('FBCategory').agg({'FBCategory': 'count', 'Target': 'sum'})
            fbMap.rename(columns={'FBCategory': 'FBCategoryCount'}, inplace=True)
            fbMap.reset_index(inplace=True)
            fbMap['FBConversionRate'] = fbMap.Target / fbMap.FBCategoryCount

            # Map to the test set
            self.test1.loc[~self.test1.FBCategory.isin(fbMap.FBCategory), 'FBCategory'] = 'OtherFB'
            self.test1 = self.test1.merge(fbMap[['FBCategory', 'FBConversionRate']], how='left', on='FBCategory')

            # --------------------------------------------------
            # Conversion rate by YelpCategory
            self.train1['YelpCategoryCount'] = self.train1.groupby('YelpCategory')['YelpCategory'].transform(lambda x: len(x))
            self.train1.loc[self.train1.YelpCategoryCount < cat_count_thresh, 'YelpCategory'] = 'OtherYelp'
            yelpMap = self.train1.groupby('YelpCategory').agg({'YelpCategory': 'count', 'Target': 'sum'})
            yelpMap.rename(columns={'YelpCategory': 'YelpCategoryCount'}, inplace=True)
            yelpMap.reset_index(inplace=True)
            yelpMap['YelpConversionRate'] = yelpMap.Target / yelpMap.YelpCategoryCount

            # Map to the test set
            self.test1.loc[~self.test1.YelpCategory.isin(yelpMap.YelpCategory), 'YelpCategory'] = 'OtherYelp'
            self.test1 = self.test1.merge(yelpMap[['YelpCategory', 'YelpConversionRate']], how='left', on='YelpCategory')

            # ----------------------------------------- ---------
            # Conversion rate by LeadSource
            self.train1['LeadSourceCount'] = self.train1.groupby('LeadSource')['LeadSource'].transform(lambda x: len(x))
            self.train1.loc[self.train1.LeadSourceCount < cat_count_thresh, 'LeadSource'] = 'OtherLeadSource'
            leadsourceMap = self.train1.groupby('LeadSource').agg({'LeadSource': 'count', 'Target': 'sum'})
            leadsourceMap.rename(columns={'LeadSource': 'LeadSourceCount'}, inplace=True)
            leadsourceMap.reset_index(inplace=True)
            leadsourceMap['LeadSourceConversionRate'] = leadsourceMap.Target / leadsourceMap.LeadSourceCount

            # Map to the test set
            self.test1.loc[~self.test1.LeadSource.isin(leadsourceMap.LeadSource), 'LeadSource'] = 'OtherLeadSource'
            self.test1 = self.test1.merge(leadsourceMap[['LeadSource', 'LeadSourceConversionRate']], how='left', on='LeadSource')

            # --------------------------------------------------
            # Conversion rate by AreaCode
            self.train1['AreaCodeCount'] = self.train1.groupby('AreaCode')['AreaCode'].transform(lambda x: len(x))
            self.train1.loc[self.train1.AreaCodeCount < cat_count_thresh, 'AreaCode'] = 'OtherAreaCode'
            areacodeMap = self.train1.groupby('AreaCode').agg({'AreaCode': 'count', 'Target': 'sum'})
            areacodeMap.rename(columns={'AreaCode': 'AreaCodeCount'}, inplace=True)
            areacodeMap.reset_index(inplace=True)
            areacodeMap['AreaCodeConversionRate'] = areacodeMap.Target / areacodeMap.AreaCodeCount

            # Map to the test set
            self.test1.loc[~self.test1.AreaCode.isin(areacodeMap.AreaCode), 'AreaCode'] = 'OtherAreaCode'
            self.test1 = self.test1.merge(areacodeMap[['AreaCode', 'AreaCodeConversionRate']], how='left', on='AreaCode')

            # --------------------------------------------------
            # Numeric Fields
            self.test1.FBPostEngagement.fillna(-100, inplace=True)
            self.test1.FBDaysSinceOldestPost.fillna(-100, inplace=True)
            self.test1.FBDaysSinceLastPost.fillna(-100, inplace=True)
            self.test1.FBCheckins.fillna(-100, inplace=True)
            self.test1.FBLikes.fillna(-100, inplace=True)
            self.test1.FBCountofPost.fillna(-100, inplace=True)
            self.test1.NumberisCell.fillna(-100, inplace=True)
            self.test1.FSNearby.fillna(-100, inplace=True)
            self.test1.FBPostEngagement.replace(to_replace='0', value=-100, inplace=True)
            self.test1.FBDaysSinceOldestPost.replace(to_replace='0', value=-100, inplace=True)
            self.test1.FBDaysSinceLastPost.replace(to_replace='0', value=-100, inplace=True)
            self.test1.FBCheckins.replace(to_replace='0', value=-100, inplace=True)
            self.test1.FBLikes.replace(to_replace='0', value=-100, inplace=True)
            self.test1.FBCountofPost.replace(to_replace='0', value=-100, inplace=True)
            self.test1.NumberisCell.replace(to_replace='0', value=-100, inplace=True)
            self.test1.FSNearby.replace(to_replace='0', value=-100, inplace=True)
            # self.test1.PardotScoreatLastTransferDate.fillna(-1, inplace=True)
            # self.test1.YelpPhotos.fillna(-1, inplace=True)
            # self.test1.YelpReviewCount.fillna(-1, inplace=True)

            # --------------------------------------------------
            # return the result


            features = [
                'CampaignConversionRate', 'FBConversionRate', 'YelpConversionRate',
                'LeadSourceConversionRate', 'NumberisCell',
                'FBPostEngagement', 'FBDaysSinceOldestPost', 'FBDaysSinceLastPost', 'FBCheckins',
                'FBLikes', 'FBCountofPost', 'FSNearby'
            ]

            # if Target is in the test set, return it as well
            if(test.columns.str.contains('Target').sum() > 0):
                features = features + ['Target']
        except Exception, err:
            print err
            logger.exception("error in transform_test")
        return self.test1[features]

    def get_model_results(self, build_ensemble):
        try:
            logger.info("get_model_results")
            # ===============================================================================================
            # Train a random forest model on the entire training set
            np.random.seed(123)
            if build_ensemble:
                rf_base_1 = ensemble.RandomForestClassifier  # (n_jobs=-1)  # max_features=5, min_samples_leaf=819)  #, n_estimators=2000, n_jobs=-1)
                rf_base_2 = ensemble.RandomForestClassifier
                ex_base_1 = ensemble.ExtraTreesClassifier  # (n_jobs=-1)
                ex_base_2 = ensemble.ExtraTreesClassifier
                gb_base = ensemble.GradientBoostingClassifier  # ()
                # dt_base = DecisionTreeClassifier  # ()
            else:
                # dt_base = DecisionTreeClassifier(criterion='entropy', max_features=3, max_depth=2, min_samples_leaf=1455)
                # gb_base = ensemble.GradientBoostingClassifier(max_features=3, min_samples_leaf=9)
                rf_base_1 = ensemble.RandomForestClassifier(max_features=3, min_samples_leaf=205, n_estimators=1700, n_jobs=-1)
                rf_base_2 = ensemble.RandomForestClassifier(max_features=4, min_samples_leaf=405, n_estimators=2700, n_jobs=-1)
                ex_base_1 = ensemble.ExtraTreesClassifier(max_features=7, min_samples_leaf=110, n_estimators=500, n_jobs=-1)
                ex_base_2 = ensemble.ExtraTreesClassifier(max_features=3, min_samples_leaf=310, n_estimators=1000, n_jobs=-1)
                bc_base_1 = ensemble.BaggingClassifier(base_estimator=ex_base_1, n_estimators=5, random_state=2526)
                bc_base_2 = ensemble.BaggingClassifier(base_estimator=rf_base_1, n_estimators=5, random_state=2526)
                bc_base_3 = ensemble.BaggingClassifier(base_estimator=ex_base_2, n_estimators=5, random_state=2526)
                bc_base_4 = ensemble.BaggingClassifier(base_estimator=rf_base_2, n_estimators=5, random_state=2526)
                vc_base_1 = ensemble.VotingClassifier(estimators=[('bc_base_1', bc_base_1), ('bc_base_2', bc_base_2), ('bc_base_3', bc_base_3), ('bc_base_4', bc_base_4)], voting='soft')

            # svc_base = SVC()
            # lg_base = LogisticRegression()
            # gnb_base = GaussianNB()

            # used for gridsearch
            # svc_base = SVC()
            # lg_base = LogisticRegression(n_jobs=-1)
            # gnb_base = GaussianNB()
            # rf_base = ensemble.AdaBoostClassifier(rf_base, n_estimators=10, random_state=777)
            # ex_base = ensemble.AdaBoostClassifier(ex_base, n_estimators=10, random_state=293)
            # gb_base = ensemble.AdaBoostClassifier(dt_base, n_estimators=10, random_state=3777)
            # dt_base = ensemble.AdaBoostClassifier(gb_base, n_estimators=10, random_state=7477)

            models = [
                # ('Ensemble Model', True),
                # ('DecisionTreeClassifier', dt_base),
                # ('GradientBoostingClassifier', gb_base),
                ('BaggingClassifier_1', bc_base_1),
                ('BaggingClassifier_2', bc_base_2),
                ('RandomForestClassifier_1', rf_base_1),
                ('RandomForestClassifier_2', rf_base_2),
                ('ExtraTreesClassifier_1', ex_base_1),
                ('ExtraTreesClassifier_2', ex_base_2),
                ('VotingClassifier_1', vc_base_1),
                # ('SVC', svc_base),
                # ('GaussianNB', gnb_base),
                # ('Logistic Regression', lg_base)
                # ('AdaBoostClassifier-gb_base', ensemble.AdaBoostClassifier(svc_base))  #, n_estimators=10, random_state=777))
            ]

            # ===============================================================================================
            # Load data

            # Contract Signed
            self.train = pd.read_pickle('./modules/ClassificationModel/train.pkl')
            self.train = self.train.drop(['FBWebsite', 'fb_domain_com', 'fb_domain_net', 'fb_domain_org', 'fb_domain_other','fb_domain_none'], 1)
            test = pd.read_pickle('./modules/ClassificationModel/test.pkl')

            # ------------------------------------------
            # Set the target variable

            # Contract Signed
            self.train['Target'] = self.train.LastStageofRecord == 'Contract Signed'
            self.train.to_pickle('./modules/ClassificationModel/train_set_for_lead_scoring.pkl')
            test['Target'] = test.LastStageofRecord == 'Contract Signed'

            # # Meeting Held
            # self.train['Target'] = self.train.LastStageofRecord >= 'Lost at Held'
            # test['Target'] = test.LastStageofRecord >= 'Lost at Held'

            # ===============================================================================================
            # Build a modified training dataset

            # Methodology:
            # Randomly split the training data into train_A and train_B
            # Use train_A to generate conversion rates by various groups and map to train_B to generate test_B.
            # Then do the same with test_B to generate test_A
            # Combine test_A and test_B to generate train_AB

            # train_A and train_B
            train_A_ids = self.train.LeadID.sample(int(self.train.shape[0] * .5), random_state=2357)
            train_A = self.train[self.train.LeadID.isin(train_A_ids)].copy()
            train_B = self.train[~self.train.LeadID.isin(train_A_ids)].copy()
            # self.test_A and test_B
            test_B = self.transform_test(train_A, train_B).copy()
            test_B['Fold'] = 'B'
            test_A = self.transform_test(train_B, train_A).copy()
            test_A['Fold'] = 'A'
            train_AB = pd.concat([test_A, test_B], axis=0)
            train_AB = train_AB.reset_index(drop=True)

            # cv iterable for grid search [(test_B, test_A), (test_A, test_B)]
            my_folds = [
                (train_AB[train_AB.Fold == 'B'].index.values.astype(int), train_AB[train_AB.Fold == 'A'].index.values.astype(int)),
                (train_AB[train_AB.Fold == 'A'].index.values.astype(int), train_AB[train_AB.Fold == 'B'].index.values.astype(int))
            ]

            # ===============================================================================================
            # Search for the best hyperparameters

            # # features to keep
            features = list(set(train_AB.columns.tolist()) - set(['Fold', 'LeadID', 'Target']))
            model_obj_list = []
            model_name_list = []
            weight = []
            for value in zip(models):
                weight.append(1)
                for name, model in value:
                    model_obj_list.append(model)
                    model_name_list.append(name)

            # quick training (~20 min) (912 fits)
            params = {
                'DecisionTreeClassifier': {'criterion': ['entropy', 'gini'], 'splitter': ['best', 'random'], 'max_depth': [3, 4, 5], 'min_samples_leaf': range(240, 501, 20), 'max_features': range(2, 17, 2)},
                'GradientBoostingClassifier': {'max_features': range(3, 5, 1), 'min_samples_leaf': range(8, 10, 1), 'n_estimators': range(1800, 2200, 200), 'learning_rate': np.linspace(0.6, 1.0, 3)},
                'ExtraTreesClassifier': {'max_features': range(8, 10, 1), 'min_samples_leaf': range(100, 121, 10), 'n_estimators': range(400, 601, 100)},
                'RandomForestClassifier': {'max_features': range(2, 4, 1), 'min_samples_leaf': range(190, 211, 10), 'n_estimators': range(1600, 1801, 100)},
            }


            # 'AdaBoostClassifier':  {'max_features': range(2, 6, 1), 'min_samples_leaf': range(4, 8, 1), 'n_estimators': range(1800, 2200, 100)},
            # 'SVC': {'kernel': ['linear', 'rbf'], 'gamma': [0.001, 0.0001], 'probability': [False]},
            # 'LogisticRegression': {'solver': ['newton-cg', 'lbfgs'], 'random_state': [0], 'multi_class': ['ovr'], 'max_iter': [300, 400, 500, 1000, 2000]}

            # # run grid search
            if build_ensemble:
                best_models = []
                for idx, model in enumerate(model_obj_list):
                    logger.info("Calcuating best paramaters for: %s" % (model_name_list[idx],))
                    clf = GridSearchCV(model(), params[model_name_list[idx]], cv=my_folds, verbose=1, n_jobs=2, scoring='roc_auc')
                    clf.fit(train_AB[features], train_AB['Target'].values)
                    best_models.append(model(**clf.best_params_))
                    logger.info("Selected: Best params: %s - Best Score: %s" % (str(clf.best_params_), str(clf.best_score_), ))
            else:
                best_models = model_obj_list
            eclf = EnsembleClassifier(clfs=best_models, weights=weight)
            for clf, label in zip(best_models + [eclf], model_name_list + ['Ensemble']):
                scores = cross_validation.cross_val_score(clf, train_AB[features], train_AB['Target'].values, cv=my_folds, scoring='roc_auc')
                logger.info("Accuracy: %0.5f (+/- %0.3f) [%s]" % (scores.mean(), scores.std(), label, ))
                if label in ['RandomForestClassifier_1', 'RandomForestClassifier_2']:
                    temp = clf.fit(X=train_AB[features], y=train_AB['Target'].values)
                    feature_importances = self.get_feature_importances(columns=features, model=temp)
                    print '\nFeature Importances\n', feature_importances, '\n\n'
                    del temp
            self.rf = clf

            self.rf.fit(X=train_AB[features], y=train_AB['Target'].values)

            # ===============================================================================================
            # Predict on the test set
            test = self.test_data(train=self.train, test=test, model=True)

            test.is_copy = False
            test['ProbTarget'] = self.rf.predict_proba(test[features])[:, 1]
        
            if build_ensemble:
                joblib.dump(self.rf, './modules/ClassificationModel/model/ensemble_model.pkl')
            # ===============================================================================================
            # Evaluate models

            # --------------------------------------------------
            # Rank the predictions
            test.sort_values('ProbTarget', ascending=False, inplace=True)
            test['PredRk'] = np.arange(test.shape[0])

            # --------------------------------------------------
            # Measure ROC AUC
            logger.info("ROC AUC Score: %s " % (roc_auc_score(y_true=test.Target.values, y_score=test.ProbTarget.values)))

            # --------------------------------------------------
            # Check the top 10 predictions and the bottom 10 predictions
            test[features + ['ProbTarget', 'Target']].head(10)
            test[features + ['ProbTarget', 'Target']].tail(10)

            # --------------------------------------------------
            # Groups of 5% bucket predictions

            test['PredRkBin'] = pd.cut(
                x=test.PredRk,
                bins=range(0, len(test), len(test) / 20),
                right=False,
                labels=None,
                retbins=False,
                precision=3,
                include_lowest=True
            )

            # binned = test.groupby('PredRkBin').agg({'Target': ['sum', 'count'], 'ProbTarget':['mean']})
            binned = test.groupby('PredRkBin').agg({'Target': ['sum', 'count'], 'ProbTarget':['min', 'max']})
            binned.columns = ['Min Score', 'Max Score', 'Target', 'Leads']
            binned['ConversionRate'] = binned.Target / binned.Leads
            logger.info("\nBinned values: \n%s" % (binned, ))

            true_overall_hit_ratio = float(test.Target.sum()) / test.shape[0]
            overall_hit_ratio = .0031
            top_decile_hit_ratio = test[test.PredRk <= int(test.shape[0] * .1)].Target.sum() / float(int(test.shape[0] * .1))
            top_two_decile_hit_ratio = test[test.PredRk <= int(test.shape[0] * .2)].Target.sum() / float(int(test.shape[0] * .2))
            top_lift = top_decile_hit_ratio / overall_hit_ratio
            true_top_lift = top_decile_hit_ratio / true_overall_hit_ratio
            top_two_lift = top_two_decile_hit_ratio / overall_hit_ratio
            true_top_two_lift = top_two_decile_hit_ratio / true_overall_hit_ratio
            logger.info("Results\n")
            logger.info("True Overall: %s%s" % (str(round(true_overall_hit_ratio * 100, 2)), "%"))
            logger.info("Overall: %s%s" % (str(round(overall_hit_ratio * 100, 2)), "%"))
            decile = 1
            for i in range(10):
                decile_val = decile / float(10)
                hit_ratio = test[test.PredRk <= int(
                    test.shape[0] * decile_val)].Target.sum() / float(int(test.shape[0] * decile_val))
                logger.info("Decile-%s%s Conversion: %s%s" % (decile_val * 100, '%', str(round(hit_ratio * 100, 2)), "%"))
                decile += 1
            logger.info("Top Lift on simulated %s%s: %s%s" % (overall_hit_ratio * 100, '%', str(round((top_lift - 1) * 100, 2)), "%"))
            logger.info("Top Two Lift on simulated %s%s: %s%s" % (overall_hit_ratio * 100, '%', str(round((top_two_lift - 1) * 100, 2)), "%"))
            logger.info("True Top Lift on actual %s%s: %s%s" % (round(true_overall_hit_ratio * 100, 2), '%', str(round((true_top_lift - 1) * 100, 2)), "%"))
            logger.info("True Top Two Lift on actual %s%s: %s%s\n" % (round(true_overall_hit_ratio * 100, 2), '%', str(round((true_top_two_lift - 1) * 100, 2)), "%"))
        except Exception, err:
            print err
            ipdb.set_trace()
            print err

    def test_data(self, rf=None, test=None, train=None, lead=False, model=False):
        logger.debug("test_data")
        if lead:
            value = rf.predict_proba(self.transform_test(train, test, lead))
            return value
        elif model:
            return self.transform_test(train, test)

    def get_feature_importances(self, columns, model=None):
        return pd.Series(model.feature_importances_, index=columns).sort_values(ascending=False)
