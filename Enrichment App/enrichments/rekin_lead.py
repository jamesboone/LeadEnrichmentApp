import logging
logger = logging.getLogger(__name__)

__db__ = 'salesforce'

query = """
    SELECT Id, Owner_Id__c, Campaign_Category__c, Status,
        Lead_Enrichment_Stage__c, Phone, Company, Street, City, State, PostalCode,
        FirstName, LastName, LeadSource, Campaign_Comp_Level__c, Phone_Disposition__c,
        Phone_Carrier__c, Number_is_Cell__c, RV_Co_Name__c, Latitude,
        Longitude, FB_Enriched_Date__c, Marketing_Initiative__c, Yelp_Page__c,
        Lead_Invalid_Reason__c, FB_Category__c, Yelp_Category__c,
        FB_Checkins__c, FB_Days_Since_Last_Post__c, FB_Days_Since_Oldest_Post__c,
        FB_Post_Engagement__c, FB_Website__c, Date_DM_Reached__c
    FROM Lead
    where Owner_Id__c like '%00GE000000369DZ%'
        and Campaign_Category__c like '%ekind%'
        and FB_Enriched_Date__c = null
"""
