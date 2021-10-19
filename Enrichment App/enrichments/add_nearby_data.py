import logging
logger = logging.getLogger(__name__)

__db__ = 'salesforce'

query = """
            SELECT Id, Owner_Id__c, Campaign_Category__c, Status,
                Lead_Enrichment_Stage__c, Phone, Company, Street, City, State, PostalCode,
                FirstName, LastName, LeadSource, Campaign_Comp_Level__c, Phone_Disposition__c,
                Phone_Carrier__c, Number_is_Cell__c, RV_Co_Name__c, Latitude,
                Longitude, FB_Enriched_Date__c, Marketing_Initiative__c, Yelp_Page__c,
                Lead_Invalid_Reason__c
            FROM Lead
            WHERE Marketing_Initiative__c = 'rekindle inside 07/20/2016'
        """
