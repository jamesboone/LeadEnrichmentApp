import logging
logger = logging.getLogger(__name__)

__db__ = 'salesforce'

query = """
            SELECT Id, Owner_Id__c, Lead_Enrichment_Stage_TESTING__C, Campaign_Category__c, Status,
                Lead_Enrichment_Stage__c, Phone, Company, Street, City, State, PostalCode,
                FirstName, LastName, LeadSource, Campaign_Comp_Level__c, Phone_Disposition__c,
                Phone_Carrier__c, Number_is_Cell__c, RV_Co_Name__c, Latitude,
                Longitude, FB_Enriched_Date__c, Marketing_Initiative__c, Yelp_Page__c,
                Lead_Invalid_Reason__c
            FROM Lead
            WHERE ID in ('00QE000001AGVrL','00QE0000016qbkK','00QE0000016oNeR','00QE000001AGKsA','00QE000000hmoit','00QE000000hmr1F','00QE000000bBQYO','00QE000000Q4kUy','00QE000000kOzxW','00QE000000daE9c','00QE0000012H3RG','00QE000000Z9Udu','00QE0000012Fqpj','00QE0000016qMBs','00QE0000012Fuyh','00QE0000016oNxa','00QE0000016qPBC','00QE000000ZQXm5','00QE000000kPJCf')
                and Status != 'Closed Converted'
           """
