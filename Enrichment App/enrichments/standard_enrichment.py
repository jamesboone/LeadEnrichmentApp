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
            WHERE Status = 'Pending'
                and Bad_Data__c = False
                and Lead_Enrichment_Stage_TESTING__c = '1.0 Not Enriched'
                and ((Campaign_Comp_Level__c = 'Cold')
                    or (Campaign_Comp_Level__c = 'cold'))
                and ((LeadSource = 'Infogroup')
                    or (LeadSource = 'Yellow Pages')
                    or (LeadSource = 'Global Analytics')
                    or (LeadSource = 'Yelp Scrape')
                    or (LeadSource = 'Yellow Pages Scrape')
                    or (LeadSource = 'Website Scrape')
                    or (LeadSource = 'SMART Lead')
                    or (LeadSource = 'Radius')
                    or (LeadSource = 'infoUSA')
                    or (LeadSource = 'GlobalDeal')
                    or (LeadSource = 'Competitor Scrape')
                    or (LeadSource = 'Cold Call')
                    or (LeadSource = 'Belly Scrape'))
                and (not Campaign_Category__c like '%ekin%')
                and Lead_Invalid_Reason__c = null
                and (not Phone_Disposition__c like 'Disconnected%')
           """
