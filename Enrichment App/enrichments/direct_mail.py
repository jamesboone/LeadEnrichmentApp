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
            WHERE (not Status = 'Closed Converted')
                and (not Status = 'Closed Lost')
                and Bad_Data__c = False
                and (FB_Enriched_Date__c < 2016-07-19
                    or FB_Enriched_Date__c = null)
                and (not Marketing_Initiative__c like '%7/%%/2016%')
                and Last_Direct_Mail_Date__c = null
                and ((Lead_Enrichment_Stage__c = 'Marketing Approved Lead')
                    or (Lead_Enrichment_Stage__c = 'NA - Unassigned ERR')
                    or (Lead_Enrichment_Stage__c = 'Enriched'))
                and ((Campaign_Category__c = 'Fast Casual')
                    or (Campaign_Category__c = 'Hobby Shops')
                    or (Campaign_Category__c = 'Juice Shops')
                    or (Campaign_Category__c = 'New Businesses')
                    or (Campaign_Category__c = 'New Businesses - Food')
                    or (Campaign_Category__c = 'No Category')
                    or (Campaign_Category__c = 'Nutrition Shops')
                    or (Campaign_Category__c = 'Paintball Shops')
                    or (Campaign_Category__c = 'Pet Shops')
                    or (Campaign_Category__c = 'Retail')
                    or (Campaign_Category__c = 'Retail Shops')
                    or (Campaign_Category__c = 'Smoke Shops')
                    or (Campaign_Category__c = 'Supplement Shops')
                    or (Campaign_Category__c = 'Toy Shops')
                    or (Campaign_Category__c = 'Vape Shops')
                    or (Campaign_Category__c = 'Vapor Shops')
                    or (Campaign_Category__c = 'Video Game Shops')
                    or (Campaign_Category__c = 'Yelp Advertisers'))
                and (not Street = null)
                and (not City = null)
                and (not State = null)
                and ((Lead_Invalid_Reason__c = null)
                    or (Lead_Invalid_Reason__c = 'Valid'))
                and (not Lead_Status_Detail__c = 'Wrong Number/Company')
                and (not Lead_Status_Detail__c = 'Unqualified - No Internet')
                and (not Lead_Status_Detail__c = 'Unqualified - Generic')
                and (not Lead_Status_Detail__c = 'DNC')
                and (not Lead_Status_Detail__c = 'Unqualified - Bad Industry Fit')
                and (not Lead_Status_Detail__c = 'Unqualified - Card Holder')
                and (not Lead_Status_Detail__c = 'Unqualified - International location')
                and (not Lead_Status_Detail__c = 'Unqualified - Online Business')
                and (not Lead_Status_Detail__c = 'Rep Declined')
                and (not Lead_Status_Detail__c = 'Unqualified')
                and (not Lead_Status_Detail__c = 'Duplicate')
                and (not Lead_Status_Detail__c = 'Enterprise')
                and (not Lead_Status_Detail__c = 'Out of Business/Bad Data')
                and (not Lead_Status_Detail__c = 'International')
                and (not Lead_Status_Detail__c = 'Competitor/Suspicious')
                and (not Lead_Status_Detail__c = 'BD/Investor')
           """
