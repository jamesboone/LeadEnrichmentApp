import logging
logger = logging.getLogger(__name__)

__db__ = 'redshift'


query = """rollback;
        select acct_description, campaign_category, campaign_name, city, comp_level, dm_reached_date,
        date_lost, date_meeting_held, date_meeting_scheduled, description, id, lead_source, merchant_name,
        mobile_phone, opp_name, owner_id, owner_role, acct_phone, postal_code, reason_lost, rep_name, state,
        street, store_description, yelp_page, contact_id
        from rekindled_opp_1
        order by date_lost desc"""
