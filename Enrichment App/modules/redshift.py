import logging
import json
import time
import psycopg2
logger = logging.getLogger(__name__)


class Redshift(object):
    def __init__(self):
        logger.debug('redshift db init')
        self.conn = None
        self.cursor = None

    def query(self, query_string):
        logger.debug('query redshift')
        try:
            with open('config.json', 'r') as f:
                config = json.load(f)
            host = config.get("redshift").get("host")
            dbname = config.get("redshift").get("dbname")
            user = config.get("redshift").get("user")
            password = config.get("redshift").get("password")
            port = config.get("redshift").get("port")

            conn_string = """
                host=%s
                dbname=%s
                user=%s
                password=%s
                port=%s
            """ % (host, dbname, user, password, port)

            self.conn = psycopg2.connect(conn_string)
            self.cursor = self.conn.cursor()
            self.cursor.execute(query_string)
            results = self.cursor.fetchall()
            logger.info('Rekindles to process: %s' % (len(results),))
            self.close()
            return results
        except Exception, psycopg2.InternalError:
            logger.error('sleeing for 2 min: %s' % (psycopg2.InternalError, ))
            time.sleep(120)
            return self.query(query_string)

    def close(self):
        logger.debug('redshift close db conn')
        if self.cursor or self.conn:
            self.cursor.close()
            self.conn.close()
