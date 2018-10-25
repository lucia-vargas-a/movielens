import unittest
import logging

logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.DEBUG)
logger = logging.getLogger()

class user_status_test(unittest.TestCase):

    @classmethod

    def complete_movies(self):
        """
        Test that all the movies were loaded into the database
        """
        query = """
        MATCH (m:Movie) RETURN count(1)
        """
        logger.info("running the query:%s" % query)
        cur = self.db_con.con.cursor()
        cur.execute(query)
        res = cur.fetchone()[0]

        expected_result = 27278
        try:
            self.assertEqual(res, expected_result)
        except AssertionError as e:
            logger.debug(e)
            logger.debug("The number of movies is not correct")
            cur = self.db_con.con.cursor()
            cur.execute(query)
            res = cur.fetchone()[0]
            logger.debug(res)
            raise
