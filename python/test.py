import unittest
import logging

logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.DEBUG)
logger = logging.getLogger()

class user_status_test(unittest.TestCase):

    @classmethod

    def complete_movies(self):
        """
        Test for movie with ID 5: correct Genre
        """
        query = """
        MATCH (m:Movie) RETURN COUNT(1)
        """
        logger.info("running the query:%s" % query)
        cur = self.db_con.con.cursor()
        cur.execute(query)
        res = cur.fetchone()[0]

        expected_result = 0
        try:
            self.assertEqual(res, expected_result)
        except AssertionError as e:
            logger.debug(e)
            logger.debug("duplicated record:")
            query = """
                SELECT user_id, cw, COUNT(1)
                FROM rl.user_status_report_weekly
                GROUP BY user_id, cw
                HAVING COUNT(1) > 1
                LIMIT 1;
            """
            cur = self.db_con.con.cursor()
            cur.execute(query)
            res = cur.fetchone()[0]
            logger.debug(res)
            raise


    def test_no_duplicates_month(self):
        """
        no user should have more than one status per period
        """
        query = """
            SELECT COUNT(1)
            FROM
            (
                SELECT user_id, cm, COUNT(1)
                FROM rl.user_status_report_monthly
                GROUP BY user_id, cm
                HAVING COUNT(1) > 1
            ) AS result;
        """
        logger.info("running the query:%s" % query)
        cur = self.db_con.con.cursor()
        cur.execute(query)
        res = cur.fetchone()[0]

        expected_result = 0
        try:
            self.assertEqual(res, expected_result)
        except AssertionError as e:
            logger.debug(e)
            logger.debug("duplicated record:")
            query = """
                SELECT user_id, cm, COUNT(1)
                FROM rl.user_status_report_monthly
                GROUP BY user_id, cm
                HAVING COUNT(1) > 1
                LIMIT 1;
            """
            cur = self.db_con.con.cursor()
            cur.execute(query)
            res = cur.fetchone()[0]
            logger.debug(res)
            raise

    def test_no_status_week(self):
        """
        no user should have null or not expected status
        """
        query = """
            SELECT COUNT(1)
            FROM
            (
                SELECT user_id, COUNT(1)
                FROM rl.user_status_report_weekly
                WHERE status IS NULL OR status NOT IN ('active','cancelled','reactivated')
                GROUP BY user_id
            ) AS result;
        """
        logger.info("running the query:%s" % query)
        cur = self.db_con.con.cursor()
        cur.execute(query)
        res = cur.fetchone()[0]

        expected_result = 0
        try:
            self.assertEqual(res, expected_result)
        except AssertionError as e:
            logger.debug(e)
            logger.debug("duplicated record:")
            query = """
                SELECT user_id
                FROM rl.user_status_report_weekly
                WHERE status IS NULL OR status NOT IN ('active','cancelled','reactivated');
            """
            cur = self.db_con.con.cursor()
            cur.execute(query)
            res = cur.fetchone()[0]
            logger.debug(res)
            raise

    def test_no_status_month(self):
        """
        no user should have null or not expected status
        """
        query = """
              SELECT COUNT(1)
              FROM
              (
                  SELECT user_id, COUNT(1)
                  FROM rl.user_status_report_monthly
                  WHERE status IS NULL OR status NOT IN ('active','cancelled','reactivated')
                  GROUP BY user_id
              ) AS result;
          """
        logger.info("running the query:%s" % query)
        cur = self.db_con.con.cursor()
        cur.execute(query)
        res = cur.fetchone()[0]

        expected_result = 0
        try:
            self.assertEqual(res, expected_result)
        except AssertionError as e:
            logger.debug(e)
            logger.debug("duplicated record:")
            query = """
                  SELECT user_id
                  FROM rl.user_status_report_monthly
                  WHERE status IS NULL OR status NOT IN ('active','cancelled','reactivated');
              """
            cur = self.db_con.con.cursor()
            cur.execute(query)
            res = cur.fetchone()[0]
            logger.debug(res)
            raise