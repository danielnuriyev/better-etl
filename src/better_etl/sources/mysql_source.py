import logging
import os
import psutil
import time

import mysql.connector
import pandas as pd

from better_etl.sources.source import Source

logger = logging.getLogger(__name__)

class MySQLSource(Source):

    # TODO: add pause, restart methods

    def __init__(self,
                 port=3306,
                 table=None,
                 unique_keys=None,
                 unique_keys_min_values=None,
                 columns="*",
                 limit=100000,
                 sleep=0,
                 max_sleep = 15 * 60,
                 stream=False,
                 schema=None,
                 **kwargs):

        """
        :param table:
        :param columns:
        :param kwargs: param names come from here: https://dev.mysql.com/doc/connector-python/en/connector-python-connectargs.html
        """

        logger.info(f"Source {time.time()}")
        self._created = time.time()

        self.__dict__.update(kwargs)
        # TODO: verify correct values for all fields
        self.port = port
        self.table = table
        self.unique_keys = unique_keys
        self.columns = ",".join(map(lambda item: item.strip(), columns.split(",")))
        self.limit = limit
        self.sleep = sleep
        self.max_sleep = max_sleep
        self.stream = stream
        self.schema = schema
        self._con = None
        self._cache_key = f"{self.host}:{self.port}/{self.database}/{self.table}"

    def _connect(self):
        if not self._con:
            self._con = mysql.connector.connect(host=self.host, port=self.port, user=self.user,
                                               password=self.password,
                                               database=self.database)
        return self._con

    def close(self):
        logger.info(f"Source {time.time()} CLOSING")
        if self._con:
            self._con.close()

    def get_columns(self):
        try:
            cur = self._connect().cursor(dictionary=True)
            cur.execute(f"SHOW columns FROM {self.database}.{self.table}")
            return cur.fetchall()
        except Exception as e:
            logger.error(f"Failed: {e}")
            raise e
        finally:
            cur.close()

    def primary_keys(self):
        if not self.unique_keys:
            keys = []
            columns = self.get_columns()
            for column in columns:
                if column["Key"] == "PRI":
                    keys.append(column["Field"])
            self.unique_keys = keys
        return self.unique_keys

    def get_last_keys(self):
        logger.info(f"Cache: {self.cache}")
        return self.cache.get(self._cache_key)

    def next_batch(self):

        logger.info(f"Source {time.time()} START")

        select = f"SELECT {self.columns} FROM {self.database}.{self.table}"
        keys = self.primary_keys()
        cur = self._connect().cursor(dictionary=True)

        next = True
        retry = 0
        while next:

            if keys:

                previous_keys = self.get_last_keys()
                logger.info(f"previous_keys: {previous_keys}")

                if previous_keys:
                    where = "WHERE"
                    for i in range(len(keys)):
                        if i > 0:
                            where += " AND"
                        key = keys[i]
                        v = previous_keys[key]
                        where += f" {key} > {v}"
                else:
                    where = ""

                order_keys = ",".join(map(lambda item: item.strip(), keys))
                order_by = f"ORDER BY {order_keys}"

            else:
                logger.warn("No unique keys")
                where = ""
                order_by = ""

            if self.limit:
                limit = f"LIMIT {self.limit}"

            query = f"{select} {where} {order_by} {limit}"
            logger.info(query)

            retry = 1
            while retry:
                try:
                    cur.execute(query)
                    rows = cur.fetchall()
                    retry = 0
                except BaseException as e: # TODO: which exceptions indicate the need to retry
                    logger.error(e)
                    logger.info(f"Retrying in {retry} seconds")
                    time.sleep(retry)
                    retry += 1

            logger.info(f"Fetched {len(rows)} rows")

            if len(rows) > 0:
                if self.schema:
                    df = pd.DataFrame(rows, schema=self.schema)
                else:
                    df = pd.DataFrame(rows)

                #

                if keys:
                    last_row = rows[-1]

                    last_keys = {}
                    for key in keys:
                        last_keys[key] = last_row[key]

                    previous_keys = self.get_last_keys()
                    tmp = 0
                    if previous_keys:
                        tmp = 1
                        found = False
                        for key in last_keys:
                            previous_key = previous_keys[key]
                            if key != previous_key:
                                found = True
                                break
                        if found:
                            logger.info(f"put last keys: {last_keys}")
                            self.cache.put(self._cache_key, last_keys)
                        else:
                            logger.warn("Exiting: unique keys in this batch are not greater than in the previous batch")
                            next = False
                    else:
                        tmp = 2
                        logger.info(f"put last keys: {last_keys}")
                        self.cache.put(self._cache_key, last_keys)

                #

                yield {
                    "data": df,
                    "metadata": {
                        "type": "data",
                        "format": "pandas.dataframe",
                        "debug": f"{id(self.cache._cache)}",
                        "last_keys": last_keys,
                    }
                }

            else:
                next = False

            pid = os.getpid()
            process = psutil.Process(pid)
            memory = process.memory_info().rss
            logger.info(f'Memory: {"{:,}".format(memory)}')

            if not self.stream and next and self.sleep:
                time.sleep(self.sleep)
            """
            elif self.stream:
                if next:
                    retry = 0
                    if self.sleep:
                        time.sleep(self.sleep)
                else:
                    sleep = self.sleep + retry ^ 2
                    if sleep > self.max_sleep:
                        sleep = self.max_sleep
                    else:
                        retry += 1
                    if sleep:
                        logger.info(f"Sleeping for {sleep} seconds")
                        time.sleep(sleep)
                """

            if self.stream:
                next = False

        logger.info("Finished")
