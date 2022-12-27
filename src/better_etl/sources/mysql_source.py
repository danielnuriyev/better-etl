import logging
import os
import psutil
import time

import mysql.connector
import pandas as pd

from better_etl.caches import NoneCache, Cache
from better_etl.sources.source import Source
from better_etl.utils.decorators import retry

_logger = logging.getLogger(__name__)

class MySQLSource(Source):

    # TODO: add pause, restart methods

    def __init__(self,
                 port=3306,
                 table=None,
                 unique_keys=None,
                 unique_keys_min_values=None,
                 columns="*",
                 limit=1000000,
                 sleep=0,
                 max_sleep = 15 * 60,
                 schema=None,
                 logger=_logger,
                 cache=None,
                 **kwargs):

        """
        :param table:
        :param columns:
        :param kwargs: param names come from here: https://dev.mysql.com/doc/connector-python/en/connector-python-connectargs.html
        """

        self.logger=logger
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
        self.schema = schema
        self.cache = NoneCache() if cache is None else cache
        self._con = None
        self._cache_key = f"{self.host}:{self.port}/{self.database}/{self.table}"

        self.logger.info("MySQLSource.__init__ done")

    def _connect(self):
        if not self._con:
            self._con = mysql.connector.connect(host=self.host, port=self.port, user=self.user,
                                               password=self.password,
                                               database=self.database)

        self.logger.info("MySQLSource._connect done")
        return self._con

    @retry
    def close(self):
        self.logger.info(f"Source {time.time()} CLOSING")
        if self._con:
            self._con.close()
        self.logger.info("MySQLSource.close done")

    def get_columns(self):
        try:
            cur = self._connect().cursor(dictionary=True)
            cur.execute(f"SHOW columns FROM {self.database}.{self.table}")
            self.logger.info("MySQLSource.get_columns done")
            return cur.fetchall()
        except Exception as e:
            self.logger.error(f"Failed: {e}")
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
        self.logger.info("MySQLSource.primary_keys done")
        return self.unique_keys

    def get_last_keys(self):
        self.logger.info("MySQLSource.get_last_keys done")
        return self.cache.get(self._cache_key)

    def next_batch(self) -> dict:

        self.logger.info(f"Source {time.time()} START")

        select = f"SELECT {self.columns} FROM {self.database}.{self.table}"
        keys = self.primary_keys()
        cur = self._connect().cursor(dictionary=True)

        next = True
        retry = 0

        previous_keys = self.get_last_keys()
        self.logger.info(f"previous_keys: {previous_keys}")

        while next:
            if keys:
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
                self.logger.warn("No unique keys")
                where = ""
                order_by = ""

            if self.limit:
                limit = f"LIMIT {self.limit}"

            query = f"{select} {where} {order_by} {limit}"
            self.logger.info(query)

            retry = 1
            while retry:
                try:
                    cur.execute(query)
                    rows = cur.fetchall()
                    retry = 0
                except BaseException as e: # TODO: which exceptions indicate the need to retry
                    self.logger.error(e)
                    self.logger.info(f"Retrying in {retry} seconds")
                    time.sleep(retry)
                    retry += 1

            self.logger.info(f"Fetched {len(rows)} rows")

            if len(rows) > 0:
                if self.schema:
                    df = pd.DataFrame(rows, schema=self.schema)
                else:
                    df = pd.DataFrame(rows)

                if keys:
                    last_row = rows[-1]

                    last_keys = {}
                    for key in keys:
                        last_keys[key] = last_row[key]

                    # previous_keys = self.get_last_keys()
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
                            self.logger.info(f"put last keys: {last_keys}")
                            # self.cache.put(self._cache_key, last_keys)
                            previous_keys = last_keys
                        else:
                            self.logger.warn("Exiting: unique keys in this batch are not greater than in the previous batch")
                            next = False
                    else:
                        tmp = 2
                        self.logger.info(f"put last keys: {last_keys}")
                        # self.cache.put(self._cache_key, last_keys)
                        previous_keys = last_keys

                pid = os.getpid()
                process = psutil.Process(pid)
                memory = process.memory_info().rss
                self.logger.info(f'Memory: {"{:,}".format(memory)}')

                yield {
                    "data": df,
                    "metadata": {
                        "type": "data",
                        "format": "pandas.dataframe",
                        "last_keys": last_keys,
                        "cache_key": self._cache_key
                    }
                }

            else:
                next = False

        cur.close()

        self.logger.info("Finished")
