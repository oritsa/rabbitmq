# console_subscriber.py
import pika
import pyodbc
import os
import sys
from pathlib import Path
import yaml
from datetime import datetime


class Util:
    @staticmethod
    def get_root() -> Path:
        return Path(__file__).parent.parent

    @staticmethod
    def get_exe_root() -> Path:
        return Path(os.path.dirname(sys.executable)).parent

    @staticmethod
    def get_config_location(custom_config_name):
        default_config = "config.yaml"
        config = (
            default_config
            if custom_config_name is None
            else f"{custom_config_name}.yaml")
        return f"{Util.get_config_path()}\\{config}"

    @staticmethod
    def get_config(config_name=None):
        with open(Util.get_config_location(config_name), 'r') as config_file:
            config = yaml.load(config_file, Loader=yaml.BaseLoader)
        return config

    @staticmethod
    def get_config_path():
        if getattr(sys, 'frozen', False):
            return f"{Util.get_exe_root().__str__()}\\config"
        elif __file__:
            return f"{Util.get_root().__str__()}\\config"


class SQLHandler:
    def __init__(self, custom_config=None, mapping='database'):
        config = Util.get_config(custom_config)

        self.__driver = f"{{{config[mapping]['driver']}}}"
        self.__host = config[mapping]["host"]
        self.__database = config[mapping]["database"]
        self.__autocommit = config[mapping]["autocommit"]
        self.__uid = config[mapping].get("uid", None)
        self.__pwd = config[mapping].get("pwd", None)

        if not self.__uid or not self.__pwd:
            self.__connection_str = (
                f"DRIVER={self.__driver};"
                f"SERVER={self.__host};"
                f"DATABASE={self.__database};"
                f"Trusted_Connection=yes")
        else:
            self.__connection_str = (
                f"DRIVER={self.__driver};"
                f"SERVER={self.__host};"
                f"DATABASE={self.__database};"
                f"UID={self.__uid};"
                f"PWD={self.__pwd}")

        self.__connection_obj = pyodbc.connect(self.__connection_str)
        self.__connection_obj.autocommit = self.__autocommit
        self.__connection_obj.timeout = 0
        self.__cursor = self.__connection_obj.cursor()

        print(f"Connected To: {self.__host}")
        print(f"Database: {self.__database}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self.__autocommit is False:
                self.__connection_obj.commit()
        except pyodbc.Error:
            print("Failed To Commit Changes To Database")

        try:
            self.__cursor.close()
            self.__connection_obj.close()
        except pyodbc.Error:
            print("Failed To Close Database Connection")

    @property
    def server(self):
        return self.__host

    @property
    def database(self):
        return self.__database

    @property
    def connection(self):
        return self.__connection_obj

    @property
    def cursor(self):
        return self.__cursor

    def close(self):
        self.__exit__(None, None, None)

    def fetchone(self, query, params=None):
        self.__cursor.execute(query, params or ())
        return self.__cursor.fetchone()

    def fetchall(self, query, params=None):
        self.__cursor.execute(query, params or ())
        return self.__cursor.fetchall()

    def execute(self, query, params=None):
        res = self.__cursor.execute(query, params or ())
        return res.rowcount


class Subscriber:
    def __init__(self, queueName, bindingKey, config):
        self.queueName = queueName
        self.bindingKey = bindingKey
        self.config = config
        self.connection = self._create_connection()

    def __del__(self):
        self.connection.close()

    def _create_connection(self):
        parameters = pika.ConnectionParameters(self.config['host'], self.config['port'],
                                               '/', pika.PlainCredentials('user', 'password'))
        return pika.BlockingConnection(parameters)

    def on_message_callback(self, channel, method, properties, body):
        print(f"Received new message: {body.decode('UTF-8')}")

        query = """
                IF OBJECT_ID(N'dbo.TIMES', N'U') IS NULL
                begin
                    CREATE TABLE [dbo].[TIMES](
                        [received_timestamp] datetime 
                        ) 
                end
                
                INSERT INTO TIMES VALUES(?)"""

        date = datetime.strptime(body.decode('UTF-8'), "%Y-%d-%m %H:%M:%S")

        with SQLHandler() as conn:
            conn.execute(query, date)

        print("DB was updated successfully")
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def setup(self):
        channel = self.connection.channel()
        channel.exchange_declare(exchange=self.config['exchange'], exchange_type='topic')

        # This method creates or checks a queue
        channel.queue_declare(queue=self.queueName)

        # Binds the queue to the specified exchang
        channel.queue_bind(queue=self.queueName, exchange=self.config['exchange'], routing_key=self.bindingKey)
        channel.basic_consume(queue=self.queueName, on_message_callback=self.on_message_callback)
        print(f" [*] Waiting for data for {self.queueName}. To exit press CTRL+C")
        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.stop_consuming()


config = {'host': 'localhost', 'port': 5672, 'exchange': 'topic_exchange'}
subscriber = Subscriber("db_writes_queue", "timestamp", config)
subscriber.setup()
