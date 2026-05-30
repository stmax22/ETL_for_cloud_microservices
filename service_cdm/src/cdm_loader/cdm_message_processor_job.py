from datetime import datetime
from logging import Logger

from cdm_loader.repository import CdmRepository
from lib.kafka_connect import KafkaConsumer


class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 batch_size: int,
                 logger: Logger) -> None:

        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._batch_size = batch_size
        self._logger = logger

    def run(self) -> None:
        # Пишем в лог, что джоба был запущена.
        self._logger.info(f'{datetime.utcnow()}: START')

        for _ in range(self._batch_size):
            message = self._consumer.consume(3.0)

            if message is None:
                # Все сообщения обработаны.
                break

            try:
                # Данные для работы:
                user_id = message['user_id']
                products = message['products']
                categories = message['categories']

                # Названия витрин.
                data_marts_name = (
                    'user_product_counters',
                    'user_category_counters'
                )

                # 1. Заполняем витрину user_product_counters.
                for product in products:
                    self._cdm_repository.insert_tables(
                        data_marts_name[0],
                        {'user_id': user_id,
                         'product_id': product['product_id'],
                         'product_name': product['product_name'],
                         'order_cnt': product['order_cnt']}
                    )

                self._logger.info(f'Витрина "{data_marts_name[0]}" заполнена данными!')

                # 2. Заполняем витрину user_category_counters.
                for category in categories:
                    self._cdm_repository.insert_tables(
                        data_marts_name[1],
                        {'user_id': user_id,
                         'category_id': category['category_id'],
                         'category_name': category['category_name'],
                         'order_cnt': category['order_cnt']}
                    )

                self._logger.info(f'Витрина "{data_marts_name[1]}" заполнена данными!')

            except Exception as e:
                # Логируем ошибку.
                self._logger.error(f"Error processing message: {e}")

        # Пишем в лог, что джоба успешно завершена.
        self._logger.info(f'{datetime.utcnow()}: FINISH')
