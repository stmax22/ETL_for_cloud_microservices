import json

from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from lib.redis import RedisClient
from stg_loader.repository import StgRepository


class StgMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 redis: RedisClient,
                 stg_repository: StgRepository,
                 batch_size: int,
                 logger: Logger) -> None:

        self._consumer = consumer
        self._producer = producer
        self._redis = redis
        self._stg_repository = stg_repository
        self._batch_size = batch_size
        self._logger = logger

    # Метод, который будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f'{datetime.utcnow()}: START')

        for _ in range(self._batch_size):
            message = self._consumer.consume(3.0)

            if message is None:
                # Все сообщения обработаны.
                break

            try:
                object_id = message['object_id']
                object_type = message['object_type']
                payload = message['payload']
                sent_dttm_str = message['sent_dttm']

                # Преобразуем payload в строку JSON.
                payload_str = json.dumps(payload, ensure_ascii=False)

                # Конвертируем строку даты в datetime.
                sent_dttm = datetime.strptime(sent_dttm_str, '%Y-%m-%d %H:%M:%S')

                # Сохраняем в таблицу через репозиторий.
                self._stg_repository.order_events_insert(
                    object_id,
                    object_type,
                    sent_dttm,
                    payload_str
                )

                # Получаем user_id и restaurant_id.
                user_id = payload['user']['id']
                restaurant_id = payload['restaurant']['id']

                # Получаем полную информацию из Redis.
                user_info = self._redis.get(user_id)
                restaurant_info = self._redis.get(restaurant_id)

                # Получаем список товаров из заказа.
                order_items = payload['order_items']

                # Получаем меню ресторана из Redis.
                restaurant_menu = restaurant_info['menu']

                # Создаем словарь для быстрого поиска категории по id товара.
                menu_dict = {item['_id']: item['category'] for item in restaurant_menu}

                # Обогащаем каждый товар, добавляя поле "category".
                for item in order_items:
                    item_id = item['id']
                    # Получаем категорию по id товара из меню.
                    category = menu_dict[item_id]
                    # Добавляем категорию в товар.
                    item['category'] = category

                # Формируем payload для выходного сообщения.
                payload_end = {
                    'id': object_id,
                    'date': payload['date'],
                    'cost': payload['cost'],
                    'payment': payload['payment'],
                    'status': payload['final_status'],
                    'restaurant': {
                        'id': restaurant_info['_id'],
                        'name': restaurant_info['name']},
                    'user': {
                        'id': user_info['_id'],
                        'name': user_info['name'],
                        'login': user_info['login']},
                    'products': order_items
                }

                # Формируем выходное сообщение.
                output_message = {
                    'object_id': object_id,
                    'object_type': object_type,
                    'payload': payload_end
                }
                self._logger.info('Выходное сообщение подготовлено!')

                # Отправляем в Kafka.
                self._producer.produce(output_message)
                self._logger.info('Выходное сообщение отправлено в Kafka!')

            except Exception as e:
                # Логируем ошибку.
                self._logger.error(f'Ошибка при обработке: {e}')

        # Пишем в лог, что джоба успешно завершена.
        self._logger.info(f'{datetime.utcnow()}: FINISH')
