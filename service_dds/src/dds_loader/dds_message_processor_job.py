import uuid

from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository import DdsRepository


class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 batch_size: int,
                 logger: Logger) -> None:

        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._batch_size = batch_size
        self._logger = logger

    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f'{datetime.utcnow()}: START')

        for _ in range(self._batch_size):
            message = self._consumer.consume(3.0)

            if message is None:
                # Все сообщения обработаны.
                break

            try:
                # Данные для работы:
                payload = message['payload']
                cost = payload['cost']
                order_dt = payload['date']
                order_id = payload['id']
                user_id = payload['user']['id']
                user_login = payload['user']['login']
                user_name = payload['user']['name']
                payment = payload['payment']
                restaurant_id = payload['restaurant']['id']
                restaurant_name = payload['restaurant']['name']
                status = payload['status']
                load_src = 'orders-system-kafka'

                # Названия хаб-таблиц.
                hub_tables_name = (
                    'h_user',
                    'h_product',
                    'h_category',
                    'h_restaurant',
                    'h_order'
                )

                # Названия линк-таблиц.
                link_tables_name = (
                    'l_order_product',
                    'l_product_restaurant',
                    'l_product_category',
                    'l_order_user'
                )

                # Названия сателлит-таблиц.
                satellite_tables_name = (
                    's_user_names',
                    's_product_names',
                    's_restaurant_names',
                    's_order_cost',
                    's_order_status'
                )

                # 1 Заполняем хаб-таблицы данными.
                # 1.1 Сохраняем данные в хаб-таблицу h_user и получаем h_user_pk.
                h_user_pk = uuid.uuid5(uuid.NAMESPACE_DNS, str(user_id))

                self._dds_repository.insert_tables(
                    hub_tables_name[0],
                    {'h_user_pk': h_user_pk,
                     'user_id': user_id,
                     'load_dt': datetime.utcnow(),
                     'load_src': load_src
                     }
                )

                self._logger.info(f'Таблица "{hub_tables_name[0]}" заполнена!')

                # 1.2 Сохраняем данные в хаб-таблицу h_product и получаем h_product_pk.
                h_product_pk = []

                for item in payload['products']:
                    product_id = item['id']
                    product_pk = uuid.uuid5(uuid.NAMESPACE_DNS, str(product_id))

                    self._dds_repository.insert_tables(
                        hub_tables_name[1],
                        {'h_product_pk': product_pk,
                         'product_id': product_id,
                         'load_dt': datetime.utcnow(),
                         'load_src': load_src
                         }
                    )

                    h_product_pk.append(product_pk)

                self._logger.info(f'Таблица "{hub_tables_name[1]}" заполнена!')

                # 1.3 Сохраняем данные в хаб-таблицу h_category и получаем h_category_pk.
                h_category_pk = []

                for item in payload['products']:
                    category = item['category']
                    category_pk = uuid.uuid5(uuid.NAMESPACE_DNS, str(category))

                    self._dds_repository.insert_tables(
                        hub_tables_name[2],
                        {'h_category_pk': category_pk,
                         'category_name': category,
                         'load_dt': datetime.utcnow(),
                         'load_src': load_src
                         }
                    )

                    h_category_pk.append(category_pk)

                self._logger.info(f'Таблица "{hub_tables_name[2]}" заполнена!')

                # 1.4 Сохраняем данные в хаб-таблицу h_restaurant и получаем h_restaurant_pk.
                h_restaurant_pk = uuid.uuid5(uuid.NAMESPACE_DNS, str(restaurant_id))

                self._dds_repository.insert_tables(
                    hub_tables_name[3],
                    {'h_restaurant_pk': h_restaurant_pk,
                     'restaurant_id': restaurant_id,
                     'load_dt': datetime.utcnow(),
                     'load_src': load_src
                     }
                )

                self._logger.info(f'Таблица "{hub_tables_name[3]}" заполнена!')

                # 1.5 Сохраняем данные в хаб-таблицу h_order и получаем h_order_pk.
                h_order_pk = uuid.uuid5(uuid.NAMESPACE_DNS, str(order_id))

                self._dds_repository.insert_tables(
                    hub_tables_name[4],
                    {'h_order_pk': h_order_pk,
                     'order_id': order_id,
                     'order_dt': order_dt,
                     'load_dt': datetime.utcnow(),
                     'load_src': load_src
                     }
                )

                self._logger.info(f'Таблица "{hub_tables_name[4]}" заполнена!')

                # 2 Заполняем линк-таблицы данными.
                # 2.1 Сохраняем данные в линк-таблицу l_order_product.
                for product_pk in h_product_pk:
                    # Создаем уникальную запись из 2-х pk для генерации UUID.
                    order_product_pk = f'{h_order_pk}_{product_pk}'

                    self._dds_repository.insert_tables(
                        link_tables_name[0],
                        {'hk_order_product_pk': uuid.uuid5(uuid.NAMESPACE_DNS, order_product_pk),
                         'h_order_pk': h_order_pk,
                         'h_product_pk': product_pk,
                         'load_dt': datetime.utcnow(),
                         'load_src': load_src
                         }
                    )

                self._logger.info(f'Таблица "{link_tables_name[0]}" заполнена!')

                # 2.2 Сохраняем данные в линк-таблицу l_product_restaurant.
                for product_pk in h_product_pk:
                    # Создаем уникальную запись из 2-х pk для генерации UUID.
                    product_restaurant_pk = uuid.uuid5(
                        uuid.NAMESPACE_DNS,
                        f'{product_pk}_{h_restaurant_pk}'
                    )

                    self._dds_repository.insert_tables(
                        link_tables_name[1],
                        {'hk_product_restaurant_pk': product_restaurant_pk,
                         'h_product_pk': product_pk,
                         'h_restaurant_pk': h_restaurant_pk,
                         'load_dt': datetime.utcnow(),
                         'load_src': load_src
                         }
                    )

                self._logger.info(f'Таблица "{link_tables_name[1]}" заполнена!')

                # 2.3 Сохраняем данные в линк-таблицу l_product_category.
                for i in range(len(h_product_pk)):
                    # Создаем уникальную запись из 2-х pk для генерации UUID.
                    product_category_pk = f'{h_product_pk[i]}_{h_category_pk[i]}'

                    self._dds_repository.insert_tables(
                        link_tables_name[2],
                        {'hk_product_category_pk': uuid.uuid5(uuid.NAMESPACE_DNS, product_category_pk),
                         'h_product_pk': h_product_pk[i],
                         'h_category_pk': h_category_pk[i],
                         'load_dt': datetime.utcnow(),
                         'load_src': load_src
                         }
                    )

                self._logger.info(f'Таблица "{link_tables_name[2]}" заполнена!')

                # 2.4 Сохраняем данные в линк-таблицу l_order_user.
                # Создаем уникальную запись из 2-х pk для генерации UUID.
                order_user_pk = f'{h_order_pk}_{h_user_pk}'

                self._dds_repository.insert_tables(
                    link_tables_name[3],
                    {'hk_order_user_pk': uuid.uuid5(uuid.NAMESPACE_DNS, order_user_pk),
                     'h_order_pk': h_order_pk,
                     'h_user_pk': h_user_pk,
                     'load_dt': datetime.utcnow(),
                     'load_src': load_src
                     }
                )

                self._logger.info(f'Таблица "{link_tables_name[3]}" заполнена!')

                # 3 Заполняем сателлит-таблицы данными.
                # 3.1 Сохраняем данные в сателлит-таблицу s_user_names.
                load_dt = datetime.utcnow()

                # Создаем уникальную запись из всей строки и генерируем UUID.
                hk_user_names_hashdiff_str = f'{h_user_pk}_{user_name}_{user_login}_{load_dt}_{load_src}'
                hk_user_names_hashdiff = uuid.uuid5(uuid.NAMESPACE_DNS, hk_user_names_hashdiff_str)

                self._dds_repository.insert_satellite_tables(
                    satellite_tables_name[0],
                    {'h_user_pk': h_user_pk,
                     'username': user_name,
                     'userlogin': user_login,
                     'load_dt': load_dt,
                     'load_src': load_src,
                     'hk_user_names_hashdiff': hk_user_names_hashdiff
                     }
                )

                self._logger.info(f'Таблица "{satellite_tables_name[0]}" заполнена!')

                # 3.2 Сохраняем данные в сателлит-таблицу s_product_names.
                for item in payload['products']:
                    product_id = item['id']
                    product_pk = uuid.uuid5(uuid.NAMESPACE_DNS, str(product_id))
                    product_name = item['name']
                    load_dt = datetime.utcnow()

                    # Создаем уникальную запись из всей строки и генерируем UUID.
                    hk_product_names_hashdiff_str = f'{product_pk}_{product_name}_{load_dt}_{load_src}'
                    hk_product_names_hashdiff = uuid.uuid5(uuid.NAMESPACE_DNS, hk_product_names_hashdiff_str)

                    self._dds_repository.insert_satellite_tables(
                        satellite_tables_name[1],
                        {'h_product_pk': product_pk,
                         'name': product_name,
                         'load_dt': load_dt,
                         'load_src': load_src,
                         'hk_product_names_hashdiff': hk_product_names_hashdiff
                         }
                    )

                self._logger.info(f'Таблица "{satellite_tables_name[1]}" заполнена!')

                # 3.3 Сохраняем данные в сателлит-таблицу s_restaurant_names.
                load_dt = datetime.utcnow()

                # Создаем уникальную запись из всей строки и генерируем UUID.
                hk_restaurant_names_hashdiff_str = f'{h_restaurant_pk}_{restaurant_name}_{load_dt}_{load_src}'
                hk_restaurant_names_hashdiff = uuid.uuid5(uuid.NAMESPACE_DNS, hk_restaurant_names_hashdiff_str)

                self._dds_repository.insert_satellite_tables(
                    satellite_tables_name[2],
                    {'h_restaurant_pk': h_restaurant_pk,
                     'name': restaurant_name,
                     'load_dt': load_dt,
                     'load_src': load_src,
                     'hk_restaurant_names_hashdiff': hk_restaurant_names_hashdiff
                     }
                )

                self._logger.info(f'Таблица "{satellite_tables_name[2]}" заполнена!')

                # 3.4 Сохраняем данные в сателлит-таблицу s_order_cost.
                load_dt = datetime.utcnow()

                # Создаем уникальную запись из всей строки и генерируем UUID.
                hk_order_cost_hashdiff_str = f'{h_order_pk}_{cost}_{payment}_{load_dt}_{load_src}'
                hk_order_cost_hashdiff = uuid.uuid5(uuid.NAMESPACE_DNS, hk_order_cost_hashdiff_str)

                self._dds_repository.insert_satellite_tables(
                    satellite_tables_name[3],
                    {'h_order_pk': h_order_pk,
                     'cost': cost,
                     'payment': payment,
                     'load_dt': load_dt,
                     'load_src': load_src,
                     'hk_order_cost_hashdiff': hk_order_cost_hashdiff
                     }
                )

                self._logger.info(f'Таблица "{satellite_tables_name[3]}" заполнена!')

                # 3.5 Сохраняем данные в сателлит-таблицу s_order_status.
                load_dt = datetime.utcnow()

                # Создаем уникальную запись из всей строки и генерируем UUID.
                hk_order_status_hashdiff_str = f'{h_order_pk}_{status}_{load_dt}_{load_src}'
                hk_order_status_hashdiff = uuid.uuid5(uuid.NAMESPACE_DNS, hk_order_status_hashdiff_str)

                self._dds_repository.insert_satellite_tables(
                    satellite_tables_name[4],
                    {'h_order_pk': h_order_pk,
                     'status': status,
                     'load_dt': load_dt,
                     'load_src': load_src,
                     'hk_order_status_hashdiff': hk_order_status_hashdiff
                     }
                )

                self._logger.info(f'Таблица "{satellite_tables_name[4]}" заполнена!')

                # 4. Генерируем сообщение для CDM слоя на основе данных из DDS.
                product_stats = self._dds_repository.get_user_product_stats(h_user_pk)
                self._logger.info(f'Данные по продуктам подготовлены, результат: {product_stats}')

                category_stats = self._dds_repository.get_user_category_stats(h_user_pk)
                self._logger.info(f'Данные по категориям подготовлены, результат: {category_stats}')

                cdm_message = {
                    'user_id': str(h_user_pk),
                    'products': [
                        {'product_id': str(item['product_id']),
                         'product_name': item['product_name'],
                         'order_cnt': item['order_cnt']
                         }
                        for item in product_stats
                    ],
                    'categories': [
                        {'category_id': str(item['category_id']),
                         'category_name': item['category_name'],
                         'order_cnt': item['order_cnt']
                         }
                        for item in category_stats
                    ]
                }
                self._logger.info('Выходное сообщение подготовлено!')

                # 5. Отправляем сообщение в Kafka.
                self._producer.produce(cdm_message)
                self._logger.info('Выходное сообщение отправлено в Kafka!')

            except Exception as e:
                # Логируем ошибку и продолжаем обработку
                self._logger.error(f'Error processing message: {e}')

        # Пишем в лог, что джоба успешно завершена.
        self._logger.info(f'{datetime.utcnow()}: FINISH')
