## Описание
Проект реализует потоковый ETL-пайплайн для обработки заказов из системы доставки еды с построением аналитических витрин. Система поддерживает идемпотентную обработку сообщений из Kafka, обогащение данных из Valkey (Redis) и построение хранилища данных по методологии Data Vault.

**Бизнес-задача:** «тегирование гостей» — формирование счётчиков заказов по блюдам и категориям для сегментации пользователей и персонализированных рекомендаций.

## Архитектура решения
Проект построен по методологии Data Vault с тремя слоями данных:

- **STG** — Staging-слой для хранения сырых событий заказов
- **DDS** — Detail Data Store, детальный слой по модели Data Vault (хабы, линки, сателлиты)
- **CDM** — Common Data Marts, слой аналитических витрин

## Структура проекта
```
.
├── stg_service/                    # Сервис заполнения STG-слоя
├── dds_service/                    # Сервис заполнения DDS-слоя
├── cdm_service/                    # Сервис заполнения CDM-витрин
├── sql/                            # Создание архитектуры DWH          
├── docker-compose.yml              # Локальный запуск инфраструктуры
├── Diagram.png                     # ER-диаграмма DDS-слоя
└── Schema.png                      # Схема работы проекта
```

## Источники данных

### Kafka — Поток заказов
Топик входных сообщений содержит события заказов в формате JSON:
| Поле | Тип | Описание |
|------|-----|----------|
| `object_id` | `STRING` | Идентификатор заказа |
| `object_type` | `STRING` | Тип объекта (order) |
| `payload` | `JSON` | Данные заказа |
| `sent_dttm` | `TIMESTAMP` | Время отправки сообщения |

**Поля payload:**
| Поле | Описание |
|------|----------|
| `id` | Идентификатор заказа |
| `date` | Дата заказа |
| `cost` | Полная стоимость заказа |
| `payment` | Сумма оплаты |
| `final_status` | Статус заказа (CLOSED и др.) |
| `user_id` | ID пользователя |
| `restaurant_id` | ID ресторана |
| `products` | Массив блюд с id, price, quantity |

### Valkey (Redis) — Словарные данные
| Ключ | Описание |
|------|----------|
| `user:{user_id}` | Пользователь: `name` (ФИО), `login` |
| `restaurant:{restaurant_id}` | Ресторан: `name`, `menu` (массив блюд с `name` и `category`) |

## Слои данных

### STG-слой
Слой для хранения сырых событий заказов из Kafka.

#### Таблица stg.order_events
| Поле | Тип | Описание |
|------|-----|----------|
| `id` | `SERIAL` | Идентификатор записи (PK) |
| `object_id` | `VARCHAR` | Идентификатор объекта в событии (UNIQUE, NOT NULL) |
| `object_type` | `VARCHAR` | Тип объекта (NOT NULL) |
| `payload` | `JSON` | Событие в формате JSON (NOT NULL) |
| `sent_dttm` | `TIMESTAMP` | Дата и время отправки сообщения (NOT NULL) |

**Ограничения:**
- `PRIMARY KEY` на `id`
- `UNIQUE` на `object_id`
- Все поля `NOT NULL`

### DDS-слой
Детальный слой хранилища, спроектированный по методологии Data Vault.

![ER-диаграмма DDS-слоя](https://github.com/stmax22/ETL_for_cloud_microservices/blob/f2c385d511b25055be5ef27d643c2711818e5375/Diagram.png)

#### Хабы (Hubs)
| Таблица | Бизнес-сущность | Поля |
|---------|----------------|------|
| `h_user` | Пользователь | `h_user_pk`, `user_id`, `load_dt`, `load_src` |
| `h_product` | Продукт (блюдо) | `h_product_pk`, `product_id`, `load_dt`, `load_src` |
| `h_category` | Категория блюда | `h_category_pk`, `category_name`, `load_dt`, `load_src` |
| `h_restaurant` | Ресторан | `h_restaurant_pk`, `restaurant_id`, `load_dt`, `load_src` |
| `h_order` | Заказ | `h_order_pk`, `order_id`, `order_dt`, `load_dt`, `load_src` |

**Особенности:**
- `h_object_pk` — UUID, генерируется на основе `object_id`
- `load_dt` — `TIMESTAMP` без таймзоны, по UTC
- `load_src` — источник данных (например, `orders-system-kafka`)

#### Линки (Links)
| Таблица | Связь | Поля |
|---------|-------|------|
| `l_order_user` | Заказ — Пользователь | `hk_order_user_pk`, `h_order_pk`, `h_user_pk`, `load_dt`, `load_src` |
| `l_order_product` | Заказ — Продукт | `hk_order_product_pk`, `h_order_pk`, `h_product_pk`, `load_dt`, `load_src` |
| `l_product_category` | Продукт — Категория | `hk_product_category_pk`, `h_product_pk`, `h_category_pk`, `load_dt`, `load_src` |
| `l_product_restaurant` | Продукт — Ресторан | `hk_product_restaurant_pk`, `h_product_pk`, `h_restaurant_pk`, `load_dt`, `load_src` |

**Особенности:**
- `hk_link_pk` — UUID, хеш-ключ записи
- Внешние ключи на таблицы-хабы

#### Сателлиты (Satellites)
| Таблица | Родитель | Атрибуты |
|---------|----------|----------|
| `s_user_names` | `h_user` | `username`, `userlogin` |
| `s_product_names` | `h_product` | `name` |
| `s_restaurant_names` | `h_restaurant` | `name` |
| `s_order_cost` | `h_order` | `cost`, `payment` |
| `s_order_status` | `h_order` | `status` |

**Особенности:**
- Составной первичный ключ: `(h_hubname_pk, load_dt)`
- `hk_tablename_hashdiff` — UUID, хеш-ключ для определения уникальности строки

### CDM-слой
Слой аналитических витрин для бизнес-аналитики.

#### Витрина cdm.user_product_counters
Счётчик заказов по блюдам для каждого пользователя.

| Поле | Тип | Описание |
|------|-----|----------|
| `id` | `SERIAL` | Идентификатор записи (PK) |
| `user_id` | `VARCHAR` | Идентификатор пользователя |
| `product_id` | `VARCHAR` | Идентификатор продукта |
| `product_name` | `VARCHAR` | Наименование продукта |
| `order_cnt` | `INTEGER` | Счётчик заказов |

**Ограничения:**
- `PRIMARY KEY` на `id`
- `order_cnt >= 0`
- Уникальный индекс на `(user_id, product_id)`

#### Витрина cdm.user_category_counters
Счётчик заказов по категориям блюд для каждого пользователя.

| Поле | Тип | Описание |
|------|-----|----------|
| `id` | `SERIAL` | Идентификатор записи (PK) |
| `user_id` | `VARCHAR` | Идентификатор пользователя |
| `category_id` | `VARCHAR` | Идентификатор категории |
| `category_name` | `VARCHAR` | Наименование категории |
| `order_cnt` | `INTEGER` | Счётчик заказов |

## Поток обработки сообщений
![Схема](https://github.com/stmax22/ETL_for_cloud_microservices/blob/b15fb9856d9a62d76761a997f9cae81380ab2c25/Schema.png)

### STG-Service
1. Чтение сообщения из Kafka
2. Upsert в stg.order_events (идемпотентность по object_id)
3. Извлечение user_id, restaurant_id, product_id из сообщения
4. Обогащение из Valkey:
   - user: name, login
   - restaurant: name, menu (с категориями)
5. Формирование обогащённого сообщения
6. Отправка в Kafka

### DDS-Service
1. Чтение обогащённого сообщения из Kafka
2. Загрузка хабов (h_user, h_order, h_restaurant, h_product, h_category)
3. Загрузка линков (l_order_user, l_order_product, l_product_category, l_product_restaurant)
4. Загрузка сателлитов (s_order_cost, s_order_status, s_user_names, s_product_names, s_restaurant_names)

### CDM-Service
1. Чтение обогащённого сообщения из Kafka
2. Инкрементальное обновление витрин:
   - user_product_counters
   - user_category_counters
3. Идемпотентность через UPSERT по (user_id, product_id) и (user_id, category_id)