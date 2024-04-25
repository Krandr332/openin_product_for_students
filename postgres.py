import psycopg2
from dotenv import load_dotenv
import os


months = {
    'январь': 1, 'февраль': 2, 'март': 3, 'апрель': 4, 'май': 5, 'июнь': 6,
    'июль': 7, 'август': 8, 'сентябрь': 9, 'октябрь': 10, 'ноябрь': 11, 'декабрь': 12
}


def check_purchase_length(subject, first_name, last_name, classyear, type, potok, month):
    load_dotenv()

    db_params = {
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT')
    }
    if classyear == "ОГЭ":
        classyear = "9 класс"
    elif classyear == "ЕГЭ":
        classyear = '11 класс'

    month_number = months.get(month.lower(), None)
    if month_number is None:
        return None

    sql_query = f"""
        SELECT DISTINCT
            mp.id   
        FROM 
            mastergroup_product mp 
        -- join Тип продукта
        LEFT JOIN mastergroup_producttype mp2 ON mp2.id = mp.product_type_id
        -- join Тип системных направлений
        LEFT JOIN core_coursetype cc ON cc.id = mp2.course_type_id
        -- join Системные направления
        LEFT JOIN core_syscoursetype cs ON cs.id = cc.sys_course_type_id
        -- join Продукт ID | Урок ID | Уровень ID
        LEFT JOIN modules_productlessonlevel llmm ON llmm.product_id = mp.id
        -- join Уровень
        LEFT JOIN modules_level ml ON ml.id = llmm.level_id
        -- join Урок
        LEFT JOIN lesson_lesson ll ON ll.id = llmm.lesson_id
        -- join Предметы
        LEFT JOIN core_classtype cc2 ON ll.class_type_id = cc2.id
        -- join Год обучения
        LEFT JOIN core_classyear cc3 ON ll.class_year_id = cc3.id
        -- join Преподаватели
        LEFT JOIN core_user cu ON ll.teacher_id = cu.id
        -- Потоки - Продукт
        LEFT JOIN mastergroup_productstreamproduct mp3 ON mp3.product_id = mp.id
        -- Потоки
        LEFT JOIN mastergroup_productstream mp4 ON mp4.id = mp3.stream_id
        -- План
        LEFT JOIN core_rateplan cr ON cr.id = mp.rate_plan_id 
        WHERE 
            cs.id = 22
            AND cc2.title ='{subject}'
            AND cc3.title = '{classyear}'
            AND cu.first_name = '{first_name}'
            AND cu.last_name = '{last_name}'
            AND cr.title = '{type}'
            AND (mp4.title LIKE '%{potok} поток%' OR mp4.title LIKE '{potok} поток %' OR mp4.title LIKE '% {potok} поток') -- Условие для поиска строк, содержащих слово "1 поток"
            AND EXTRACT(MONTH FROM mp.start_date) = {month_number}
            AND mp.is_active IS TRUE;
    """

    conn = psycopg2.connect(**db_params)
    try:
        with conn.cursor() as cur:
            cur.execute(sql_query)
            result = cur.fetchone()

            if result:
                return result[0] # Возвращаем значение первого столбца (mp.id)
            else:
                return None
    finally:
        conn.close()
