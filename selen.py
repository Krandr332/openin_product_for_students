# -*- coding: cp1251 -*-
import time

from selenium import webdriver
from selenium.common import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os

from config import username, password


def sel():
    # Получаем абсолютный путь к файлу
    current_directory = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_directory, 'add_products.csv')

    url = '**********/add-product-to-user/'

    # Данные для аутентификации

    # Настройки Chrome
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Включение headless режима

    # Запуск браузера
    driver = webdriver.Chrome(options=chrome_options)  # Укажите путь к драйверу, если он у вас в другом месте
    driver.get(url)

    # Авторизация
    username_field = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, "username")))
    password_field = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, "password")))
    login_button = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.XPATH, "//input[@type='submit']")))

    username_field.send_keys(username)
    password_field.send_keys(password)
    login_button.click()

    # Ожидание загрузки страницы с формой для загрузки файла
    file_input = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//input[@type='file']")))

    # Загрузка файла
    file_input.send_keys(file_path)

    # Отправка формы
    submit_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable(
        (By.XPATH, "//button[contains(@class, 'mt-2') and contains(@class, 'send_button')]")))
    submit_button.click()
    time.sleep(5)

    driver.quit()
