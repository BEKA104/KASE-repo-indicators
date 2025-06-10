# Загрузка библиотек
from bs4 import BeautifulSoup
import requests
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import time
import re
import glob
import os
from sqlalchemy import create_engine, text

class repo:
    def __init__ (self, name ):
        self.name = name

    def downloading(self):
        str = 'https://kase.kz/ru/indexes-and-indicators/repo/{}'   # шаблон юрл
        url = str.format(self.name)     
 # процесс создания конечных юрл    
        soup = BeautifulSoup(requests.get(url).text, 'lxml')
        table = soup.find('c-table')
        final_data = []
        for tr in table.find_all('tr')[:-1]:  # Выводит текст таблицы каждого индикатора репо
            final_data.append(re.split(r'\s{2,}', tr.text)[0:2])
            df = pd.DataFrame(final_data).T
            new_header = df.iloc[0]
            df = df[1:] 
            df.columns = new_header
            df = df.rename(index={1 : 0})
            # Настройка параметров для Selenium-a
        chromedriver = Service(ChromeDriverManager().install())         # Этот код именно для хрома, если нужно, могу сделать вторую версию для файрфокса
        options = webdriver.ChromeOptions() 
        options.add_experimental_option("detach", True)
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        browser = webdriver.Chrome(service=chromedriver, options=options)
        browser.get(url)
        browser.find_element(By.XPATH, "//button[span[text()='Скачать файл']]").click()  #Скачивает файлы за сегодняшний день
        time.sleep(2)
        return df
    def to_pep_8(self, bob):
        bob = bob.replace(' ', '_')
        bob = bob.replace(',', '')
        return bob

    def preprocessing(self, df):
        download_dir = r"C:\Users\user\Downloads"                         # !!!! Поменять под расположение папки Downloads
        list_of_files = glob.glob(os.path.join(download_dir, "*"))
        list_of_files.sort(key=os.path.getctime, reverse=True)
        last_file = list_of_files[0]
        df2 = pd.read_excel(last_file, header = 1)
        # Преобразуем имена колонок к PEP-8
        df.columns = [self.to_pep_8(col) for col in df.columns]
        df2.columns = [self.to_pep_8(col) for col in df2.columns]
        # Удаляем дубликаты колонок из второго датафрейма перед объединением
        overlap_cols = set(df2.columns).intersection(set(df.columns))
        df_cleaned = df.drop(columns=overlap_cols, errors='ignore')
        df3 = pd.concat([df2, df_cleaned], axis=1, join="inner")
        return df3
        

    def inserting(self, df3):
        engine = create_engine("mysql+mysqlconnector://root:@127.0.0.1:3306/repos", echo=True)

        conn = engine.connect()

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS tonia (
            id INT AUTO_INCREMENT PRIMARY KEY,
            Дата DATE,
            Открытие INT,
            Максимум FLOAT,
            Минимум FLOAT,
            Закрытие FLOAT,
            Объем_сделок_млрд_KZT FLOAT,
            Объем_сделок_млн_USD FLOAT,
            Изменение_с_начала_месяца FLOAT,
            Изменение_с_начала_года FLOAT,
            Максимум_за_52_недели FLOAT,
            Минимум_за_52_недели FLOAT,
            Исторический_максимум FLOAT,
            Исторический_минимум FLOAT
        )
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS trion(
            id INT AUTO_INCREMENT PRIMARY KEY,
            Дата DATE,
            Открытие INT,
            Максимум FLOAT,
            Минимум FLOAT,
            Закрытие FLOAT,
            Объем_сделок_млрд_KZT FLOAT,
            Объем_сделок_млн_USD FLOAT,
            Изменение_с_начала_месяца FLOAT,
            Изменение_с_начала_года FLOAT,
            Максимум_за_52_недели FLOAT,
            Минимум_за_52_недели FLOAT,
            Исторический_максимум FLOAT,
            Исторический_минимум FLOAT
        )
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS twina (
            id INT AUTO_INCREMENT PRIMARY KEY,
            Дата DATE,
            Открытие INT,
            Максимум FLOAT,
            Минимум FLOAT,
            Закрытие FLOAT,
            Объем_сделок_млрд_KZT FLOAT,
            Объем_сделок_млн_USD FLOAT,
            Изменение_с_начала_месяца FLOAT,
            Изменение_с_начала_года FLOAT,
            Максимум_за_52_недели FLOAT,
            Минимум_за_52_недели FLOAT,
            Исторический_максимум FLOAT,
            Исторический_минимум FLOAT
        )
        """))
        conn.commit()
        df3.to_sql(self.name, engine, if_exists = 'append',  index=False)


process = repo(name = 'tonia')
df = process.downloading()
df3 = process.preprocessing(df)
process.inserting(df3)