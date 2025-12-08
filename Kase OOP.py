# Importing libraries
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
        str = 'https://kase.kz/ru/indexes-and-indicators/repo/{}'   # URL format template
        url = str.format(self.name)     
   
        soup = BeautifulSoup(requests.get(url).text, 'lxml')  # Accessing the website
        table = soup.find('c-table')
        final_data = []
        for tr in table.find_all('tr')[:-1]:  # Extracting table text for the selected indicator
            final_data.append(re.split(r'\s{2,}', tr.text)[0:2])
            df = pd.DataFrame(final_data).T
            new_header = df.iloc[0]
            df = df[1:]
            df.columns = new_header
            df = df.rename(index={1 : 0})
            
        # Selenium setup settings
        chromedriver = Service(ChromeDriverManager().install())  # Browser driver (Chrome version), can make Firefox separately if needed
        options = webdriver.ChromeOptions() 
        options.add_experimental_option("detach", True)
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        browser = webdriver.Chrome(service=chromedriver, options=options)
        browser.get(url)
        browser.find_element(By.XPATH, "//button[span[text()='Скачать файл']]").click()  # Downloads today’s files
        time.sleep(2)
        return df


    def to_pep_8(self, bob):
        bob = bob.replace(' ', '_')
        bob = bob.replace(',', '')
        return bob

    
    def preprocessing(self, df):
        download_dir = r"C:\Users\user\Downloads"  # !!!! Change this to your actual Downloads folder path
        list_of_files = glob.glob(os.path.join(download_dir, "*"))
        list_of_files.sort(key=os.path.getctime, reverse=True)
        last_file = list_of_files[0]
        df2 = pd.read_excel(last_file, header = 1)  # Opening downloaded file

        # Converting column names to PEP-8 style
        df.columns = [self.to_pep_8(col) for col in df.columns]
        df2.columns = [self.to_pep_8(col) for col in df2.columns]

        # Removing duplicate columns before merging
        overlap_cols = set(df2.columns).intersection(set(df.columns))
        df_cleaned = df.drop(columns=overlap_cols, errors='ignore')
        df3 = pd.concat([df2, df_cleaned], axis=1, join="inner")  # Merging data from website and file
        return df3
        

    def inserting(self, df3):
        engine = create_engine("mysql+mysqlconnector://root:@127.0.0.1:3306/repos", echo=True)
        conn = engine.connect()
        
        # Manually creating tables to correctly specify data types
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
        df3.to_sql(self.name, engine, if_exists='append', index=False)


process = repo(name='tonia')  # Available indicators: tonia, trion, twina
df = process.downloading()
df3 = process.preprocessing(df)
process.inserting(df3)
