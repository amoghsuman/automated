# -*- coding: utf-8 -*-
"""
Created on Sat May 28 19:02:00 2022

@author: HP
"""
from selenium import webdriver
import os
from selenium.webdriver.common.by import By
import time
from pyotp import TOTP

cwd = os.chdir("C:/Users/HP/Desktop/Zerodha_API_Testing")

token_path = "api_key.txt"
key_secret = open(token_path,'r').read().split()
options = webdriver.ChromeOptions()
#options.add_argument('--headless')
options = options.to_capabilities()
service = webdriver.chrome.service.Service('./chromedriver')
service.start()
driver = webdriver.Remote(service.service_url, options)
driver.get("https://web.sensibull.com/option-chain?expiry=2022-07-28&tradingsymbol=BANKNIFTY")
driver.implicitly_wait(30)
time.sleep(5)

driver.find_element(by=By.XPATH, value='/html/body/div[1]/div/div[4]/div[2]/div[1]/div/div[2]/button').click()
driver.find_element(by=By.XPATH, value='//*[@id="notloggedInSegment"]/div/div[2]/div[2]/button[1]').click()
username = driver.find_element_by_xpath('/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[1]/input')
password = driver.find_element_by_xpath('/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[2]/input')
username.send_keys(key_secret[2])
password.send_keys(key_secret[3])
driver.find_element_by_xpath('/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[4]/button').click()
pin = driver.find_element_by_xpath('/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[2]/div/input')
totp = TOTP(key_secret[4])
token = totp.now()
pin.send_keys(token)
driver.find_element_by_xpath('/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[3]/button').click()
time.sleep(10)
driver.maximize_window()
driver.find_element(by=By.XPATH, value='//*[@id="app"]/div/div[4]/div[2]/div[2]/div/div/div[2]/div[2]').click()
driver.find_element(by=By.XPATH, value='/html/body/div[3]/div[3]/div/div[3]/button').click()







