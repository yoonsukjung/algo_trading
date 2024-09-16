from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time

# ChromeDriver 설정
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service)

url = "https://www.binance.com/en/futures/markets/overview-um"
driver.get(url)

# 페이지가 로드될 시간을 기다림
driver.implicitly_wait(10)

# 카테고리 클릭
category_xpath = r"/html/body/div[1]/div[2]/div[3]/div/div[1]/div[2]/div/div[1]/div[2]/div[1]/div/div[3]/ul/li[2]/div"
category_element = driver.find_element(By.XPATH, category_xpath)
category_element.click()

# 카테고리가 로드될 시간을 기다림
time.sleep(5)  # 필요에 따라 조정

# XPath로 요소 찾기 (raw string 사용)
xpath = r"/html/body/div[1]/div[2]/div[3]/div/div[1]/div[2]/div/div[2]/div[1]/div/div/div/div/table/tbody"
elements = driver.find_elements(By.XPATH, f"{xpath}/*")

# 데이터 저장을 위한 리스트 초기화
data = []

# 각 요소의 첫 번째 열의 첫 번째 줄 텍스트를 추출하여 리스트에 저장
for element in elements:
    first_column = element.find_elements(By.TAG_NAME, "td")[0]
    symbol = first_column.text.split('\n')[0]  # 첫 번째 줄만 가져옴
    data.append([symbol])

# DataFrame으로 변환
df = pd.DataFrame(data)

# 첫 번째 행의 이름을 'symbol'로 설정
df.columns = ['symbol']

# CSV 파일로 저장
df.to_csv("output.csv", index=False)

# 브라우저 종료
driver.quit()