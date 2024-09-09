from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd

# ChromeDriver 설정
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service)

url = "https://www.binance.com/en/futures/markets/overview-um"
driver.get(url)

# 페이지가 로드될 시간을 기다림
driver.implicitly_wait(10)

# XPath로 요소 찾기
xpath = "/html/body/div[1]/div[2]/div[3]/div/div[1]/div[2]/div/div[2]/div[1]/div/div/div/div/table/tbody"
elements = driver.find_elements(By.XPATH, f"{xpath}/*")

# 데이터 저장을 위한 리스트 초기화
data = []

# 각 요소의 텍스트를 추출하여 리스트에 저장
for element in elements:
    row_data = [td.text for td in element.find_elements(By.TAG_NAME, "td")]
    data.append(row_data)

# DataFrame으로 변환
df = pd.DataFrame(data)

# CSV 파일로 저장
df.to_csv("output.csv", index=False)

# 브라우저 종료
driver.quit()