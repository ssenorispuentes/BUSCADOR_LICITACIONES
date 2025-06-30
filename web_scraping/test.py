from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests
import os

# Configure webdriver (ensure you have ChromeDriver installed)
options = webdriver.ChromeOptions()
options.headless = True
driver = webdriver.Chrome(options=options)

# Set download folder
download_folder = os.path.join(os.getcwd(), "downloads")
os.makedirs(download_folder, exist_ok=True)

# URL to test
url = "https://contrataciondelestado.es/wps/portal/!ut/p/b0/DcoxCoAwDADA1zjHRRShg4ObLi7aLhLaUKJpLRgEf6_jwYGDDVzGhyMqXxnltw1ERTiffSBFEdqFPSv6P8AKDhyH8RGwZ6K5qqe706NtFnqbzmIs0RgoKQ0fAji4rQ!!/"


# Start testing function
def find_pdf_pliego(url):
    try:
        # Open URL in browser
        driver.get(url)

        # Wait until the page is loaded
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a.TextAlignCenter.celdaTam2")))

        # Find the PDF download link
        enlace_pdf = driver.find_element(By.CSS_SELECTOR, "a.TextAlignCenter.celdaTam2")

        # Get the href attribute (download link)
        pdf_url = enlace_pdf.get_attribute("href")
        if pdf_url:
            print(f"Found PDF link: {pdf_url}")
            # Download the PDF
            response = requests.get(pdf_url)
            if response.status_code == 200:
                pdf_filename = os.path.join(download_folder, "pliego_prescripciones.pdf")
                with open(pdf_filename, 'wb') as f:
                    f.write(response.content)
                print(f"✅ PDF downloaded to {pdf_filename}")
            else:
                print(f"❌ Failed to download PDF. HTTP Status: {response.status_code}")
        else:
            print("❌ No valid PDF link found.")

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        driver.quit()


# Run test
test_pdf_download(url)
