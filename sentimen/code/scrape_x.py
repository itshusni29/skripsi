
import os
import time
import random
import pandas as pd
import undetected_chromedriver as uc # Library Baru
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ==========================================
# 1. KONFIGURASI
# ==========================================
SAVE_DIR = "/home/mrsnow/dev/python/skripsi/skripsi/sentimen/data"
# Simaung265034
# XnTuv-=223
# Target Scraping
TARGET_START_DATE = "2017-04-13"
TARGET_UNTIL_DATE = "2017-04-16"
MAX_DATA = 50 

# ==========================================
# 2. QUERY BUILDER
# ==========================================
def build_query():
    keywords = ["Persib", "#PersibDay", "Maung Bandung", "Sib"]
    query_string = "(" + " OR ".join(keywords) + ")"
    # Kita hapus filter strict agar data lama yang sedikit bisa terambil
    final_query = f"{query_string} lang:id since:{TARGET_START_DATE} until:{TARGET_UNTIL_DATE}"
    return final_query

# ==========================================
# 3. SETUP DRIVER (MODIFIED)
# ==========================================
def setup_driver():
    print("🔧 Menyiapkan Undetected Chrome...")
    options = uc.ChromeOptions()
    # options.add_argument("--headless") # Jangan nyalakan headless saat login manual
    options.add_argument("--disable-popup-blocking")
    
    # Inisialisasi driver versi undetected
    # use_subprocess=True membantu kestabilan di Linux
    driver = uc.Chrome(options=options, use_subprocess=True) 
    return driver

def random_sleep(min_sec=2, max_sec=5):
    time.sleep(random.uniform(min_sec, max_sec))

# ==========================================
# 4. LOGIN MANUAL (STEALTH MODE)
# ==========================================
def login_manual_step(driver):
    print("🚀 Membuka Halaman Login...")
    driver.get("https://twitter.com/i/flow/login")
    
    print("\n" + "="*50)
    print("🛑 PERHATIAN: SILAKAN LOGIN MANUAL SEKARANG")
    print("="*50)
    print("Karena pakai 'undetected-chromedriver', X tidak akan tahu ini bot.")
    print("Silakan masukkan username & password dengan tenang.")
    print("="*50)
    
    input("👉 Setelah berhasil masuk Beranda, TEKAN ENTER di sini...")
    print("✅ Lanjut proses scraping...")

# ==========================================
# 5. SCRAPING LOGIC
# ==========================================
def scrape_tweets(driver, query):
    print(f"🔎 Mencari data: {query}")
    encoded_query = query.replace(" ", "%20").replace(":", "%3A").replace("(", "%28").replace(")", "%29")
    url = f"https://twitter.com/search?q={encoded_query}&src=typed_query&f=live"
    
    driver.get(url)
    time.sleep(5) 

    data = []
    unique_ids = set()
    scroll_attempts = 0
    last_height = driver.execute_script("return document.body.scrollHeight")

    while len(data) < MAX_DATA:
        articles = driver.find_elements(By.XPATH, '//article[@data-testid="tweet"]')
        
        if not articles:
            time.sleep(2)
        
        for article in articles:
            try:
                try:
                    username = article.find_element(By.XPATH, './/div[@data-testid="User-Name"]').text.split('\n')[0]
                except: continue

                try:
                    text_content = article.find_element(By.XPATH, './/div[@data-testid="tweetText"]').text
                except: text_content = "[Media Only]"

                time_elm = article.find_element(By.TAG_NAME, 'time')
                dt_str = time_elm.get_attribute('datetime')
                
                tweet_id = f"{username}_{dt_str}"
                
                if tweet_id not in unique_ids:
                    unique_ids.add(tweet_id)
                    clean_text = text_content.replace('\n', ' ').replace(';', ',')
                    
                    data.append({'date': dt_str, 'username': username, 'text': clean_text})
                    print(f"[+{len(data)}] {dt_str[:10]} | {username}")
                
                if len(data) >= MAX_DATA: break
            except Exception: continue

        if len(data) >= MAX_DATA: break

        driver.execute_script("window.scrollBy(0, 600);") 
        random_sleep(2, 4)
        
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            scroll_attempts += 1
            print(f"⏳ Loading... ({scroll_attempts}/4)")
            if scroll_attempts >= 4:
                print("🛑 Mentok / Tidak ada data lagi.")
                break
        else:
            scroll_attempts = 0
            last_height = new_height
            
    return pd.DataFrame(data)

# ==========================================
# 6. MAIN EXECUTION
# ==========================================
if __name__ == "__main__":
    if not os.path.exists(SAVE_DIR):
        os.makedirs(SAVE_DIR)

    driver = setup_driver()
    
    try:
        login_manual_step(driver)
        
        search_query = build_query()
        df_result = scrape_tweets(driver, search_query)
        
        if not df_result.empty:
            filename = f"tweets_persib_{TARGET_START_DATE}_stealth.csv"
            full_path = os.path.join(SAVE_DIR, filename)
            df_result.to_csv(full_path, index=False)
            print(f"✅ SUKSES! Data disimpan di: {full_path}")
        else:
            print("❌ Data kosong.")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # driver.quit() # Opsional: Komen ini jika ingin browser tetap terbuka untuk debug
        pass