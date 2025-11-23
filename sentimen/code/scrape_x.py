import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# Setup Chrome Options (Agar tidak terdeteksi bot)
options = webdriver.ChromeOptions()
# options.add_argument("--headless") # Jangan nyalakan headless dulu agar bisa lihat prosesnya
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36")

# Inisialisasi Driver
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)


def login_twitter(username, password, email_verification):
    driver.get("https://twitter.com/i/flow/login")
    time.sleep(5) # Tunggu loading

    # 1. Masukkan Username
    user_input = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.NAME, "text"))
    )
    user_input.send_keys(username)
    user_input.send_keys(Keys.ENTER)
    time.sleep(3)

    # 2. Cek apakah minta verifikasi (kadang muncul minta email/no hp)
    try:
        verif_input = driver.find_element(By.NAME, "text")
        verif_input.send_keys(email_verification) # Masukkan email/no hp jika diminta
        verif_input.send_keys(Keys.ENTER)
        time.sleep(3)
    except:
        pass # Jika tidak diminta, lanjut

    # 3. Masukkan Password
    pass_input = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.NAME, "password"))
    )
    pass_input.send_keys(password)
    pass_input.send_keys(Keys.ENTER)
    
    print("Login Berhasil!")
    time.sleep(5)



def scrape_tweets(keyword, since_date, until_date, max_tweets=50):
    # Format query pencarian X: "Persib since:2023-01-01 until:2023-01-02 lang:id"
    # lang:id agar hanya mengambil tweet bahasa Indonesia
    query = f"{keyword} since:{since_date} until:{until_date} lang:id"
    encoded_query = query.replace(" ", "%20").replace(":", "%3A")
    
    url = f"https://twitter.com/search?q={encoded_query}&src=typed_query&f=live"
    driver.get(url)
    time.sleep(5)

    data = []
    tweet_ids = set()
    last_height = driver.execute_script("return document.body.scrollHeight")
    
    while len(data) < max_tweets:
        # Cari semua elemen tweet yang ada di layar
        cards = driver.find_elements(By.XPATH, '//article[@data-testid="tweet"]')
        
        for card in cards:
            try:
                # Ambil Username
                user = card.find_element(By.XPATH, './/div[@data-testid="User-Name"]').text
                
                # Ambil Teks Tweet
                text = card.find_element(By.XPATH, './/div[@data-testid="tweetText"]').text
                
                # Ambil Waktu
                date_element = card.find_element(By.TAG_NAME, 'time')
                date = date_element.get_attribute('datetime')
                
                # Unik ID (Untuk mencegah duplikat)
                tweet_signature = user + date
                
                if tweet_signature not in tweet_ids:
                    tweet_ids.add(tweet_signature)
                    data.append([date, user, text])
                    
            except Exception as e:
                continue # Skip jika ada elemen yang gagal diambil
        
        # Scroll ke bawah
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3) # Tunggu loading tweet baru
        
        # Cek apakah sudah mentok bawah
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height
        
        print(f"Mengumpulkan {len(data)} tweet...")

    return pd.DataFrame(data, columns=['Date', 'User', 'Text'])


# --- CONFIGURASI ANDA ---
MY_USER = "username_x_anda"
MY_PASS = "password_x_anda"
MY_EMAIL = "email_atau_nohp_anda" # Untuk verifikasi jika diminta

KEYWORD = "Persib" 
# Contoh: Ambil data sebelum laga Persib vs Persija (misal laga tgl 10 Jan 2023)
# Maka ambil dari tgl 7 sampai 9 Jan.
START_DATE = "2023-01-07"
END_DATE = "2023-01-09"

try:
    # 1. Login
    login_twitter(MY_USER, MY_PASS, MY_EMAIL)
    
    # 2. Scrape
    df_tweets = scrape_tweets(KEYWORD, START_DATE, END_DATE, max_tweets=100)
    
    # 3. Simpan
    filename = f"tweets_persib_{START_DATE}.csv"
    df_tweets.to_csv(filename, index=False)
    print(f"Selesai! Data disimpan di {filename}")
    print(df_tweets.head())

finally:
    driver.quit()