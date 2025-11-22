import requests
from bs4 import BeautifulSoup
import re
import time
import random
import pandas as pd

# --- KONFIGURASI ---
# Header agar dianggap sebagai browser manusia (Chrome)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

def get_attendance_from_news(lawan, tahun):
    """
    Fungsi untuk mencari jumlah penonton laga Away Persib di Google News/Search
    """
    # 1. Buat Query Pencarian Spesifik
    # Kita gunakan operator 'site:' untuk fokus ke situs berita terpercaya agar datanya valid
    query = f'jumlah penonton "Persib" vs "{lawan}" {tahun} site:detik.com OR site:kompas.com OR site:bola.net OR site:antaranews.com'
    
    # Format URL Google Search
    url = f"https://www.google.com/search?q={query}"
    
    try:
        # 2. Request ke Google
        response = requests.get(url, headers=HEADERS)
        
        if response.status_code == 429:
            return "Error: Terblokir Google (Too Many Requests)"
        
        soup = BeautifulSoup(response.text, "html.parser")
        
        # 3. Ambil semua teks snippet (judul dan deskripsi hasil pencarian)
        # Di Google HTML, snippet biasanya ada di dalam tag <div> atau <span> tertentu
        text_content = soup.get_text()
        
        # 4. EKSTRAKSI ANGKA MENGGUNAKAN REGEX (Regular Expression)
        # Pola A: Mencari angka format "20.000 penonton" atau "20.000 orang"
        # Penjelasan Regex: (\d{1,3}[.]\d{3}) artinya mencari angka digit yg dipisah titik
        pattern_a = r"(\d{1,3}[.]\d{3})\s?(?:penonton|orang|suporter)"
        
        # Pola B: Mencari angka format "20 ribu penonton"
        pattern_b = r"(\d+)\s?ribu\s?(?:penonton|orang|suporter)"
        
        match_a = re.search(pattern_a, text_content)
        match_b = re.search(pattern_b, text_content)
        
        if match_a:
            # Bersihkan titik (misal 15.000 jadi 15000)
            angka = match_a.group(1).replace('.', '')
            return int(angka)
        
        elif match_b:
            # Konversi "15 ribu" jadi 15000
            angka = int(match_b.group(1)) * 1000
            return angka
        
        else:
            return "Tidak ditemukan di snippet"

    except Exception as e:
        return f"Error: {e}"

# --- CONTOH DATA AWAY PERSIB YANG INGIN DICARI ---
# (Nanti Anda bisa isi list ini dari jadwal pertandingan yg sudah Anda punya)
list_laga_away = [
    {"Lawan": "PSS Sleman", "Tahun": 2023},
    {"Lawan": "PSIS Semarang", "Tahun": 2023},
    {"Lawan": "Arema FC", "Tahun": 2022},  # Contoh laga away Big Match
    {"Lawan": "Persija Jakarta", "Tahun": 2023}
]

results = []

print("--- MEMULAI SCRAPING DATA BERITA ---\n")

for match in list_laga_away:
    lawan = match['Lawan']
    tahun = match['Tahun']
    
    print(f"Mencari data: Persib (Away) vs {lawan} ({tahun})...")
    
    jumlah_penonton = get_attendance_from_news(lawan, tahun)
    
    print(f"--> Hasil: {jumlah_penonton}")
    
    results.append({
        "Lawan": lawan,
        "Tahun": tahun,
        "Penonton_Away_Berita": jumlah_penonton
    })
    
    # PENTING: Jeda waktu acak 5-10 detik agar tidak diblokir Google
    time.sleep(random.uniform(5, 10))

# --- HASIL AKHIR ---
df_hasil = pd.DataFrame(results)
print("\n--- TABEL DATA FINAL ---")
print(df_hasil)

# df_hasil.to_csv('data_penonton_berita.csv', index=False)