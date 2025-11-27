import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable, GeocoderServiceError
from tqdm import tqdm
import os
import time
import logging
from datetime import datetime
import sys

# ==========================================
# 1. KONFIGURASI & LOGGING KOMPREHENSIF
# ==========================================

# Timestamp untuk file log
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Path files
input_csv = "/home/mrsnow/dev/python/skripsi/skripsi/sentimen/data/data_fnb_karawang_comprehensive.csv"
output_csv = "/home/mrsnow/dev/python/skripsi/skripsi/sentimen/data/data_fnb_karawang_fixed_full.csv"
backup_csv = "/home/mrsnow/dev/python/skripsi/skripsi/sentimen/data/data_fnb_backup_progress.csv"
log_file = f"/home/mrsnow/dev/python/skripsi/skripsi/sentimen/data/geocoding_log_{timestamp}.txt"
summary_report = f"/home/mrsnow/dev/python/skripsi/skripsi/sentimen/data/summary_report_{timestamp}.txt"

# Setup logging KOMPREHENSIF
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),  # Simpan ke file
        logging.StreamHandler(sys.stdout)  # Tampilkan di terminal
    ]
)
logger = logging.getLogger(__name__)

def write_summary_report(content):
    """Menulis laporan summary ke file terpisah"""
    with open(summary_report, 'a', encoding='utf-8') as f:
        f.write(content + '\n')

# Header laporan
write_summary_report("=" * 70)
write_summary_report("LAPORAN PROSES GEOCODING DATA FNB KARAWANG")
write_summary_report("=" * 70)
write_summary_report(f"Tanggal Proses: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
write_summary_report(f"File Input: {input_csv}")
write_summary_report(f"File Output: {output_csv}")
write_summary_report(f"File Log: {log_file}")
write_summary_report("=" * 70)

# Cek apakah file input ada
if not os.path.exists(input_csv):
    error_msg = f"❌ File input tidak ditemukan: {input_csv}"
    logger.error(error_msg)
    write_summary_report(error_msg)
    exit()

logger.info("📂 Membaca data CSV...")
df = pd.read_csv(input_csv)

# Pastikan nilai NaN atau string kosong terdeteksi dengan benar
df = df.replace([None, 'nan', 'NaN', 'None'], '')
logger.info(f"📊 Total data awal: {len(df)} baris")

write_summary_report(f"Total data awal: {len(df)} baris")

# ==========================================
# 2. SETUP GEOCODING DENGAN KONFIGURASI ROBUST
# ==========================================

logger.info("🌍 Menyiapkan layanan Geocoding (Nominatim) dengan timeout yang lebih longgar...")

# User_agent WAJIB unik agar tidak diblokir server OSM
geolocator = Nominatim(
    user_agent=f"skripsi_karawang_fnb_fixer_{timestamp}",
    timeout=10,  # Timeout dinaikkan dari 1 ke 10 detik
    scheme='https'
)

# RateLimiter dengan delay lebih panjang untuk menghindari banned
reverse_geocode = RateLimiter(
    geolocator.reverse, 
    min_delay_seconds=2.0,  # Delay dinaikkan dari 1 ke 2 detik
    max_retries=2,          # Retry jika gagal
    error_wait_seconds=5.0  # Tunggu 5 detik jika error
)

# ==========================================
# 3. FUNGSI EKSTRAKSI ALAMAT DENGAN ERROR HANDLING & LOGGING
# ==========================================

def get_precise_address(lat, lon, current_street, current_city, max_attempts=3):
    """
    Mengambil alamat lengkap jika data saat ini kosong.
    Memprioritaskan data eksisting jika sudah ada.
    """
    # Jika Jalan DAN Kota sudah terisi, tidak perlu request API (hemat waktu)
    if current_street != '' and current_city != '':
        return current_street, current_city, "skip"

    attempts = 0
    while attempts < max_attempts:
        try:
            # Request ke API dengan timeout explicit
            location = reverse_geocode(
                (lat, lon), 
                language='id', 
                exactly_one=True,
                timeout=10  # Timeout explicit
            )
            
            if location and location.raw:
                address = location.raw.get('address', {})
                
                # --- LOGIKA PENGISIAN JALAN ---
                if current_street == '':
                    # Cari tag jalan dari yang paling spesifik
                    new_street = address.get('road', 
                                   address.get('street', 
                                     address.get('pedestrian', 
                                       address.get('highway', ''))))
                    # Jika masih kosong, coba ambil dari display_name
                    if new_street == '' and location.address:
                        # Ekstrak bagian jalan dari display_name (heuristik sederhana)
                        address_parts = location.address.split(',')
                        if len(address_parts) > 0:
                            potential_street = address_parts[0].strip()
                            if any(keyword in potential_street.lower() for keyword in ['jalan', 'jl', 'jln', 'street', 'road']):
                                new_street = potential_street
                                logger.info(f"📍 Jalan diekstrak dari display_name: {new_street}")
                else:
                    new_street = current_street

                # --- LOGIKA PENGISIAN KOTA/KABUPATEN ---
                if current_city == '':
                    # Hierarki administrasi di Indonesia dalam OSM:
                    new_city = address.get('city', 
                                 address.get('town', 
                                   address.get('county', 
                                     address.get('state_district', 
                                       address.get('village', '')))))
                    
                    # Pembersihan nama (misal: "Kabupaten Karawang" -> "Karawang")
                    if new_city:
                        new_city = new_city.replace('Kabupaten ', '').replace('Kota ', '')
                        
                    # Jika masih kosong, coba ambil dari display_name
                    if new_city == '' and location.address:
                        address_parts = location.address.split(',')
                        for part in address_parts:
                            part = part.strip()
                            if 'karawang' in part.lower():
                                new_city = 'Karawang'
                                logger.info(f"🏙️  Kota diekstrak dari display_name: {new_city}")
                                break
                else:
                    new_city = current_city
                    
                # Log keberhasilan
                if current_street == '' and new_street != '':
                    logger.info(f"✅ Berhasil menemukan jalan: {new_street}")
                if current_city == '' and new_city != '':
                    logger.info(f"✅ Berhasil menemukan kota: {new_city}")
                    
                return new_street, new_city, "success"
            else:
                logger.warning(f"⚠️  Tidak ada data location untuk ({lat}, {lon})")
                return current_street, current_city, "no_data"
                
        except (GeocoderTimedOut, GeocoderUnavailable, GeocoderServiceError) as e:
            attempts += 1
            error_msg = f"❌ Attempt {attempts}/{max_attempts} gagal untuk ({lat}, {lon}): {str(e)}"
            logger.warning(error_msg)
            if attempts < max_attempts:
                time.sleep(2)  # Tunggu sebelum retry
            else:
                logger.error(f"🚫 Semua attempt gagal untuk ({lat}, {lon})")
                return current_street, current_city, "error"
        except Exception as e:
            error_msg = f"💥 Unexpected error untuk ({lat}, {lon}): {str(e)}"
            logger.error(error_msg)
            return current_street, current_city, "error"

    return current_street, current_city, "max_attempts"

# ==========================================
# 4. EKSEKUSI DENGAN RESUME CAPABILITY & LOGGING
# ==========================================

logger.info("\n🚀 Memulai proses melengkapi data...")
logger.info("ℹ️  Estimasi: 2-3 detik per data (karena batasan API dan retry)")

write_summary_report("\nPROSES GEOCODING:")
write_summary_report("-" * 50)

# Cek apakah ada backup progress sebelumnya
if os.path.exists(backup_csv):
    logger.info("🔄 Backup progress ditemukan, melanjutkan dari backup...")
    df_backup = pd.read_csv(backup_csv)
    fixed_streets = df_backup['Alamat_Jalan_Fixed'].tolist()
    fixed_cities = df_backup['Kota_Fixed'].tolist()
    status_log = df_backup['Status'].tolist()
    start_index = len(fixed_streets)
    
    logger.info(f"↳ Melanjutkan dari index: {start_index}/{len(df)}")
    write_summary_report(f"Melanjutkan dari backup: index {start_index}/{len(df)}")
else:
    # Inisialisasi baru
    fixed_streets = []
    fixed_cities = []
    status_log = []
    start_index = 0
    write_summary_report("Memulai proses dari awal")

# Statistik real-time
stats = {
    'success': 0,
    'skip': 0,
    'error': 0,
    'no_data': 0,
    'start_time': time.time()
}

# Process dengan progress bar
pbar = tqdm(total=len(df), initial=start_index, desc="Processing")

for index in range(start_index, len(df)):
    row = df.iloc[index]
    lat = row['Latitude']
    lon = row['Longitude']
    street = str(row['Alamat_Jalan']).strip() if pd.notna(row['Alamat_Jalan']) else ''
    city = str(row['Kota']).strip() if pd.notna(row['Kota']) else ''
    
    # Panggil fungsi perbaikan
    new_street, new_city, status = get_precise_address(lat, lon, street, city)
    
    fixed_streets.append(new_street)
    fixed_cities.append(new_city)
    status_log.append(status)
    
    # Update statistik
    stats[status] = stats.get(status, 0) + 1
    
    # Update progress bar
    pbar.update(1)
    pbar.set_postfix({
        'Status': status, 
        'Index': index,
        'Success': stats['success'],
        'Error': stats['error']
    })
    
    # Backup progress setiap 10 data
    if index % 10 == 0:
        df_temp = df.iloc[:len(fixed_streets)].copy()
        df_temp['Alamat_Jalan_Fixed'] = fixed_streets
        df_temp['Kota_Fixed'] = fixed_cities
        df_temp['Status'] = status_log
        df_temp.to_csv(backup_csv, index=False)
        logger.info(f"💾 Backup created at index {index}")

pbar.close()

# Hapus backup file setelah selesai
if os.path.exists(backup_csv):
    os.remove(backup_csv)
    logger.info("🗑️  Backup file dihapus (proses selesai)")

# ==========================================
# 5. SIMPAN HASIL & BUAT LAPORAN LENGKAP
# ==========================================

# Hitung waktu proses
processing_time = time.time() - stats['start_time']
minutes = int(processing_time // 60)
seconds = int(processing_time % 60)

# Buat dataframe hasil
df_fixed = df.copy()
df_fixed['Alamat_Jalan'] = fixed_streets
df_fixed['Kota'] = fixed_cities
df_fixed['Geocoding_Status'] = status_log

# Hitung perbaikan
original_empty_street = (df['Alamat_Jalan'] == '') | (df['Alamat_Jalan'].isna())
original_empty_city = (df['Kota'] == '') | (df['Kota'].isna())

filled_street_count = sum(original_empty_street & (df_fixed['Alamat_Jalan'] != ''))
filled_city_count = sum(original_empty_city & (df_fixed['Kota'] != ''))

# Statistik status
status_counts = pd.Series(status_log).value_counts()

# SIMPAN LAPORAN LENGKAP
write_summary_report("\nHASIL PROSES:")
write_summary_report("-" * 50)
write_summary_report(f"Waktu proses: {minutes} menit {seconds} detik")
write_summary_report(f"Total data diproses: {len(df)} baris")
write_summary_report(f"Alamat Jalan diperbaiki: {filled_street_count} baris")
write_summary_report(f"Kota/Kabupaten diperbaiki: {filled_city_count} baris")
write_summary_report("\nSTATISTIK STATUS GEOCODING:")
write_summary_report(f"  ✅ Success: {status_counts.get('success', 0)}")
write_summary_report(f"  ⏭️  Skip: {status_counts.get('skip', 0)}")
write_summary_report(f"  ❌ Error: {status_counts.get('error', 0)}")
write_summary_report(f"  ⚠️  No Data: {status_counts.get('no_data', 0)}")

# Hitung success rate
total_attempted = len(df) - status_counts.get('skip', 0)
if total_attempted > 0:
    success_rate = (status_counts.get('success', 0) / total_attempted) * 100
    write_summary_report(f"  📊 Success Rate: {success_rate:.2f}%")

write_summary_report("\nDETAIL PERBAIKAN:")
write_summary_report("-" * 50)

# Analisis perbaikan lebih detail
original_street_empty = original_empty_street.sum()
original_city_empty = original_empty_city.sum()

write_summary_report(f"Alamat Jalan kosong awal: {original_street_empty}")
write_summary_report(f"Alamat Jalan kosong akhir: {original_street_empty - filled_street_count}")
write_summary_report(f"Kota kosong awal: {original_city_empty}")
write_summary_report(f"Kota kosong akhir: {original_city_empty - filled_city_count}")

write_summary_report("\nFILE OUTPUT:")
write_summary_report("-" * 50)
write_summary_report(f"Data hasil: {output_csv}")
write_summary_report(f"Log proses: {log_file}")
write_summary_report(f"Laporan ini: {summary_report}")

write_summary_report("\n" + "=" * 70)
write_summary_report("PROSES SELESAI")
write_summary_report("=" * 70)

# Simpan ke CSV baru
df_fixed.to_csv(output_csv, index=False, encoding='utf-8')

# LOG FINAL
logger.info("\n" + "=" * 60)
logger.info(f"✅ PROSES SELESAI dalam {minutes}m {seconds}s!")
logger.info(f"📊 Statistik Final:")
logger.info(f"   ✅ Success: {status_counts.get('success', 0)}")
logger.info(f"   ⏭️  Skip: {status_counts.get('skip', 0)}") 
logger.info(f"   ❌ Error: {status_counts.get('error', 0)}")
logger.info(f"   📝 Jalan diperbaiki: {filled_street_count} baris")
logger.info(f"   🏙️  Kota diperbaiki: {filled_city_count} baris")

logger.info(f"\n📂 File hasil tersimpan di: {output_csv}")
logger.info(f"📋 Laporan lengkap: {summary_report}")
logger.info(f"📝 Log detail: {log_file}")
logger.info("=" * 60)

# Preview Data untuk log
logger.info("\n🔍 Preview 5 Data Teratas (Setelah Perbaikan):")
preview_df = df_fixed[['Nama_Tempat', 'Alamat_Jalan', 'Kota', 'Geocoding_Status']].head(5)
for _, row in preview_df.iterrows():
    logger.info(f"  📍 {row['Nama_Tempat']} | Jalan: {row['Alamat_Jalan']} | Kota: {row['Kota']} | Status: {row['Geocoding_Status']}")

print(f"\n🎉 PROSES SELESAI! Semua log tersimpan di:")
print(f"   📋 Laporan: {summary_report}")
print(f"   📝 Log detail: {log_file}")
print(f"   💾 Data hasil: {output_csv}")