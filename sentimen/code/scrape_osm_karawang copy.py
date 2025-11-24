import osmnx as ox
import pandas as pd
import matplotlib.pyplot as plt

# ==========================================
# 1. KONFIGURASI WILAYAH & KATEGORI
# ==========================================

# Daftar Kecamatan Target (Gunakan nama resmi di OSM)
target_places = [
    # --- KOTA & INDUSTRI ---
    "Kecamatan Karawang Barat, Karawang, Indonesia",
    "Kecamatan Karawang Timur, Karawang, Indonesia",
    "Kecamatan Telukjambe Timur, Karawang, Indonesia",
    "Kecamatan Telukjambe Barat, Karawang, Indonesia",
    "Kecamatan Ciampel, Karawang, Indonesia",  # Banyak Pabrik
    
    # --- HUNIAN & PENYANGGA ---
    "Kecamatan Klari, Karawang, Indonesia",
    "Kecamatan Majalaya, Karawang, Indonesia",
    "Kecamatan Purwasari, Karawang, Indonesia",
    
    # --- KORIDOR CIKAMPEK ---
    "Kecamatan Cikampek, Karawang, Indonesia",
    "Kecamatan Kotabaru, Karawang, Indonesia",
    "Kecamatan Jatisari, Karawang, Indonesia",
    "Kecamatan Tirtamulya, Karawang, Indonesia",

    # --- SATELIT UTARA (NON-LAUT) ---
    "Kecamatan Rengasdengklok, Karawang, Indonesia", # Pusat ekonomi utara
    "Kecamatan Kutawaluya, Karawang, Indonesia",
    
    # --- SELATAN (WISATA ALAM) ---
    "Kecamatan Pangkalan, Karawang, Indonesia",
    "Kecamatan Tegalwaru, Karawang, Indonesia"
]

# Kategori Bisnis yang mau diambil (Tags OSM)
# Referensi tags: https://wiki.openstreetmap.org/wiki/Map_features
target_tags = {
    "amenity": [
        "cafe", "restaurant", "fast_food", "food_court", 
        "coworking_space", "gym", "pharmacy", "cinema", 
    ],
    "shop": [
        "coffee", "bakery", "laundry", "supermarket", "convenience",
        "clothes", "beauty", "pet", "pet_grooming"
    ],
    "leisure": [
        "fitness_centre", "park"
    ]
}

# ==========================================
# 2. PROSES DOWNLOAD DATA
# ==========================================

print(f"🚀 Memulai download data OSM untuk wilayah: {target_places}")
print("⏳ Mohon tunggu, proses ini butuh waktu tergantung koneksi internet...")

try:
    # Fungsi sakti osmnx untuk mengambil fitur (POI)
    gdf = ox.features_from_place(target_places, tags=target_tags)
    
    print(f"✅ Berhasil mengunduh data mentah! Total entitas ditemukan: {len(gdf)}")

except Exception as e:
    print(f"❌ Terjadi kesalahan saat download: {e}")
    exit()

# ==========================================
# 3. PEMBERSIHAN DATA (CLEANING)
# ==========================================

print("🧹 Sedang membersihkan dan merapikan data...")

# A. Pilih kolom yang penting saja
# OSM mengembalikan ratusan kolom, kita cuma butuh ini:
desired_columns = ['name', 'amenity', 'shop', 'leisure', 'addr:street', 'geometry']

# Filter kolom yang ada saja (karena kadang kolom 'leisure' kosong jika tidak ada data)
existing_columns = [col for col in desired_columns if col in gdf.columns]
df = gdf[existing_columns].copy()

# B. Gabungkan kategori menjadi satu kolom 'category'
# Prioritas: amenity -> shop -> leisure
df['category'] = df['amenity'].fillna(df['shop']).fillna(df.get('leisure', pd.Series([None]*len(df))))

# C. Ambil Koordinat Latitude & Longitude
# Geometry di OSM bisa berupa Point (Titik) atau Polygon (Gedung)
# Kita ambil titik tengahnya (centroid) agar seragam
df['lat'] = df.geometry.centroid.y
df['lon'] = df.geometry.centroid.x

# D. Hapus data yang tidak punya Nama
# (Biasanya bangunan tanpa identitas bisnis)
df_clean = df.dropna(subset=['name']).copy()

# E. Rapikan dataframe akhir
final_df = df_clean[['name', 'category', 'lat', 'lon', 'addr:street']]
final_df.columns = ['Nama Tempat', 'Kategori', 'Latitude', 'Longitude', 'Nama Jalan']

# ==========================================
# 4. SIMPAN HASIL & VISUALISASI
# ==========================================

# Simpan ke CSV (Excel)
csv_filename = "data_pesaing_karawang.csv"
final_df.to_csv(csv_filename, index=False)

print("\n" + "="*40)
print(f"🎉 SELESAI! Data Supply Karawang berhasil diambil.")
print(f"📂 File tersimpan di: {csv_filename}")
print(f"📊 Total Data Bersih: {len(final_df)} lokasi bisnis")
print("="*40)

# Tampilkan 10 data pertama
print(final_df.head(10))

# (Opsional) Plot Peta Sederhana
print("\n🗺️ Menampilkan peta sebaran...")
try:
    ox.plot_graph(ox.graph_from_place(target_places), show=False, close=False, edge_color='#999999', edge_linewidth=0.2, node_size=0)
    plt.scatter(final_df['Longitude'], final_df['Latitude'], c='red', s=10, alpha=0.7, label='Bisnis')
    plt.title(f"Peta Sebaran Bisnis di {target_places[0]} & {target_places[1]}")
    plt.legend()
    plt.show()
except:
    print("⚠️ Tidak bisa menampilkan plot peta (mungkin error di library matplotlib), tapi CSV aman.")