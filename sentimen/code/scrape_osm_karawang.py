import osmnx as ox
import pandas as pd
import matplotlib.pyplot as plt
from shapely.geometry import box, Polygon
import os
import numpy as np

# ==========================================
# 1. KONFIGURASI PATH & WILAYAH
# ==========================================

# Path penyimpanan data
output_dir = "/home/mrsnow/dev/python/skripsi/skripsi/sentimen/data"
os.makedirs(output_dir, exist_ok=True)

# ==========================================
# 2. DEFINE AREA COVERAGE - SELURUH KARAWANG TANPA PESISIR
# ==========================================

# Bounding box utama untuk seluruh Kabupaten Karawang
karawang_bbox_main = [106.90, -6.60, 107.50, -6.10]  # [min_lon, min_lat, max_lon, max_lat]

# Area pesisir yang akan di-exclude (daerah pantai utara)
coastal_exclude_bbox = [
    [106.90, -6.10, 107.50, -6.00],   # Area paling utara (pesisir)
    [106.90, -6.00, 107.20, -5.90]    # Area pesisir timur laut
]

# Area tambahan untuk memastikan coverage penuh
additional_areas = [
    [107.00, -6.50, 107.60, -6.20],   # Area timur
    [106.80, -6.40, 107.00, -6.20],   # Area barat
    [107.20, -6.30, 107.50, -6.10]    # Area tengah timur
]

print("🗺️ Mendefinisikan area coverage Karawang (tanpa pesisir pantai)...")

# ==========================================
# 3. KATEGORI F&B (FOOD & BEVERAGE) LEBIH LUAS
# ==========================================

target_tags = {
    "amenity": [
        "restaurant", "cafe", "fast_food", "food_court", 
        "bar", "pub", "ice_cream", "bbq", "food_court"
    ],
    "shop": [
        "coffee", "bakery", "butcher", "seafood", 
        "beverages", "wine", "cheese", "deli",
        "supermarket", "convenience", "grocery",
        "butcher", "pastry"
    ],
    "tourism": [
        "hotel", "guest_house", "apartment", "motel"
    ],
    "leisure": [
        "park"  # kadang ada cafe di taman
    ]
}

# Keyword F&B yang lebih komprehensif
f_b_keywords = [
    # English
    'restaurant', 'cafe', 'food', 'bakery', 'coffee', 'bistro',
    'warung', 'rumah makan', 'kedai', 'kopi', 'resto', 'café',
    'fast food', 'burger', 'pizza', 'nasi', 'ayam', 'sate',
    'steak', 'seafood', 'sushi', 'ramen', 'dimsum',
    'juice', 'tea', 'milk', 'bubble', 'dessert', 'ice cream',
    
    # Indonesian
    'warteg', 'padang', 'sunda', 'jawa', 'bali', 'china',
    'goreng', 'bakar', 'rebus', 'kuah', 'soto', 'bakso',
    'mie', 'bubur', 'martabak', 'roti', 'kue', 'donat',
    'gorengan', 'es', 'jus', 'teh', 'kopi', 'susu',
    
    # Nama franchise lokal
    'alfamart', 'indomaret', 'circle k', 'family mart'
]

# ==========================================
# 4. FUNGSI UNTUK DOWNLOAD DATA DARI BERBAGAI AREA
# ==========================================

def download_osm_data(bbox_list, area_name):
    """Download data OSM dari multiple bounding boxes"""
    all_data = []
    
    for i, bbox in enumerate(bbox_list):
        try:
            print(f"📍 Download {area_name} area {i+1}...")
            bbox_polygon = box(*bbox)
            gdf_area = ox.features_from_polygon(bbox_polygon, tags=target_tags)
            
            if not gdf_area.empty:
                all_data.append(gdf_area)
                print(f"   ✅ Berhasil: {len(gdf_area)} entitas")
            else:
                print(f"   ⚠️ Tidak ada data di area ini")
                
        except Exception as e:
            print(f"   ❌ Gagal area {i+1}: {e}")
            continue
    
    if all_data:
        combined_gdf = pd.concat(all_data, ignore_index=True)
        # Hapus duplikat berdasarkan geometry
        combined_gdf = combined_gdf.drop_duplicates(subset=['geometry'], keep='first')
        return combined_gdf
    else:
        return pd.DataFrame()

def exclude_coastal_areas(gdf, exclude_bboxes):
    """Filter out data yang berada di area pesisir"""
    if gdf.empty:
        return gdf
    
    print(f"🗺️  Memfilter data di area pesisir...")
    
    # Reset index untuk memastikan indexing yang konsisten
    gdf = gdf.reset_index(drop=True)
    coastal_mask = []
    
    for i, row in gdf.iterrows():
        try:
            # Dapatkan centroid dari geometry
            if hasattr(row.geometry, 'x') and hasattr(row.geometry, 'y'):
                # Jika sudah Point
                lon, lat = row.geometry.x, row.geometry.y
            else:
                # Jika Polygon/MultiPolygon, ambil centroid
                centroid = row.geometry.centroid
                lon, lat = centroid.x, centroid.y
            
            # Cek apakah titik berada di area pesisir yang di-exclude
            in_coastal = False
            for coastal_bbox in exclude_bboxes:
                min_lon, min_lat, max_lon, max_lat = coastal_bbox
                if (min_lon <= lon <= max_lon) and (min_lat <= lat <= max_lat):
                    in_coastal = True
                    break
            
            coastal_mask.append(in_coastal)
            
        except Exception as e:
            print(f"⚠️ Error memproses row {i}: {e}")
            coastal_mask.append(False)  # Default tidak di-exclude jika error
    
    # Hanya ambil data yang TIDAK di area pesisir
    filtered_gdf = gdf[~pd.Series(coastal_mask)].copy()
    print(f"🗺️  Difilter {np.sum(coastal_mask)} data di area pesisir")
    print(f"🗺️  Sisa {len(filtered_gdf)} data setelah filter pesisir")
    
    return filtered_gdf

# ==========================================
# 5. PROSES DOWNLOAD DATA KOMPREHENSIF
# ==========================================

print("🚀 Memulai download data F&B di seluruh Karawang (tanpa pesisir)...")

# Download data dari area utama
main_areas = [karawang_bbox_main] + additional_areas
gdf_main = download_osm_data(main_areas, "utama")

if gdf_main.empty:
    print("❌ Gagal mendapatkan data utama, mencoba metode alternatif...")
    
    # Fallback: Download berdasarkan titik-titik strategis
    strategic_points = [
        (-6.30, 107.30, 12000),  # Karawang Kota, radius 12km
        (-6.35, 107.25, 10000),  # Telukjambe, radius 10km
        (-6.40, 107.20, 8000),   # Klari, radius 8km
        (-6.25, 107.35, 8000),   # Karawang Timur, radius 8km
        (-6.45, 107.15, 8000),   # Ciampel, radius 8km
        (-6.50, 107.10, 8000),   # Cikampek, radius 8km
    ]
    
    all_point_data = []
    for lat, lon, dist in strategic_points:
        try:
            print(f"📍 Download dari titik ({lat}, {lon}) radius {dist/1000}km...")
            gdf_point = ox.features_from_point((lat, lon), dist=dist, tags=target_tags)
            if not gdf_point.empty:
                all_point_data.append(gdf_point)
                print(f"   ✅ Berhasil: {len(gdf_point)} entitas")
        except Exception as e:
            print(f"   ❌ Gagal: {e}")
    
    if all_point_data:
        gdf_main = pd.concat(all_point_data, ignore_index=True)
        gdf_main = gdf_main.drop_duplicates(subset=['geometry'], keep='first')
    else:
        print("❌ Semua metode download gagal")
        exit()

print(f"✅ Total data awal: {len(gdf_main)} entitas")

# Filter out area pesisir
gdf_filtered = exclude_coastal_areas(gdf_main, coastal_exclude_bbox)

print(f"📊 Data setelah exclude pesisir: {len(gdf_filtered)} entitas")

# ==========================================
# 6. PEMBERSIHAN & FILTERING DATA F&B
# ==========================================

print("🧹 Sedang membersihkan dan memfilter data F&B...")

# A. Pilih kolom yang penting saja
desired_columns = ['name', 'amenity', 'shop', 'tourism', 'leisure', 'cuisine', 'diet', 
                   'addr:street', 'addr:city', 'addr:full', 'addr:postcode', 'geometry']
existing_columns = [col for col in desired_columns if col in gdf_filtered.columns]
df = gdf_filtered[existing_columns].copy()

# B. Gabungkan kategori menjadi satu kolom 'category'
df['category'] = df['amenity'].fillna(df['shop']).fillna(df.get('tourism', pd.Series([None]*len(df)))).fillna(df.get('leisure', pd.Series([None]*len(df))))

# C. Ambil Koordinat Latitude & Longitude
if not df.empty and 'geometry' in df.columns:
    try:
        # Convert to projected CRS untuk koordinat yang akurat
        df_projected = df.to_crs('EPSG:3857')
        df_projected['lat'] = df_projected.geometry.centroid.y
        df_projected['lon'] = df_projected.geometry.centroid.x
        # Convert back to lat/lon
        df_4326 = df_projected.to_crs('EPSG:4326')
        df['lat'] = df_4326.geometry.centroid.y
        df['lon'] = df_4326.geometry.centroid.x
    except Exception as e:
        print(f"⚠️ Warning dalam konversi koordinat: {e}")
        # Fallback
        df['lat'] = df.geometry.centroid.y
        df['lon'] = df.geometry.centroid.x

# D. Hapus data yang tidak punya Nama
df_clean = df.dropna(subset=['name']).copy()

print(f"📝 Data setelah cleaning: {len(df_clean)} lokasi")

# E. Filter F&B
def is_food_beverage(row):
    """Filter untuk memastikan ini bisnis F&B"""
    if pd.isna(row['name']):
        return False
        
    name = str(row['name']).lower()
    category = str(row['category']).lower() if pd.notna(row['category']) else ""
    
    # Auto-include berdasarkan kategori OSM
    if pd.notna(row.get('amenity')) and row['amenity'] in ['restaurant', 'cafe', 'fast_food', 'food_court', 'bar', 'pub', 'ice_cream']:
        return True
    
    if pd.notna(row.get('shop')) and row['shop'] in ['coffee', 'bakery', 'butcher', 'seafood', 'beverages', 'wine', 'cheese', 'deli', 'supermarket', 'convenience', 'grocery']:
        return True
    
    if pd.notna(row.get('tourism')) and row['tourism'] in ['hotel', 'guest_house', 'apartment', 'motel']:
        return True
    
    # Cek keyword di nama
    for keyword in f_b_keywords:
        if keyword in name:
            return True
    
    return False

# Terapkan filter F&B
if not df_clean.empty:
    f_b_mask = df_clean.apply(is_food_beverage, axis=1)
    df_f_b = df_clean[f_b_mask].copy()
else:
    df_f_b = pd.DataFrame()

print(f"📊 Setelah filtering F&B: {len(df_f_b)} dari {len(df_clean)} lokasi")

# ==========================================
# 7. TAMBAHAN: DOWNLOAD DATA SPESIFIK UNTUK AREA YANG KURANG
# ==========================================

print("🔍 Mencari data tambahan untuk area yang mungkin terlewat...")

try:
    # Area yang mungkin kurang ter-cover
    supplemental_areas = [
        [107.10, -6.55, 107.30, -6.40],  # Area barat daya
        [107.35, -6.45, 107.50, -6.30],  # Area tenggara
        [106.95, -6.35, 107.15, -6.20],  # Area barat laut (non-pesisir)
    ]
    
    gdf_supplement = download_osm_data(supplemental_areas, "supplemental")
    
    if not gdf_supplement.empty:
        # Filter out pesisir
        gdf_supplement = exclude_coastal_areas(gdf_supplement, coastal_exclude_bbox)
        
        # Process supplemental data
        supplemental_columns = [col for col in desired_columns if col in gdf_supplement.columns]
        df_supplement = gdf_supplement[supplemental_columns].copy()
        df_supplement['category'] = df_supplement['amenity'].fillna(df_supplement['shop']).fillna(df_supplement.get('tourism', pd.Series([None]*len(df_supplement))))
        
        # Get coordinates
        if not df_supplement.empty and 'geometry' in df_supplement.columns:
            try:
                df_supplement_projected = df_supplement.to_crs('EPSG:3857')
                df_supplement_projected['lat'] = df_supplement_projected.geometry.centroid.y
                df_supplement_projected['lon'] = df_supplement_projected.geometry.centroid.x
                df_supplement_4326 = df_supplement_projected.to_crs('EPSG:4326')
                df_supplement['lat'] = df_supplement_4326.geometry.centroid.y
                df_supplement['lon'] = df_supplement_4326.geometry.centroid.x
            except:
                df_supplement['lat'] = df_supplement.geometry.centroid.y
                df_supplement['lon'] = df_supplement.geometry.centroid.x
        
        # Clean and filter
        df_supplement = df_supplement.dropna(subset=['name']).copy()
        supplement_f_b_mask = df_supplement.apply(is_food_beverage, axis=1)
        df_supplement_f_b = df_supplement[supplement_f_b_mask].copy()
        
        # Remove duplicates
        if not df_f_b.empty and not df_supplement_f_b.empty:
            existing_names = set(df_f_b['name'].astype(str))
            df_supplement_f_b = df_supplement_f_b[~df_supplement_f_b['name'].astype(str).isin(existing_names)]
        
        # Combine
        if not df_supplement_f_b.empty:
            df_f_b = pd.concat([df_f_b, df_supplement_f_b], ignore_index=True)
            print(f"✅ Ditambahkan {len(df_supplement_f_b)} data supplemental")
            
except Exception as e:
    print(f"⚠️ Tidak bisa mengambil data supplemental: {e}")

# ==========================================
# 8. PEMBERSIHAN FINAL & SIMPAN DATA
# ==========================================

print("🎯 Melakukan cleaning final...")

if not df_f_b.empty:
    # Hapus duplikat final
    df_f_b = df_f_b.drop_duplicates(subset=['name'], keep='first')
    
    # Pastikan semua kolom ada
    required_columns = ['name', 'category', 'cuisine', 'lat', 'lon', 'addr:street', 'addr:city']
    for col in required_columns:
        if col not in df_f_b.columns:
            df_f_b[col] = np.nan
    
    # Rapikan dataframe final
    final_df = df_f_b[required_columns].copy()
    final_df.columns = ['Nama_Tempat', 'Kategori', 'Jenis_Masakan', 'Latitude', 'Longitude', 'Alamat_Jalan', 'Kota']
    
    # Cleaning data
    final_df = final_df.replace([np.nan, None], '')
    final_df = final_df.fillna('')
    
    # Filter valid data
    final_df = final_df[final_df['Nama_Tempat'].str.strip() != '']
    final_df = final_df[final_df['Kategori'].str.strip() != '']
    
    # Simpan ke CSV
    csv_filename = os.path.join(output_dir, "data_fnb_karawang_comprehensive.csv")
    final_df.to_csv(csv_filename, index=False, encoding='utf-8')
    
    print("\n" + "="*70)
    print(f"🎉 SELESAI! Data F&B Karawang Comprehensive berhasil diambil.")
    print(f"📂 File tersimpan di: {csv_filename}")
    print(f"📊 Total Data F&B: {len(final_df)} lokasi")
    print(f"🗺️  Coverage: Seluruh Karawang (kecuali pesisir pantai)")
    print("="*70)
    
    # Tampilkan statistik
    print("\n📋 10 Data Pertama:")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    print(final_df.head(10))
    
    print("\n📈 Statistik Kategori:")
    category_stats = final_df['Kategori'].value_counts()
    print(category_stats)
    
    # Visualisasi dengan peta area coverage
    try:
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 8))
        
        # Plot 1: Peta coverage area
        for bbox in main_areas:
            min_lon, min_lat, max_lon, max_lat = bbox
            rect = plt.Rectangle((min_lon, min_lat), max_lon-min_lon, max_lat-min_lat,
                               fill=False, edgecolor='blue', linewidth=1, alpha=0.5, label='Coverage Area')
            ax1.add_patch(rect)
        
        # Plot area yang di-exclude
        for bbox in coastal_exclude_bbox:
            min_lon, min_lat, max_lon, max_lat = bbox
            rect = plt.Rectangle((min_lon, min_lat), max_lon-min_lon, max_lat-min_lat,
                               fill=True, edgecolor='red', facecolor='red', linewidth=1, alpha=0.3, label='Excluded Area')
            ax1.add_patch(rect)
        
        # Plot data points
        ax1.scatter(final_df['Longitude'], final_df['Latitude'], c='green', s=10, alpha=0.6, label='F&B Locations')
        ax1.set_xlabel('Longitude')
        ax1.set_ylabel('Latitude')
        ax1.set_title('Area Coverage Karawang (Red = Excluded Coastal Areas)')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # Plot 2: Sebaran data F&B
        categories = final_df['Kategori'].unique()
        colors = plt.cm.Set3(np.linspace(0, 1, len(categories)))
        
        for i, category in enumerate(categories):
            mask = final_df['Kategori'] == category
            ax2.scatter(final_df[mask]['Longitude'], final_df[mask]['Latitude'], 
                       c=[colors[i]], s=20, alpha=0.7, label=f"{category}")
        
        ax2.set_xlabel('Longitude')
        ax2.set_ylabel('Latitude')
        ax2.set_title(f'Sebaran {len(final_df)} Tempat F&B di Karawang')
        ax2.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
        ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        # Simpan peta
        map_filename = os.path.join(output_dir, "peta_coverage_fnb.png")
        plt.savefig(map_filename, dpi=300, bbox_inches='tight')
        print(f"\n📍 Peta coverage disimpan di: {map_filename}")
        
    except Exception as e:
        print(f"⚠️ Tidak bisa membuat peta: {e}")
    
else:
    print("❌ Tidak ada data F&B yang ditemukan")

print("\n✨ Proses selesai!")
print(f"📁 Data tersimpan di: {output_dir}")