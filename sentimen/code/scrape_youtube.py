import os
import pandas as pd
from googleapiclient.discovery import build

# ==========================================
# 1. KONFIGURASI KHUSUS LAGA 15 APRIL 2017
# ==========================================
# ⚠️ PASTE API KEY ANDA DI BAWAH INI JANGAN SAMPAI KOSONG
API_KEY = "AIzaSyA2H6sLFkITW7ieA7M8Oj_nz_m3vuBKfwc" 

# Keyword Spesifik Laga Tersebut
# Kita pakai tahun 2017 agar mesin pencari YouTube memberikan video lama
SEARCH_QUERY = "Persib vs Arema 15 April 2017" 

MAX_VIDEOS = 10 
MAX_COMMENTS_PER_VIDEO = 50

SAVE_DIR = "/home/mrsnow/dev/python/skripsi/skripsi/sentimen/data"

# ==========================================
# 2. FUNGSI PENCARIAN (MODIFIKASI)
# ==========================================

def get_video_ids(youtube, query, max_results):
    print(f"🔎 Mencari video lawas dengan keyword: '{query}'...")
    
    # Kita gunakan order='relevance' agar video yang judulnya paling mirip 
    # (Laga 2017) muncul di atas, bukan video terbaru 2024.
    request = youtube.search().list(
        part="snippet",
        q=query,
        type="video",
        maxResults=max_results,
        order="relevance", 
        regionCode="ID"
    )
    response = request.execute()
    
    video_data = []
    for item in response['items']:
        video_id = item['id']['videoId']
        title = item['snippet']['title']
        publish_time = item['snippet']['publishedAt']
        channel = item['snippet']['channelTitle']
        
        # Filter Opsional: Hanya ambil video yang rilis tahun 2017/2018 
        # agar komentarnya murni suasana saat itu (opsional, saya matikan dulu agar hasil banyak)
        # if "2017" in publish_time: 
        
        video_data.append({
            'video_id': video_id,
            'video_title': title,
            'video_date': publish_time,
            'channel': channel
        })
        print(f"   Found: {title[:50]}... ({publish_time[:10]})")
        
    print(f"✅ Ditemukan {len(video_data)} video relevan.")
    return video_data

def get_comments(youtube, video_id, video_title, max_comments):
    comments_data = []
    try:
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=max_comments, # Request langsung sesuai jumlah diminta
            textFormat="plainText",
            order="relevance" # Ambil komentar populer dulu
        )
        
        response = request.execute()
        
        for item in response['items']:
            comment = item['snippet']['topLevelComment']['snippet']
            text = comment['textDisplay']
            author = comment['authorDisplayName']
            date = comment['publishedAt']
            likes = comment['likeCount']
            
            comments_data.append({
                'source': 'youtube',
                'match_context': 'Persib vs Arema 2017', # Penanda Laga
                'video_title': video_title,
                'date': date,
                'username': author,
                'text': text,
                'likes': likes
            })
            if len(comments_data) >= max_comments: break
                
    except Exception as e:
        print(f"⚠️ Skip video: Komentar dimatikan/Error.")
        
    return comments_data

# ==========================================
# 3. EKSEKUSI
# ==========================================
if __name__ == "__main__":
    if not os.path.exists(SAVE_DIR):
        os.makedirs(SAVE_DIR)
    
    # Cek API KEY
    if "MASUKKAN" in API_KEY:
        print("❌ ERROR: Anda belum memasukkan API KEY di baris 9!")
        exit()

    try:
        youtube = build('youtube', 'v3', developerKey=API_KEY)
        
        # 1. Cari Video
        videos = get_video_ids(youtube, SEARCH_QUERY, MAX_VIDEOS)
        
        all_comments = []
        
        # 2. Ambil Komentar
        print(f"\n🚀 Mengambil komentar (Target: {MAX_VIDEOS} video x {MAX_COMMENTS_PER_VIDEO} komentar)...")
        
        for i, vid in enumerate(videos):
            print(f"[{i+1}/{len(videos)}] Processing: {vid['video_title'][:40]}...")
            comments = get_comments(youtube, vid['video_id'], vid['video_title'], MAX_COMMENTS_PER_VIDEO)
            all_comments.extend(comments)
            
        # 3. Simpan
        if all_comments:
            df = pd.DataFrame(all_comments)
            
            # Bersihkan enter/newline
            df['text'] = df['text'].astype(str).str.replace('\n', ' ').str.replace('\r', '')
            
            filename = f"youtube_persib_arema_2017.csv"
            full_path = os.path.join(SAVE_DIR, filename)
            
            df.to_csv(full_path, index=False)
            print("\n" + "="*40)
            print(f"✅ SELESAI! Total {len(df)} komentar berhasil diambil.")
            print(f"📂 File tersimpan di: {full_path}")
            print("="*40)
            print(df[['username', 'text', 'likes']].head())
        else:
            print("❌ Tidak ada data komentar yang berhasil diambil.")
            
    except Exception as e:
        print(f"❌ Terjadi Error: {e}")