import pandas as pd
from googleapiclient.discovery import build
from tqdm import tqdm
import os
import re
import time
import logging
from datetime import datetime
import sys

# ==========================================
# 1. KONFIGURASI PENGAMBILAN DATA & LOGGING
# ==========================================

# MASUKKAN API KEY ANDA
API_KEY = "AIzaSyA2H6sLFkITW7ieA7M8Oj_nz_m3vuBKfwc"

# === KONFIGURASI LIMIT YANG DITINGKATKAN ===
MAX_VIDEOS_PER_KEYWORD = 30   # Naik dari 10 ke 30 video per kata kunci
MAX_COMMENTS_PER_VIDEO = 50   # Naik dari 15 ke 50 komentar per video

# === DAFTAR KEYWORD LENGKAP & TERSTRUKTUR ===
KEYWORDS = [
    # 1. Kategori Umum & Viral
    "kuliner karawang viral",
    "jajanan karawang hits",
    "rekomendasi makanan karawang",
    "wisata kuliner karawang",
    "tempat nongkrong karawang",
    "hidden gem karawang",
    "street food karawang",
    "makanan pedas karawang",
    
    # 2. Kategori Spesifik (Jenis Makanan Populer)
    "bakso enak di karawang",
    "mie ayam karawang legendaris",
    "seblak karawang viral",
    "sate maranggi karawang",
    "seafood karawang murah",
    "nasgor karawang enak",
    "bubur ayam karawang",
    "soto tangkar karawang",
    "durian karawang",
    
    # 3. Kategori Cafe & Resto (Tempat)
    "cafe aesthetic karawang",
    "coffee shop karawang",
    "restoran keluarga karawang",
    "tempat makan romantis karawang",
    "cafe instagramable karawang",
    "resto sunda karawang",
    
    # 4. Berdasarkan Lokasi Sentral
    "kuliner galuh mas karawang",
    "kuliner karawang barat",
    "kuliner cikampek",
    "kuliner perumnas karawang",
    "kuliner tuparev karawang",
    "jajanan gor panatayuda karawang",
    
    # 5. Tipe Konten Reviewer
    "mukbang karawang",
    "review jujur makanan karawang",
    "vlog kuliner karawang"
]

# === KONFIGURASI PATH & LOGGING ===
BASE_DIR = "/home/mrsnow/dev/python/skripsi/skripsi/sentimen/data/yt"
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Buat direktori jika belum ada
os.makedirs(BASE_DIR, exist_ok=True)

# File paths
OUTPUT_FILE = os.path.join(BASE_DIR, f"dataset_youtube_fnb_karawang_MASSIVE_{timestamp}.csv")
BACKUP_FILE = os.path.join(BASE_DIR, f"backup_progress_{timestamp}.csv")
LOG_FILE = os.path.join(BASE_DIR, f"youtube_scraping_log_{timestamp}.txt")
SUMMARY_REPORT = os.path.join(BASE_DIR, f"scraping_summary_{timestamp}.txt")

# Setup logging komprehensif
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def write_summary_report(content):
    """Menulis laporan summary ke file terpisah"""
    with open(SUMMARY_REPORT, 'a', encoding='utf-8') as f:
        f.write(content + '\n')

# ==========================================
# 2. FUNGSI UTILITY & API DENGAN LOGGING
# ==========================================

def get_youtube_service(api_key):
    """Membuat service YouTube dengan error handling"""
    try:
        service = build('youtube', 'v3', developerKey=api_key)
        logger.info("✅ YouTube API service berhasil dibuat")
        return service
    except Exception as e:
        logger.error(f"❌ Gagal membuat YouTube service: {str(e)}")
        raise

def clean_text(text):
    """Membersihkan text agar rapi di CSV"""
    if not text: return ""
    text = str(text).replace('\n', ' ').replace('\r', ' ').replace(';', ',')
    return re.sub(' +', ' ', text).strip()

def get_video_comments(youtube, video_id, max_comments, video_title=""):
    """Mengambil komentar dengan paginasi dan logging"""
    comments_data = []
    try:
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=min(max_comments, 100),
            textFormat="plainText",
            order="relevance"
        )
        response = request.execute()

        comments_count = 0
        while response and comments_count < max_comments:
            for item in response['items']:
                comment = item['snippet']['topLevelComment']['snippet']
                comments_data.append({
                    'comment_text': clean_text(comment['textDisplay']),
                    'comment_author': comment['authorDisplayName'],
                    'comment_likes': comment['likeCount'],
                    'comment_date': comment['publishedAt']
                })
                comments_count += 1
                
                if comments_count >= max_comments:
                    break
            
            if comments_count >= max_comments:
                break
                
            if 'nextPageToken' in response:
                request = youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=min(max_comments - comments_count, 100),
                    textFormat="plainText",
                    order="relevance",
                    pageToken=response['nextPageToken']
                )
                response = request.execute()
            else:
                break
            
        logger.info(f"📝 Video '{video_title[:50]}...' - {len(comments_data)} komentar berhasil diambil")
            
    except Exception as e:
        error_msg = f"komentar dimatikan atau error"
        logger.warning(f"⚠️ Gagal mengambil komentar untuk video '{video_title[:30]}...': {error_msg}")
        
    return comments_data

def save_backup(data, keyword, video_count, comment_count):
    """Menyimpan backup progress"""
    try:
        backup_df = pd.DataFrame([{
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'keyword': keyword,
            'total_videos': video_count,
            'total_comments': comment_count,
            'data_points': len(data)
        }])
        
        if os.path.exists(BACKUP_FILE):
            existing_df = pd.read_csv(BACKUP_FILE)
            backup_df = pd.concat([existing_df, backup_df], ignore_index=True)
        
        backup_df.to_csv(BACKUP_FILE, index=False)
        logger.debug(f"💾 Backup progress disimpan untuk keyword: {keyword}")
    except Exception as e:
        logger.error(f"❌ Gagal menyimpan backup: {str(e)}")

def search_and_scrape_massive(youtube):
    """Fungsi utama scraping dengan logging komprehensif"""
    final_data = []
    seen_video_ids = set()
    
    # Statistics tracking
    stats = {
        'total_videos_processed': 0,
        'total_comments_collected': 0,
        'keywords_processed': 0,
        'keywords_failed': 0,
        'start_time': time.time()
    }

    # Tulis header laporan
    write_summary_report("=" * 70)
    write_summary_report("LAPORAN SCRAPING YOUTUBE DATA FNB KARAWANG")
    write_summary_report("=" * 70)
    write_summary_report(f"Tanggal Proses: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    write_summary_report(f"API Key: {'***' + API_KEY[-8:] if API_KEY != 'MASUKKAN_API_KEY_GOOGLE_ANDA_DISINI' else 'TIDAK VALID'}")
    write_summary_report(f"Max Video/Keyword: {MAX_VIDEOS_PER_KEYWORD}")
    write_summary_report(f"Max Komentar/Video: {MAX_COMMENTS_PER_VIDEO}")
    write_summary_report(f"Total Keywords: {len(KEYWORDS)}")
    write_summary_report("=" * 70)
    write_summary_report("\nPROGRES PER KEYWORD:")
    write_summary_report("-" * 50)

    logger.info("🚀 MEMULAI SCRAPING MASSIF YOUTUBE")
    logger.info(f"🔑 Total Keywords: {len(KEYWORDS)}")
    logger.info(f"🎥 Target Video/Keyword: {MAX_VIDEOS_PER_KEYWORD}")
    logger.info(f"🗣️ Target Komentar/Video: {MAX_COMMENTS_PER_VIDEO}")

    for keyword in tqdm(KEYWORDS, desc="Proses Keywords"):
        keyword_start_time = time.time()
        keyword_videos = 0
        keyword_comments = 0
        
        try:
            logger.info(f"🔍 Memproses keyword: '{keyword}'")
            
            # 1. SEARCH QUERY
            search_req = youtube.search().list(
                q=keyword,
                part='id',
                type='video',
                maxResults=MAX_VIDEOS_PER_KEYWORD,
                order='relevance',
                regionCode='ID'
            )
            search_res = search_req.execute()
            
            video_ids = [item['id']['videoId'] for item in search_res['items']]
            new_ids = [vid for vid in video_ids if vid not in seen_video_ids]
            
            if not new_ids:
                logger.info(f"⏭️ Semua video untuk '{keyword}' sudah diproses sebelumnya")
                stats['keywords_processed'] += 1
                continue
            
            logger.info(f"📹 Ditemukan {len(new_ids)} video baru untuk keyword '{keyword}'")
            
            # 2. AMBIL DETAIL VIDEO (Chunking per 50 ID)
            for i in range(0, len(new_ids), 50):
                chunk_ids = new_ids[i:i+50]
                
                stats_req = youtube.videos().list(
                    part="snippet,statistics",
                    id=','.join(chunk_ids)
                )
                stats_res = stats_req.execute()
                
                # 3. PROSES SETIAP VIDEO
                for item in stats_res['items']:
                    vid_id = item['id']
                    snippet = item['snippet']
                    stats_vid = item['statistics']
                    
                    seen_video_ids.add(vid_id)
                    stats['total_videos_processed'] += 1
                    keyword_videos += 1
                    
                    view_count = int(stats_vid.get('viewCount', 0))
                    comment_count = int(stats_vid.get('commentCount', 0))
                    
                    base_info = {
                        'video_id': vid_id,
                        'search_keyword': keyword,
                        'title': clean_text(snippet['title']),
                        'channel_name': clean_text(snippet['channelTitle']),
                        'publish_date': snippet['publishedAt'],
                        'view_count': view_count,
                        'like_count': int(stats_vid.get('likeCount', 0)),
                        'comment_count_total': comment_count,
                        'video_url': f"https://www.youtube.com/watch?v={vid_id}"
                    }
                    
                    # 4. AMBIL KOMENTAR
                    video_comments = []
                    if comment_count > 0:
                        time.sleep(0.1)  # Jeda untuk menghindari rate limit
                        video_comments = get_video_comments(
                            youtube, vid_id, MAX_COMMENTS_PER_VIDEO, 
                            base_info['title']
                        )
                        keyword_comments += len(video_comments)
                        stats['total_comments_collected'] += len(video_comments)
                    
                    if video_comments:
                        for com in video_comments:
                            row = base_info.copy()
                            row.update(com)
                            final_data.append(row)
                    else:
                        row = base_info.copy()
                        row.update({
                            'comment_text': '', 
                            'comment_author': '',
                            'comment_likes': 0,
                            'comment_date': ''
                        })
                        final_data.append(row)
            
            stats['keywords_processed'] += 1
            keyword_time = time.time() - keyword_start_time
            
            # Log hasil per keyword
            keyword_summary = f"✅ '{keyword}': {keyword_videos} video, {keyword_comments} komentar ({keyword_time:.1f}s)"
            logger.info(keyword_summary)
            write_summary_report(keyword_summary)
            
            # Backup progress
            save_backup(final_data, keyword, keyword_videos, keyword_comments)

        except Exception as e:
            error_msg = f"❌ Error processing keyword '{keyword}': {str(e)}"
            logger.error(error_msg)
            write_summary_report(f"❌ '{keyword}': GAGAL - {str(e)}")
            stats['keywords_failed'] += 1
            time.sleep(2)  # Jeda jika ada error
    
    return pd.DataFrame(final_data), stats

# ==========================================
# 3. EKSEKUSI UTAMA DENGAN ERROR HANDLING
# ==========================================

if __name__ == "__main__":
    if API_KEY == "MASUKKAN_API_KEY_GOOGLE_ANDA_DISINI":
        error_msg = "❌ Masukkan API Key Google YouTube Data API v3 terlebih dahulu!"
        logger.error(error_msg)
        write_summary_report(f"STATUS: GAGAL - {error_msg}")
        print(error_msg)
    else:
        try:
            # Inisialisasi
            logger.info("🔄 Menginisialisasi YouTube API service...")
            youtube = get_youtube_service(API_KEY)
            
            # Eksekusi scraping
            df, stats = search_and_scrape_massive(youtube)
            
            # Hitung waktu proses
            total_time = time.time() - stats['start_time']
            minutes = int(total_time // 60)
            seconds = int(total_time % 60)
            
            if not df.empty:
                # Post-Processing
                initial_count = len(df)
                df = df.drop_duplicates(subset=['video_id', 'comment_text'])
                duplicates_removed = initial_count - len(df)
                
                # Simpan hasil final
                df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
                
                # Tulis laporan akhir
                write_summary_report("\n" + "=" * 50)
                write_summary_report("HASIL AKHIR SCRAPING")
                write_summary_report("=" * 50)
                write_summary_report(f"Waktu proses: {minutes}m {seconds}s")
                write_summary_report(f"Keywords berhasil: {stats['keywords_processed'] - stats['keywords_failed']}")
                write_summary_report(f"Keywords gagal: {stats['keywords_failed']}")
                write_summary_report(f"Total video diproses: {stats['total_videos_processed']}")
                write_summary_report(f"Total komentar dikumpulkan: {stats['total_comments_collected']}")
                write_summary_report(f"Duplikat dihapus: {duplicates_removed}")
                write_summary_report(f"Data final: {len(df)} baris")
                write_summary_report(f"File output: {OUTPUT_FILE}")
                
                # Log final
                logger.info("\n" + "=" * 60)
                logger.info("✅ SCRAPING MASSIF SELESAI!")
                logger.info(f"⏱️  Waktu proses: {minutes}m {seconds}s")
                logger.info(f"📊 Statistik Final:")
                logger.info(f"   ✅ Keywords berhasil: {stats['keywords_processed'] - stats['keywords_failed']}")
                logger.info(f"   ❌ Keywords gagal: {stats['keywords_failed']}")
                logger.info(f"   🎥 Total video: {stats['total_videos_processed']}")
                logger.info(f"   🗣️  Total komentar: {stats['total_comments_collected']}")
                logger.info(f"   📝 Data points: {len(df)} baris")
                logger.info(f"   🗑️  Duplikat dihapus: {duplicates_removed}")
                
                logger.info(f"\n📂 File hasil: {OUTPUT_FILE}")
                logger.info(f"📋 Laporan: {SUMMARY_REPORT}")
                logger.info(f"📝 Log detail: {LOG_FILE}")
                logger.info("=" * 60)
                
                # Preview data
                logger.info("\n🔍 Preview 5 Data Teratas:")
                preview_df = df[['title', 'view_count', 'comment_text']].head()
                for idx, row in preview_df.iterrows():
                    logger.info(f"  📹 {row['title'][:50]}... | 👁️ {row['view_count']} | 💬 {row['comment_text'][:30]}...")
                
                print(f"\n🎉 PROSES SELESAI!")
                print(f"   💾 Data: {OUTPUT_FILE}")
                print(f"   📋 Laporan: {SUMMARY_REPORT}")
                print(f"   📝 Log: {LOG_FILE}")
                
            else:
                error_msg = "❌ Tidak ada data yang berhasil dikumpulkan"
                logger.error(error_msg)
                write_summary_report(f"STATUS: GAGAL - {error_msg}")
                print(error_msg)
                
        except Exception as e:
            error_msg = f"❌ Error utama: {str(e)}"
            logger.error(error_msg)
            write_summary_report(f"STATUS: GAGAL - {error_msg}")
            print(error_msg)
        
        finally:
            # Cleanup: Hapus file backup jika ada
            if os.path.exists(BACKUP_FILE):
                try:
                    os.remove(BACKUP_FILE)
                    logger.info("🗑️ File backup progress dihapus")
                except:
                    pass