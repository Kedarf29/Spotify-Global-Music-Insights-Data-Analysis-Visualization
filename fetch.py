import pandas as pd
import time
from tqdm import tqdm
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

# ---------------------------------------
# 1. Spotify API Setup
# ---------------------------------------
client_id = '4636801f83644c2aa5f6e6735a3f78ff'
client_secret = '513cb7bf330f43759b47a2322918de9f'

auth_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
sp = spotipy.Spotify(auth_manager=auth_manager)

# ---------------------------------------
# 2. Load Dataset Safely
# ---------------------------------------
with open('spotify-2023.csv', encoding='ISO-8859-1') as f:
    df = pd.read_csv(f)
    df.columns = df.columns.str.strip()  # Remove extra spaces in column names

# Show sample columns
print("📄 Columns in CSV:", df.columns.tolist())

# Rename for consistency
df.rename(columns={'artist(s)_name': 'artist_name'}, inplace=True)

# ---------------------------------------
# 3. Function to Search and Extract Cover URL
# ---------------------------------------
def get_album_cover_url(track_name, artist):
    try:
        query = f'track:{track_name} artist:{artist}'
        result = sp.search(q=query, type='track', limit=1)
        items = result.get('tracks', {}).get('items', [])
        if items:
            return items[0]['album']['images'][0]['url']
    except Exception as e:
        print(f"❌ Error for '{track_name}' by '{artist}': {e}")
    return None

# ---------------------------------------
# 4. Caching API Results
# ---------------------------------------
tqdm.pandas()
track_url_map = {}

unique_tracks = df[['track_name', 'artist_name']].drop_duplicates()

for _, row in tqdm(unique_tracks.iterrows(), total=len(unique_tracks), desc="🔍 Fetching Album URLs"):
    track = row['track_name']
    artist = row['artist_name']
    key = (track, artist)
    if key not in track_url_map:
        url = get_album_cover_url(track, artist)
        track_url_map[key] = url
        time.sleep(0.1)  # Delay to respect API rate limits

# ---------------------------------------
# 5. Apply URLs Back to Original DF
# ---------------------------------------
df['Cover_URL'] = df.apply(lambda row: track_url_map.get((row['track_name'], row['artist_name']), None), axis=1)

# ---------------------------------------
# 6. Save Final CSV
# ---------------------------------------
df.to_csv('spotify-2023-with-cover-urls.csv', index=False)
print("✅ Done! Saved as 'spotify-2023-with-cover-urls.csv'")
