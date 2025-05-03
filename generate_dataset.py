import pandas as pd
import random
from datetime import datetime, timedelta

def generate_song_metadata(num_songs=100):
    """
    Generate a DataFrame with random song metadata.
    
    Columns:
      - song_id: Unique song identifier, e.g., 'song_1'
      - title: Song title, e.g., 'Song 1'
      - artist: Randomly selected artist name
      - genre: Randomly selected genre from a list
      - mood: Randomly selected mood from a list
    """
    song_ids = [f"song_{i+1}" for i in range(num_songs)]
    titles = [f"Song {i+1}" for i in range(num_songs)]
    
    # Predefined lists for random selection
    artists_list = ["Artist A", "Artist B", "Artist C", "Artist D", "Artist E"]
    genres_list = ["Pop", "Rock", "Jazz", "Hip-Hop", "Classical"]
    moods_list = ["Happy", "Sad", "Energetic", "Chill"]
    
    artists = [random.choice(artists_list) for _ in range(num_songs)]
    genres = [random.choice(genres_list) for _ in range(num_songs)]
    moods = [random.choice(moods_list) for _ in range(num_songs)]
    
    data = {
        "song_id": song_ids,
        "title": titles,
        "artist": artists,
        "genre": genres,
        "mood": moods
    }
    return pd.DataFrame(data)

def generate_listening_logs(num_logs=2000, num_users=50, songs_df=None):
    """
    Generate a DataFrame with random user listening logs.
    
    For a subset of users, we introduce a bias so that:
      - Users whose number is divisible by 7 are "Sad heavy." With 80% probability, they listen to a "Sad" song.
      - Users whose number is divisible by 5 are "loyal" to a favorite genre (randomly chosen for each such user). With 85% probability, they listen to a song from that genre.
    
    The timestamp is generated within a range from (today - 30 days) to (today + 30 days) so that 
    several records fall into the current week (for the "Top 10 Most Played Songs This Week" query).
    
    Columns:
      - user_id: Unique user identifier (e.g., 'user_1')
      - song_id: Unique song identifier chosen from songs_df
      - timestamp: Random timestamp between (now - 30 days) and (now + 30 days)
      - duration_sec: Random duration (in seconds) between 30 and 300
    """
    if songs_df is None:
        raise ValueError("A songs_df DataFrame must be provided")
        
    songs = songs_df.to_dict(orient="records")
    user_ids = [f"user_{i+1}" for i in range(num_users)]
    logs = []
    
    # Use a date range that spans 30 days back and 30 days forward from now.
    now = datetime.now()
    start_date = now - timedelta(days=30)
    end_date = now + timedelta(days=30)
    total_seconds = int((end_date - start_date).total_seconds())
    
    # Pre-select favorite genres for loyal users (users with id % 5 == 0).
    loyal_users = {}
    for i, user in enumerate(user_ids):
        if (i + 1) % 5 == 0:
            loyal_users[user] = random.choice(["Pop", "Rock", "Jazz", "Hip-Hop", "Classical"])
    
    for _ in range(num_logs):
        user_id = random.choice(user_ids)
        song = None
        
        # If user is "loyal" (divisible by 5), with probability 0.85 choose a song from their favorite genre.
        if user_id in loyal_users and random.random() < 0.85:
            fav_genre = loyal_users[user_id]
            genre_songs = [s for s in songs if s["genre"] == fav_genre]
            if genre_songs:
                song = random.choice(genre_songs)
                
        # If not chosen yet, and if user is "Sad heavy" (divisible by 7), with probability 0.80 choose a "Sad" song.
        if song is None:
            user_num = int(user_id.split("_")[1])
            if user_num % 7 == 0 and random.random() < 0.80:
                sad_songs = [s for s in songs if s["mood"] == "Sad"]
                if sad_songs:
                    song = random.choice(sad_songs)
                    
        # If still not chosen, select a random song.
        if song is None:
            song = random.choice(songs)
        
        # Generate random timestamp within the defined date range.
        rand_seconds = random.randint(0, total_seconds)
        timestamp = start_date + timedelta(seconds=rand_seconds)
        timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
        
        # Random play duration between 30 and 300 seconds.
        duration_sec = random.randint(30, 300)
        
        logs.append({
            "user_id": user_id,
            "song_id": song["song_id"],
            "timestamp": timestamp_str,
            "duration_sec": duration_sec
        })
        
    return pd.DataFrame(logs)

def main():
    # Generate songs metadata
    songs_df = generate_song_metadata(num_songs=100)
    songs_df.to_csv("songs_metadata.csv", index=False)
    
    # Generate listening logs using the songs metadata
    logs_df = generate_listening_logs(num_logs=2000, num_users=50, songs_df=songs_df)
    logs_df.to_csv("listening_logs.csv", index=False)

if __name__ == "__main__":
    main()