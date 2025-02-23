import snowflake.connector
import pandas as pd
import numpy as np
from nba_api.stats.endpoints import leaguegamefinder, playbyplayv3
import time
import config

custom_headers = {
    'Host': 'stats.nba.com',
    'Connection': 'keep-alive',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://stats.nba.com'
}

def fetch_season_games(season_id='2024-25'):
    game_finder = leaguegamefinder.LeagueGameFinder(
        season_nullable=season_id,
        league_id_nullable='00',
        season_type_nullable='Regular Season',
        headers=custom_headers
    )
    games_df = game_finder.get_data_frames()[0]
    games_df = games_df[~games_df['MATCHUP'].str.contains('@')]
    
    # Sort by date so that earlier games are processed first
    games_df['GAME_DATE'] = pd.to_datetime(games_df['GAME_DATE'])
    games_df = games_df.sort_values(by='GAME_DATE', ascending=True).reset_index(drop=True)
    
    # Build a DataFrame with the essential game-level metadata
    return pd.DataFrame({
        'Game_ID': games_df['GAME_ID'],
        'Home': games_df['MATCHUP'].str[:3],
        'Away': games_df['MATCHUP'].str[-3:],
        'Home_Result': games_df['WL'],
        'Date': games_df['GAME_DATE']
    })

def compute_records(games_df):
    # Compute pre-game records using partner's logic
    # Initialize a dictionary to track each team's record (wins, losses)
    records = {}
    home_records = []
    away_records = []
    for idx, row in games_df.iterrows():
        home_team = row['Home']
        away_team = row['Away']
        # Get current record (default to [0, 0] if not played before)
        home_rec = records.get(home_team, [0, 0])
        away_rec = records.get(away_team, [0, 0])
        # Record the record BEFORE the game
        home_records.append(f"{home_rec[0]}-{home_rec[1]}")
        away_records.append(f"{away_rec[0]}-{away_rec[1]}")
        # Update records based on result; assume 'W' means home win, 'L' means home loss.
        if row['Home_Result'] == 'W':
            records[home_team] = [home_rec[0] + 1, home_rec[1]]
            records[away_team] = [away_rec[0], away_rec[1] + 1]
        elif row['Home_Result'] == 'L':
            records[home_team] = [home_rec[0], home_rec[1] + 1]
            records[away_team] = [away_rec[0] + 1, away_rec[1]]
        else:
            # No update if result is missing or a tie.
            records[home_team] = home_rec
            records[away_team] = away_rec

    games_df['Home_Record'] = home_records
    games_df['Away_Record'] = away_records
    # Compute win percentages (defaulting to 0 if no games played)
    games_df['Home_win_pct'] = games_df['Home_Record'].apply(
        lambda rec: int(rec.split('-')[0]) / (int(rec.split('-')[0]) + int(rec.split('-')[1])) 
                    if (int(rec.split('-')[0]) + int(rec.split('-')[1])) > 0 else 0
    )
    games_df['Away_win_pct'] = games_df['Away_Record'].apply(
        lambda rec: int(rec.split('-')[0]) / (int(rec.split('-')[0]) + int(rec.split('-')[1])) 
                    if (int(rec.split('-')[0]) + int(rec.split('-')[1])) > 0 else 0
    )
    return games_df

def process_play_by_play(game_id, home, away, home_result):
    pbp = playbyplayv3.PlayByPlayV3(game_id=game_id, headers=custom_headers)
    pbp_df = pbp.get_data_frames()[0]
    # Append game-level metadata to each play-by-play row
    pbp_df['Game_ID'] = game_id
    pbp_df['Home'] = home
    pbp_df['Away'] = away
    pbp_df['Home_Result'] = home_result
    return pbp_df

def create_table_and_ingest_data(conn, df):
    cs = conn.cursor()
    # Create table with all columns as VARCHAR to avoid type issues
    columns = df.columns.tolist()
    col_defs = ",\n".join([f"{col} VARCHAR" for col in columns])
    create_table_sql = f"CREATE TABLE IF NOT EXISTS NBA_RAW_DATA (\n{col_defs}\n)"
    cs.execute(create_table_sql)
    
    csv_file = "nba_data.csv"
    df.to_csv(csv_file, index=False)
    
    put_sql = f"PUT file://{csv_file} @%NBA_RAW_DATA"
    cs.execute(put_sql)
    
    copy_sql = """
    COPY INTO NBA_RAW_DATA
    FROM @%NBA_RAW_DATA/nba_data.csv
    FILE_FORMAT = (TYPE = 'CSV', FIELD_DELIMITER = ',', SKIP_HEADER = 1)
    ON_ERROR = 'CONTINUE'
    """
    cs.execute(copy_sql)
    
    conn.commit()
    cs.close()

def main():
    conn = snowflake.connector.connect(
        user=config.SNOWFLAKE_USER,
        password=config.SNOWFLAKE_PASSWORD,
        account=config.SNOWFLAKE_ACCOUNT,
        role=config.SNOWFLAKE_ROLE,
        warehouse=config.SNOWFLAKE_WAREHOUSE,
        database=config.SNOWFLAKE_DATABASE,
        schema=config.SNOWFLAKE_SCHEMA
    )
    
    # Fetch season games with game-level metadata
    games_df = fetch_season_games()
    # Compute pre-game records (Home_Record, Away_Record, win percentages)
    games_df = compute_records(games_df)
    
    all_games = []
    for idx, row in games_df.iterrows():
        # test first 5 games
        try:
            game_pbp = process_play_by_play(row['Game_ID'], row['Home'], row['Away'], row['Home_Result'])
            # Merge the game record columns into the play-by-play DataFrame
            for col in ['Home_win_pct', 'Away_win_pct']:
                game_pbp[col] = row[col]
            all_games.append(game_pbp)
            print(f"Processed game {idx + 1} with Game_ID: {row['Game_ID']}")
        except Exception as e:
            print(f"Error processing game {row['Game_ID']}: {e}")
        time.sleep(1)  # Respect API rate limits
    
    if all_games:
        combined_df = pd.concat(all_games, ignore_index=True)
        create_table_and_ingest_data(conn, combined_df)
        print(f"Ingested {len(combined_df)} rows for {len(all_games)} games")
    
    conn.close()

if __name__ == "__main__":
    main()
