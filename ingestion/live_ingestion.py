import snowflake.connector
import pandas as pd
import numpy as np
import re
from nba_api.stats.endpoints import leaguegamefinder
from nba_api.live.nba.endpoints import playbyplay
import time
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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
    # Filter out away games if needed
    games_df = games_df[~games_df['MATCHUP'].str.contains('@')]
    
    # For live games, assume that Home_Result is not yet set (or is None)
    live_games = games_df[games_df['WL'].isnull()]
    
    # Add default win percentages (since live games don't have results yet)
    live_games['Home_win_pct'] = 0
    live_games['Away_win_pct'] = 0
    
    # Return essential game-level metadata
    return pd.DataFrame({
        'Game_ID': live_games['GAME_ID'],
        'Home': live_games['MATCHUP'].str[:3],
        'Away': live_games['MATCHUP'].str[-3:],
        'Home_Result': live_games['WL'],  # Likely None
        'Home_win_pct': live_games['Home_win_pct'],
        'Away_win_pct': live_games['Away_win_pct']
    })

def fetch_live_pbp(game):
    # Use the live play-by-play endpoint for a given game.
    live_id = game['Game_ID']
    live = playbyplay.PlayByPlay(game_id=live_id)
    data = live.get_dict()
    plays = data['game']['actions'].copy()
    rows = []
    
    for play in plays:
        scoreHome = play['scoreHome']
        scoreAway = play['scoreAway']
        clock = play['clock']
        
        # Convert clock from format "PTMMMSss.SS" to "MM:ss.SS"
        pattern = r"^PT(\d{2})M(\d{2}\.\d{2})S$"
        clock = re.sub(pattern, r"\1:\2", clock)
        # Calculate minutes and seconds left
        minutes = float(clock.split(':')[0])
        seconds = float(clock.split(':')[1])
        minutes_left = minutes + (seconds / 60)
        period = play['period']
        # Assume standard period length of 12 minutes for regulation
        time_left = minutes_left + max(4 - period, 0) * 12
        row = {
            'scoreHome': scoreHome,
            'scoreAway': scoreAway,
            'clock': clock,
            'period': period,
            'time_left': time_left
        }
        rows.append(row)
    
    df = pd.DataFrame(rows)
    return df

def create_table_and_ingest_data(conn, df):
    cs = conn.cursor()
    # Create table with all columns as VARCHAR to avoid type issues.
    columns = df.columns.tolist()
    col_defs = ",\n".join([f"{col} VARCHAR" for col in columns])
    create_table_sql = f"CREATE TABLE IF NOT EXISTS NBA_LIVE_DATA (\n{col_defs}\n)"
    cs.execute(create_table_sql)
    
    csv_file = "nba_live_data.csv"
    df.to_csv(csv_file, index=False)
    
    put_sql = f"PUT file://{csv_file} @%NBA_LIVE_DATA"
    cs.execute(put_sql)
    
    copy_sql = """
    COPY INTO NBA_LIVE_DATA
    FROM @%NBA_LIVE_DATA/nba_live_data.csv
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
    
    # Fetch live games with game-level metadata
    games_df = fetch_season_games()
    all_games = []
    
    for idx, row in games_df.iterrows():
        try:
            live_pbp = fetch_live_pbp(row)
            # Merge the game-level metadata into each play-by-play row
            for col in ['Home_win_pct', 'Away_win_pct']:
                live_pbp[col] = row[col]
            # Also attach Game_ID, Home, Away, and Home_Result
            live_pbp['Game_ID'] = row['Game_ID']
            live_pbp['Home'] = row['Home']
            live_pbp['Away'] = row['Away']
            live_pbp['Home_Result'] = row['Home_Result']
            all_games.append(live_pbp)
            print(f"Processed live game {idx + 1} with Game_ID: {row['Game_ID']}")
        except Exception as e:
            print(f"Error processing game {row['Game_ID']}: {e}")
        time.sleep(1)  # Respect API rate limits
    
    if all_games:
        combined_df = pd.concat(all_games, ignore_index=True)
        create_table_and_ingest_data(conn, combined_df)
        print(f"Ingested {len(combined_df)} rows for {len(all_games)} live games")
    
    conn.close()

if __name__ == "__main__":
    main()
