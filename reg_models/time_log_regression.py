import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import snowflake.connector
import config

def run_regression_model():
    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=config.SNOWFLAKE_USER,
        password=config.SNOWFLAKE_PASSWORD,
        account=config.SNOWFLAKE_ACCOUNT,
        role=config.SNOWFLAKE_ROLE,
        warehouse=config.SNOWFLAKE_WAREHOUSE,
        database=config.SNOWFLAKE_DATABASE,
        schema=config.SNOWFLAKE_SCHEMA
    )
    
    # Pull all data from the live_timed_data table (already built by dbt)
    df = pd.read_sql("SELECT * FROM live_timed_data", conn)
    conn.close()
    
    # Create a new column for score_diff
    df['score_diff'] = df['SCOREHOME'] - df['SCOREAWAY']
    
    # For demonstration, process one game (assuming GAME_ID identifies a game)
    selected_game_id = df['GAME_ID'].iloc[0]
    selected_game = df[df['GAME_ID'] == selected_game_id].copy()
    
    # Calculate pre-game win percentage for home using provided coefficients
    intercept_pg = -0.026670487364573996
    coef_home_pg = 4.129212
    coef_away_pg = -3.643242
    linear_pred_pg = intercept_pg + coef_home_pg * selected_game['Home_win_pct'] + coef_away_pg * selected_game['Away_win_pct']
    selected_game['PG_Home_W_pct'] = 1 / (1 + np.exp(-linear_pred_pg))
    
    # Calculate live win probability using regression coefficients
    intercept_live = -1.029508469093677
    coef_score_diff_live = 0.148322
    coef_time_left_live = 0.001460
    coef_PG_Home_W_pct_live = 2.145808
    linear_pred_live = (intercept_live +
                        coef_score_diff_live * selected_game['score_diff'] +
                        coef_time_left_live * selected_game['time_left'] +
                        coef_PG_Home_W_pct_live * selected_game['PG_Home_W_pct'])
    selected_game['predicted_win_prob'] = 1 / (1 + np.exp(-linear_pred_live))
    
    # Sample data for plotting (e.g., every 15 rows)
    selected_game_sampled = selected_game.iloc[::15, :]
    home_team = selected_game['Home'].unique()[0]
    away_team = selected_game['Away'].unique()[0]
    
    # Plot predicted win probability vs. time left
    plt.figure(figsize=(10, 6))
    plt.plot(selected_game_sampled['time_left'], selected_game_sampled['predicted_win_prob'], 
             marker='o', linestyle='-', color='black')
    plt.xlabel("Time Left (minutes)")
    plt.ylabel("Predicted Win Probability")
    plt.title(f"Predicted Win Probability vs Time Left for {home_team} vs {away_team}")
    plt.axhline(y=0.5, color='r', linestyle='--', linewidth=1.5)
    plt.axhspan(0, 0.5, facecolor='purple', alpha=0.2)
    plt.axhspan(0.5, 1, facecolor='yellow', alpha=0.2)
    plt.text(0.05, 0.1, f'{home_team} Favored', transform=plt.gca().transAxes, fontsize=12, color='blue')
    plt.text(0.05, 0.9, f'{away_team} Favored', transform=plt.gca().transAxes, fontsize=12, color='red')
    plt.ylim(0, 1)
    plt.xlim(0, 48)
    plt.gca().invert_xaxis()
    plt.gca().invert_yaxis()
    plt.grid(True)
    
    # Save the plot as an image file
    plot_path = f"/tmp/{selected_game_id}_win_prob_plot.png"
    plt.savefig(plot_path)
    plt.close()
    print(f"Regression model run complete for GAME_ID: {selected_game_id}. Plot saved at {plot_path}")
