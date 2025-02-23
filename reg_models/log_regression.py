#imports 
import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report

def pregame_win_prob(games_df):
    X = games_df[['Home_win_pct', 'Away_win_pct']]
    y = games_df['win']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.05)

    model = LogisticRegression(max_iter=1000)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1]

    print("Accuracy:", accuracy_score(y_test, y_pred))
    print("Classification Report:")
    print(classification_report(y_test, y_pred))

    coefficients = model.coef_[0]
    features = X.columns

    coef_df = pd.DataFrame({
        'Feature': features,
        'Coefficient': coefficients})
    return coef_df, model.intercept_[0]

def live_win_prob(games_df):
    preg_df, preg_intercept = pregame_win_prob(games_df)
    w_pct_home_coef = coef_df.loc[coef_df['Feature'] == 'W_PCT_home', 'Coefficient'].values[0]
    w_pct_away_coef = coef_df.loc[coef_df['Feature'] == 'W_PCT_away', 'Coefficient'].values[0]

    linear_pred = preg_intercept + w_pct_home_coef * games_df['Home_win_pct'] + w_pct_away_coef * games_df['Away_win_pct']

    games_df['PG_Home_W_pct'] = 1 / (1 + np.exp(-linear_pred))

    features = ['score_diff', 'time_left', 'PG_Home_W_pct']
    X = games_df[features]
    y = games_df['win']    

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.1, random_state=42)

    model = LogisticRegression()
    model.fit(X_train, y_train)

    y_pred_prob = model.predict_proba(X_test)[:, 1]

    y_pred = model.predict(X_test)

    accuracy = accuracy_score(y_test, y_pred)
    print("Accuracy:", accuracy)

    print(classification_report(y_test, y_pred))

    coefficients = model.coef_[0]
    intercept = model.intercept_[0]

    coef_df = pd.DataFrame({
        'Feature': features,
        'Coefficient': coefficients})

    return coef_df, intercept

