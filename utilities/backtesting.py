import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import datetime
import numpy as np

def basic_single_asset_backtest(trades, days):
    df_trades = trades.copy()
    df_days = days.copy()
    
    df_days['evolution'] = df_days['wallet'].diff()
    df_days['daily_return'] = df_days['evolution']/df_days['wallet'].shift(1)
    
    df_trades['trade_result'] = df_trades["close_trade_size"] - df_trades["open_trade_size"] - df_trades["open_fee"]
    df_trades['trade_result_pct'] = df_trades['trade_result']/df_trades["open_trade_size"]
    df_trades['trade_result_pct_wallet'] = df_trades['trade_result']/(df_trades["wallet"]+df_trades["trade_result"])
    
    df_trades['wallet_ath'] = df_trades['wallet'].cummax()
    df_trades['drawdown'] = df_trades['wallet_ath'] - df_trades['wallet']
    df_trades['drawdown_pct'] = df_trades['drawdown'] / df_trades['wallet_ath']
    df_days['wallet_ath'] = df_days['wallet'].cummax()
    df_days['drawdown'] = df_days['wallet_ath'] - df_days['wallet']
    df_days['drawdown_pct'] = df_days['drawdown'] / df_days['wallet_ath']
    
    good_trades = df_trades.loc[df_trades['trade_result'] > 0]
    
    initial_wallet = df_days.iloc[0]["wallet"]
    total_trades = len(df_trades)
    total_good_trades = len(good_trades)
    avg_profit = df_trades['trade_result_pct'].mean()   
    global_win_rate = total_good_trades / total_trades
    max_trades_drawdown = df_trades['drawdown_pct'].max()
    max_days_drawdown = df_days['drawdown_pct'].max()
    final_wallet = df_days.iloc[-1]['wallet']
    buy_and_hold_pct = (df_days.iloc[-1]['price'] - df_days.iloc[0]['price']) / df_days.iloc[0]['price']
    buy_and_hold_wallet = initial_wallet + initial_wallet * buy_and_hold_pct
    vs_hold_pct = (final_wallet - buy_and_hold_wallet)/buy_and_hold_wallet
    vs_usd_pct = (final_wallet - initial_wallet)/initial_wallet
    sharpe_ratio = (365**0.5)*(df_days['daily_return'].mean()/df_days['daily_return'].std())
    total_fees = df_trades['open_fee'].sum() + df_trades['close_fee'].sum()
    
    best_trade = df_trades['trade_result_pct'].max()
    best_trade_date1 =  str(df_trades.loc[df_trades['trade_result_pct'] == best_trade].iloc[0]['open_date'])
    best_trade_date2 =  str(df_trades.loc[df_trades['trade_result_pct'] == best_trade].iloc[0]['close_date'])
    worst_trade = df_trades['trade_result_pct'].min()
    worst_trade_date1 =  str(df_trades.loc[df_trades['trade_result_pct'] == worst_trade].iloc[0]['open_date'])
    worst_trade_date2 =  str(df_trades.loc[df_trades['trade_result_pct'] == worst_trade].iloc[0]['close_date'])
    
    print("Period: [{}] -> [{}]".format(df_days.iloc[0]["day"], df_days.iloc[-1]["day"]))
    print("Initial wallet: {} $".format(round(initial_wallet,2)))
    
    print("\n--- General Information ---")
    print("Final wallet: {} $".format(round(final_wallet,2)))
    print("Performance vs US dollar: {} %".format(round(vs_usd_pct*100,2)))
    print("Sharpe Ratio: {}".format(round(sharpe_ratio,2)))
    print("Worst Drawdown T|D: -{}% | -{}%".format(round(max_trades_drawdown*100, 2), round(max_days_drawdown*100, 2)))
    print("Buy and hold performance: {} %".format(round(buy_and_hold_pct*100,2)))
    print("Performance vs buy and hold: {} %".format(round(vs_hold_pct*100,2)))
    print("Total trades on the period: {}".format(total_trades))
    print("Global Win rate: {} %".format(round(global_win_rate*100, 2)))
    print("Average Profit: {} %".format(round(avg_profit*100, 2)))
    print("Total fees paid {}$".format(round(total_fees, 2)))
    
    print("\nBest trades: +{} % the {} -> {}".format(round(best_trade*100, 2), best_trade_date1, best_trade_date2))
    print("Worst trades: {} % the {} -> {}".format(round(worst_trade*100, 2), worst_trade_date1, worst_trade_date2))

    return df_trades, df_days

def basic_multi_asset_backtest(trades, days):
    df_trades = trades.copy()
    df_days = days.copy()
    
    df_days['evolution'] = df_days['wallet'].diff()
    df_days['daily_return'] = df_days['evolution']/df_days['wallet'].shift(1)
    
    
    df_trades = df_trades.copy()
    df_trades['trade_result'] = df_trades["close_trade_size"] - df_trades["open_trade_size"] - df_trades["open_fee"] - df_trades["close_fee"]
    df_trades['trade_result_pct'] = df_trades['trade_result']/(df_trades["open_trade_size"] + df_trades["open_fee"])
    df_trades['trade_result_pct_wallet'] = df_trades['trade_result']/(df_trades["wallet"]+df_trades["trade_result"])
    good_trades = df_trades.loc[df_trades['trade_result_pct'] > 0]
    
    df_trades['wallet_ath'] = df_trades['wallet'].cummax()
    df_trades['drawdown'] = df_trades['wallet_ath'] - df_trades['wallet']
    df_trades['drawdown_pct'] = df_trades['drawdown'] / df_trades['wallet_ath']
    df_days['wallet_ath'] = df_days['wallet'].cummax()
    df_days['drawdown'] = df_days['wallet_ath'] - df_days['wallet']
    df_days['drawdown_pct'] = df_days['drawdown'] / df_days['wallet_ath']
    
    good_trades = df_trades.loc[df_trades['trade_result'] > 0]
    
    total_pair_traded = df_trades['pair'].nunique()
    initial_wallet = df_days.iloc[0]["wallet"]
    total_trades = len(df_trades)
    total_good_trades = len(good_trades)
    avg_profit = df_trades['trade_result_pct'].mean()   
    global_win_rate = total_good_trades / total_trades
    max_trades_drawdown = df_trades['drawdown_pct'].max()
    max_days_drawdown = df_days['drawdown_pct'].max()
    final_wallet = df_days.iloc[-1]['wallet']
    buy_and_hold_pct = (df_days.iloc[-1]['price'] - df_days.iloc[0]['price']) / df_days.iloc[0]['price']
    buy_and_hold_wallet = initial_wallet + initial_wallet * buy_and_hold_pct
    vs_hold_pct = (final_wallet - buy_and_hold_wallet)/buy_and_hold_wallet
    vs_usd_pct = (final_wallet - initial_wallet)/initial_wallet
    sharpe_ratio = (365**0.5)*(df_days['daily_return'].mean()/df_days['daily_return'].std())
    
    print("Period: [{}] -> [{}]".format(df_days.iloc[0]["day"], df_days.iloc[-1]["day"]))
    print("Initial wallet: {} $".format(round(initial_wallet,2)))
    print("Trades on {} pairs".format(total_pair_traded))
    
    print("\n--- General Information ---")
    print("Final wallet: {} $".format(round(final_wallet,2)))
    print("Performance vs US dollar: {} %".format(round(vs_usd_pct*100,2)))
    print("Sharpe Ratio: {}".format(round(sharpe_ratio,2)))
    print("Worst Drawdown T|D: -{}% | -{}%".format(round(max_trades_drawdown*100, 2), round(max_days_drawdown*100, 2)))
    print("Buy and hold performance: {} %".format(round(buy_and_hold_pct*100,2)))
    print("Performance vs buy and hold: {} %".format(round(vs_hold_pct*100,2)))
    print("Total trades on the period: {}".format(total_trades))
    print("Global Win rate: {} %".format(round(global_win_rate*100, 2)))
    print("Average Profit: {} %".format(round(avg_profit*100, 2)))
    
    print("\n----- Pair Result -----")
    print('-' * 95)
    print('{:<6s}{:>10s}{:>15s}{:>15s}{:>15s}{:>15s}{:>15s}'.format(
                "Trades","Pair","Sum-result","Mean-trade","Worst-trade","Best-trade","Win-rate"
                ))
    print('-' * 95)
    for pair in df_trades["pair"].unique():
        df_pair = df_trades.loc[df_trades["pair"] == pair]
        pair_total_trades = len(df_pair)
        pair_good_trades = len(df_pair.loc[df_pair["trade_result"] > 0])
        pair_worst_trade = str(round(df_pair["trade_result_pct"].min() * 100, 2))+' %'
        pair_best_trade = str(round(df_pair["trade_result_pct"].max() * 100, 2))+' %'
        pair_win_rate = str(round((pair_good_trades / pair_total_trades) * 100, 2))+' %'
        pair_sum_result = str(round(df_pair["trade_result_pct"].sum() * 100, 2))+' %'
        pair_avg_result = str(round(df_pair["trade_result_pct"].mean() * 100, 2))+' %'
        print('{:<6d}{:>10s}{:>15s}{:>15s}{:>15s}{:>15s}{:>15s}'.format(
                            pair_total_trades,pair,pair_sum_result,pair_avg_result,pair_worst_trade,pair_best_trade,pair_win_rate
                        ))
    
    return df_trades, df_days

def plot_sharpe_evolution(df_days):
    df_days_copy = df_days.copy()
    df_days_copy['evolution'] = df_days_copy['wallet'].diff()
    df_days_copy['daily_return'] = df_days_copy['evolution']/df_days_copy['wallet'].shift(1)

    df_days_copy['mean'] = df_days_copy['daily_return'].rolling(365).mean()
    df_days_copy['std'] = df_days_copy['daily_return'].rolling(365).std()
    df_days_copy['sharpe'] = (365**0.5)*(df_days_copy['mean']/df_days_copy['std'])
    df_days_copy['sharpe'].plot(figsize=(18, 9))

def plot_wallet_vs_asset(df_days, log=False):
    days = df_days.copy()
    # print("-- Plotting equity vs asset and drawdown --")
    fig, ax_left = plt.subplots(figsize=(15, 20), nrows=4, ncols=1)

    ax_left[0].title.set_text("Strategy equity curve")
    ax_left[0].plot(days['wallet'], color='royalblue', lw=1)
    if log:
        ax_left[0].set_yscale('log')
    ax_left[0].fill_between(days['wallet'].index, days['wallet'], alpha=0.2, color='royalblue')
    ax_left[0].axhline(y=days.iloc[0]['wallet'], color='black', alpha=0.3)
    ax_left[0].legend(['Wallet evolution (equity)'], loc ="upper left")

    ax_left[1].title.set_text("Base currency evolution")
    ax_left[1].plot(days['price'], color='sandybrown', lw=1)
    if log:
        ax_left[1].set_yscale('log')
    ax_left[1].fill_between(days['price'].index, days['price'], alpha=0.2, color='sandybrown')
    ax_left[1].axhline(y=days.iloc[0]['price'], color='black', alpha=0.3)
    ax_left[1].legend(['Asset evolution'], loc ="upper left")

    ax_left[2].title.set_text("Drawdown curve")
    ax_left[2].plot(-days['drawdown_pct']*100, color='indianred', lw=1)
    ax_left[2].fill_between(days['drawdown_pct'].index, -days['drawdown_pct']*100, alpha=0.2, color='indianred')
    ax_left[2].axhline(y=0, color='black', alpha=0.3)
    ax_left[2].legend(['Drawdown in %'], loc ="lower left")

    ax_right = ax_left[3].twinx()
    if log:
        ax_left[3].set_yscale('log')
        ax_right.set_yscale('log')

    ax_left[3].title.set_text("Wallet VS Asset (not on the same scale)")
    ax_left[3].set_yticks([])
    ax_right.set_yticks([])
    ax_left[3].plot(days['wallet'], color='royalblue', lw=1)
    ax_right.plot(days['price'], color='sandybrown', lw=1)
    ax_left[3].legend(['Wallet evolution (equity)'], loc ="lower right")
    ax_right.legend(['Asset evolution'], loc ="upper left")

    plt.show()
    
def get_metrics(df_trades, df_days):
    df_days_copy = df_days.copy()
    df_days_copy['evolution'] = df_days_copy['wallet'].diff()
    df_days_copy['daily_return'] = df_days_copy['evolution']/df_days_copy['wallet'].shift(1)
    sharpe_ratio = (365**0.5)*(df_days_copy['daily_return'].mean()/df_days_copy['daily_return'].std())
    
    df_days_copy['wallet_ath'] = df_days_copy['wallet'].cummax()
    df_days_copy['drawdown'] = df_days_copy['wallet_ath'] - df_days_copy['wallet']
    df_days_copy['drawdown_pct'] = df_days_copy['drawdown'] / df_days_copy['wallet_ath']
    max_drawdown = -df_days_copy['drawdown_pct'].max() * 100
    
    df_trades_copy = df_trades.copy()
    df_trades_copy['trade_result'] = df_trades_copy["close_trade_size"] - df_trades_copy["open_trade_size"] - df_trades_copy["open_fee"] - df_trades_copy["close_fee"]
    df_trades_copy['trade_result_pct'] = df_trades_copy['trade_result']/df_trades_copy["open_trade_size"]
    df_trades_copy['trade_result_pct_wallet'] = df_trades_copy['trade_result']/(df_trades_copy["wallet"]+df_trades_copy["trade_result"])
    good_trades = df_trades_copy.loc[df_trades_copy['trade_result_pct'] > 0]
    win_rate = len(good_trades) / len(df_trades)
    avg_profit = df_trades_copy['trade_result_pct'].mean()
    
    return {
        "sharpe_ratio": sharpe_ratio,
        "win_rate": win_rate,
        "avg_profit": avg_profit,
        "total_trades": len(df_trades_copy),
        "max_drawdown": max_drawdown
    }
    
def get_n_columns(df, columns, n=1):
    dt = df.copy()
    for col in columns:
        dt["n"+str(n)+"_"+col] = dt[col].shift(n)
    return dt

def plot_bar_by_month(df_days):
    sns.set(rc={'figure.figsize':(11.7,8.27)})
    custom_palette = {}
    
    last_month = int(df_days.iloc[-1]['day'].month)
    last_year = int(df_days.iloc[-1]['day'].year)
    
    current_month = int(df_days.iloc[0]['day'].month)
    current_year = int(df_days.iloc[0]['day'].year)
    current_year_array = []
    while current_year != last_year or current_month-1 != last_month:
        date_string = str(current_year) + "-" + str(current_month)
        
        monthly_perf = (df_days.loc[date_string]['wallet'].iloc[-1] - df_days.loc[date_string]['wallet'].iloc[0]) / df_days.loc[date_string]['wallet'].iloc[0]
        monthly_row = {
            'date': str(datetime.date(1900, current_month, 1).strftime('%B')),
            'result': round(monthly_perf*100)
        }
        if monthly_row["result"] >= 0:
            custom_palette[str(datetime.date(1900, current_month, 1).strftime('%B'))] = 'g'
        else:
            custom_palette[str(datetime.date(1900, current_month, 1).strftime('%B'))] = 'r'
        current_year_array.append(monthly_row)
        # print(monthly_perf*100) 
        if ((current_month == 12) or (current_month == last_month and current_year == last_year)):
            current_df = pd.DataFrame(current_year_array)
            # print(current_df)
            g = sns.barplot(data=current_df,x='date',y='result', palette=custom_palette)
            for index, row in current_df.iterrows():
                if row.result >= 0:
                    g.text(row.name,row.result, '+'+str(round(row.result))+'%', color='black', ha="center", va="bottom")
                else:
                    g.text(row.name,row.result, '-'+str(round(row.result))+'%', color='black', ha="center", va="top")
            g.set_title(str(current_year) + ' performance in %')
            g.set(xlabel=current_year, ylabel='performance %')
            
            year_result = (df_days.loc[str(current_year)]['wallet'].iloc[-1] - df_days.loc[str(current_year)]['wallet'].iloc[0]) / df_days.loc[str(current_year)]['wallet'].iloc[0]
            print("----- " + str(current_year) +" Cumulative Performances: " + str(round(year_result*100,2)) + "% -----")
            plt.show()

            current_year_array = []
        
        current_month += 1
        if current_month > 12:
            current_month = 1
            current_year += 1

def complete_multi_asset_backtest(
    trades, 
    days, 
    general_info=True, 
    trades_info=False,
    days_info=False,
    long_short_info=False,
    entry_exit_info=False,
    pair_info=False,
    exposition_info=False,
    indepedant_trade=True
):
    df_trades = trades.copy()
    df_days = days.copy().loc[df_trades.index.values[0] - np.timedelta64(1,'D'):]
    
    if df_trades.empty:
        raise Exception("No trades found")
    if df_days.empty:
        raise Exception("No days found")
    
    df_days['evolution'] = df_days['wallet'].diff()
    df_days['daily_return'] = df_days['evolution']/df_days['wallet'].shift(1)
    df_days['total_exposition'] = df_days['long_exposition']+df_days['short_exposition']
    
    df_trades['trade_result'] = df_trades["close_trade_size"] - df_trades["open_trade_size"] - df_trades["open_fee"]
    df_trades['trade_result_pct'] = df_trades['trade_result']/df_trades["open_trade_size"]
    df_trades['trade_result_pct_wallet'] = df_trades['trade_result']/(df_trades["wallet"]+df_trades["trade_result"])
    if indepedant_trade:
        result_to_use = "trade_result_pct"
    else:
        result_to_use = "trade_result_pct_wallet"
    
    df_trades['trades_duration'] = df_trades['close_date'] - df_trades['open_date']
    
    df_trades['wallet_ath'] = df_trades['wallet'].cummax()
    df_trades['drawdown'] = df_trades['wallet_ath'] - df_trades['wallet']
    df_trades['drawdown_pct'] = df_trades['drawdown'] / df_trades['wallet_ath']
    df_days['wallet_ath'] = df_days['wallet'].cummax()
    df_days['drawdown'] = df_days['wallet_ath'] - df_days['wallet']
    df_days['drawdown_pct'] = df_days['drawdown'] / df_days['wallet_ath']
    
    good_trades = df_trades.loc[df_trades['trade_result'] > 0]
    if good_trades.empty:
        print("!!! No good trades found")
    bad_trades = df_trades.loc[df_trades['trade_result'] < 0]
    if bad_trades.empty:
        print("!!! No bad trades found")
    
    initial_wallet = df_days.iloc[0]["wallet"]
    total_trades = len(df_trades)
    total_days = len(df_days)
    if good_trades.empty:
        total_good_trades = 0
    else:
        total_good_trades = len(good_trades)
        
    avg_profit = df_trades[result_to_use].mean()  
     
    try:
        avg_profit_good_trades = good_trades[result_to_use].mean()   
        avg_profit_bad_trades = bad_trades[result_to_use].mean() 
        total_bad_trades = len(bad_trades)
        mean_good_trades_duration = good_trades['trades_duration'].mean()
        mean_bad_trades_duration = bad_trades['trades_duration'].mean()

    except Exception as e:
        pass  
    
    global_win_rate = total_good_trades / total_trades
    max_trades_drawdown = df_trades['drawdown_pct'].max()
    max_days_drawdown = df_days['drawdown_pct'].max()
    mean_drawdown = df_days['drawdown_pct'].mean()
    final_wallet = df_days.iloc[-1]['wallet']
    buy_and_hold_pct = (df_days.iloc[-1]['price'] - df_days.iloc[0]['price']) / df_days.iloc[0]['price']
    buy_and_hold_wallet = initial_wallet + initial_wallet * buy_and_hold_pct
    vs_hold_pct = (final_wallet - buy_and_hold_wallet)/buy_and_hold_wallet
    vs_usd_pct = (final_wallet - initial_wallet)/initial_wallet
    sharpe_ratio = (365**0.5)*(df_days['daily_return'].mean()/df_days['daily_return'].std())
    sortino_ratio = 365**0.5 * (df_days["daily_return"].mean() / df_days["daily_return"][df_days["daily_return"] < 0].std())
    calmar_ratio = (df_days["daily_return"].mean()*365) / max_days_drawdown
    mean_trades_duration = df_trades['trades_duration'].mean()
    mean_trades_per_days = total_trades/total_days
    
    best_trade = df_trades[result_to_use].max()
    best_trade_date1 =  str(df_trades.loc[df_trades[result_to_use] == best_trade].iloc[0]['open_date'])
    best_trade_date2 =  str(df_trades.loc[df_trades[result_to_use] == best_trade].iloc[0]['close_date'])
    best_trade_pair =  str(df_trades.loc[df_trades[result_to_use] == best_trade].iloc[0]['pair'])
    worst_trade = df_trades[result_to_use].min()
    worst_trade_date1 =  str(df_trades.loc[df_trades[result_to_use] == worst_trade].iloc[0]['open_date'])
    worst_trade_date2 =  str(df_trades.loc[df_trades[result_to_use] == worst_trade].iloc[0]['close_date'])
    worst_trade_pair =  str(df_trades.loc[df_trades[result_to_use] == worst_trade].iloc[0]['pair'])

    df_days["win_loose"] = 0
    df_days.loc[df_days['daily_return'] > 0, "win_loose"] = 1
    df_days.loc[df_days['daily_return'] < 0, "win_loose"] = -1
    trade_days = df_days.loc[df_days['win_loose'] != 0]
    grouper = (trade_days["win_loose"] != trade_days["win_loose"].shift()).cumsum()
    df_days['streak'] = trade_days["win_loose"].groupby(grouper).cumsum()
    df_days['streak'] = df_days['streak'].ffill(axis = 0)

    best_day = df_days.loc[df_days['daily_return'] == df_days['daily_return'].max()].iloc[0]
    worst_day = df_days.loc[df_days['daily_return'] == df_days['daily_return'].min()].iloc[0]
    worst_day_return = worst_day['daily_return']
    best_day_return = best_day['daily_return']
    worst_day_date = worst_day['day']
    best_day_date = best_day['day']
    best_streak = df_days.loc[df_days['streak'] == df_days['streak'].max()].iloc[0]
    worst_streak = df_days.loc[df_days['streak'] == df_days['streak'].min()].iloc[0]
    best_streak_date = best_streak['day']
    worst_streak_date = worst_streak['day']
    best_streak_number = best_streak['streak']
    worst_streak_number = worst_streak['streak']
    win_days_number = len(df_days.loc[df_days['win_loose'] == 1])
    loose_days_number = len(df_days.loc[df_days['win_loose'] == -1])
    neutral_days_number = len(df_days.loc[df_days['win_loose'] == 0])

    mean_exposition = df_days['total_exposition'].mean()
    max_exposition = df_days['total_exposition'].max()
    max_long_exposition = df_days['long_exposition'].max()
    max_short_exposition = df_days['short_exposition'].max()
    max_risk = df_days['risk'].max()
    min_risk = df_days['risk'].min()
    mean_risk = df_days['risk'].mean()
    
    if general_info:
        print(f"Period: [{df_days.iloc[0]['day']}] -> [{df_days.iloc[-1]['day']}]")
        print(f"Initial wallet: {round(initial_wallet,2)} $")
        
        print("\n--- General Information ---")
        print(f"Final wallet: {round(final_wallet,2)} $")
        print(f"Performance: {round(vs_usd_pct*100,2)} %")
        print(f"Sharpe Ratio: {round(sharpe_ratio,2)} | Sortino Ratio: {round(sortino_ratio,2)} | Calmar Ratio: {round(calmar_ratio,2)}")
        print(f"Worst Drawdown T|D: -{round(max_trades_drawdown*100, 2)}% | -{round(max_days_drawdown*100, 2)}%")
        print(f"Mean daily Drawdown: -{round(mean_drawdown*100, 2)}%")
        print(f"Buy and hold performance: {round(buy_and_hold_pct*100,2)} %")
        print(f"Performance vs buy and hold: {round(vs_hold_pct*100,2)} %")
        print(f"Total trades on the period: {total_trades}")
        print(f"Average Profit: {round(avg_profit*100, 2)} %")
        print(f"Global Win rate: {round(global_win_rate*100, 2)} %")
    
    if trades_info:
        print("\n--- Trades Information ---")
        print(f"Mean Trades per day: {round(mean_trades_per_days, 2)}")
        print(f"Mean Trades Duration: {mean_trades_duration}")
        print(f"Best trades: +{round(best_trade*100, 2)} % the {best_trade_date1} -> {best_trade_date2} ({best_trade_pair})")
        print(f"Worst trades: {round(worst_trade*100, 2)} % the {worst_trade_date1} -> {worst_trade_date2} ({worst_trade_pair})")
        try:
            print(f"Total Good trades on the period: {total_good_trades}")
            print(f"Total Bad trades on the period: {total_bad_trades}")
            print(f"Average Good Trades result: {round(avg_profit_good_trades*100, 2)} %")
            print(f"Average Bad Trades result: {round(avg_profit_bad_trades*100, 2)} %")
            print(f"Mean Good Trades Duration: {mean_good_trades_duration}")
            print(f"Mean Bad Trades Duration: {mean_bad_trades_duration}")
        except Exception as e: 
            pass

    if days_info:
        print("\n--- Days Informations ---")
        print(f"Total: {len(df_days)} days recorded")
        print(f"Winning days: {win_days_number} days ({round(100*win_days_number/len(df_days), 2)}%)")
        print(f"Neutral days: {neutral_days_number} days ({round(100*neutral_days_number/len(df_days), 2)}%)")
        print(f"Loosing days: {loose_days_number} days ({round(100*loose_days_number/len(df_days), 2)}%)")
        print(f"Longest winning streak: {round(best_streak_number)} days ({best_streak_date})")
        print(f"Longest loosing streak: {round(-worst_streak_number)} days ({worst_streak_date})")
        print(f"Best day: {best_day_date} (+{round(best_day_return*100, 2)}%)")
        print(f"Worst day: {worst_day_date} ({round(worst_day_return*100, 2)}%)")

    if exposition_info:
        print("\n--- Exposition Informations ---")
        print(f"Mean Exposition: {round(mean_exposition, 2)}")
        print(f"Max Exposition: {round(max_exposition, 2)}")
        print(f"Max Long Exposition: {round(max_long_exposition, 2)}")
        print(f"Max Short Exposition: {round(max_short_exposition, 2)}")
        print(f"Mean VAR Rsik: {round(mean_risk, 2)}%")
        print(f"Max VAR Rsik: {round(max_risk, 2)}%")
        print(f"Min VAR Rsik: {round(min_risk, 2)}%")
        
    if long_short_info:
        long_trades = df_trades.loc[df_trades['position'] == "LONG"]
        short_trades = df_trades.loc[df_trades['position'] == "SHORT"]
        if long_trades.empty or short_trades.empty:
            print("!!! No long or short trades found")
        else:
            total_long_trades = len(long_trades)
            total_short_trades = len(short_trades)
            good_long_trades = long_trades.loc[long_trades['trade_result'] > 0]
            good_short_trades = short_trades.loc[short_trades['trade_result'] > 0]
            if good_long_trades.empty: 
                total_good_long_trades = 0
            else:
                total_good_long_trades = len(good_long_trades)
            if good_short_trades.empty: 
                total_good_short_trades = 0
            else:
                total_good_short_trades = len(good_short_trades)
            long_win_rate = total_good_long_trades / total_long_trades
            short_win_rate = total_good_short_trades / total_short_trades
            long_average_profit = long_trades[result_to_use].mean()
            short_average_profit = short_trades[result_to_use].mean()
            print("\n--- " + "LONG informations" + " ---")
            print(f"Total LONG trades on the period: {total_long_trades}")
            print(f"LONG Win rate: {round(long_win_rate*100, 2)} %")
            print(f"Average LONG Profit: {round(long_average_profit*100, 2)} %")
            print("\n--- " + "SHORT informations" + " ---")
            print(f"Total SHORT trades on the period: {total_short_trades}")
            print(f"SHORT Win rate: {round(short_win_rate*100, 2)} %")
            print(f"Average SHORT Profit: {round(short_average_profit*100, 2)} %")
    
    if entry_exit_info:
        print("\n" + "-" * 16 + " Entries " + "-" * 16)
        total_entries = len(df_trades)
        open_dict = df_trades.groupby("position")["open_reason"].value_counts().to_dict()
        for entry in open_dict:
            print(
                "{:<25s}{:>15s}".format(
                    entry[0] + " - " + entry[1],
                    str(open_dict[entry])
                    + " ("
                    + str(round(100 * open_dict[entry] / total_entries, 1))
                    + "%)",
                )
            )
        print("-" * 17 + " Exits " + "-" * 17)
        total_exits = len(df_trades)
        close_dict = df_trades.groupby("position")["close_reason"].value_counts().to_dict()
        for entry in close_dict:
            print(
                "{:<25s}{:>15s}".format(
                    entry[0] + " - " + entry[1],
                    str(close_dict[entry])
                    + " ("
                    + str(round(100 * close_dict[entry] / total_entries, 1))
                    + "%)",
                )
            )
        print("-" * 40)

    if pair_info:
        print("\n--- Pair Result ---")
        print('-' * 95)
        print('{:<6s}{:>10s}{:>15s}{:>15s}{:>15s}{:>15s}{:>15s}'.format(
                    "Trades","Pair","Sum-result","Mean-trade","Worst-trade","Best-trade","Win-rate"
                    ))
        print('-' * 95)
        for pair in df_trades["pair"].unique():
            df_pair = df_trades.loc[df_trades["pair"] == pair]
            pair_total_trades = len(df_pair)
            pair_good_trades = len(df_pair.loc[df_pair["trade_result"] > 0])
            pair_worst_trade = str(round(df_pair["trade_result_pct"].min() * 100, 2))+' %'
            pair_best_trade = str(round(df_pair["trade_result_pct"].max() * 100, 2))+' %'
            pair_win_rate = str(round((pair_good_trades / pair_total_trades) * 100, 2))+' %'
            pair_sum_result = str(round(df_pair["trade_result_pct"].sum() * 100, 2))+' %'
            pair_avg_result = str(round(df_pair["trade_result_pct"].mean() * 100, 2))+' %'
            print('{:<6d}{:>10s}{:>15s}{:>15s}{:>15s}{:>15s}{:>15s}'.format(
                                pair_total_trades,pair,pair_sum_result,pair_avg_result,pair_worst_trade,pair_best_trade,pair_win_rate
                            ))

    return df_trades, df_days