#!/usr/bin/python3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import statsmodels.api as sm
import math
from data_access import *

#Disable UserWarning - About timezone information drop
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)

#How many days should be used to train the model
TRAINING_START_DAYS=90
TRAINING_START_HOURS=(TRAINING_START_DAYS*24)+1

#How many hours should be used for scoring
#TRAINING_END_HOURS=72
TRAINING_END_HOURS=48

#How many hours to the future should forecast be generated
#FUTURE_HOURS=336
FUTURE_HOURS=24

#How long is the seasonal training window. You will want to use 24 or 168
TRAINING_WINDOW=168
#TRAINING_WINDOW=24

#If all the training data should be displayed
SHOW_TRAINING=False
#SHOW_TRAINING=True

#If the graph should be plotted to the screen. False for file only
PLOT_GRAPH=False
#PLOT_GRAPH=True

#If the graph should always be plotted. False for outliers only.
#ALWAYS_GRAPH=True
ALWAYS_GRAPH=True

#Round up the High CI
ROUND_UP_HIGH_CI=True

#Alert when lower than the Low CI. This only affects the trigger to generate a graphic.
#Lower outlier display works even when false, as long as there is an upper outlier.
ALERT_LOWER_CI=False

def train(training,description,file_name):
    print('Training model for {}'.format(description))
    mod = sm.tsa.statespace.SARIMAX(training,order=(1,0,1), 
        seasonal_order=(1,0,1,TRAINING_WINDOW),
        enforce_stationarity=False,
        enforce_invertibility=False)
    try:
        results = mod.fit(disp=0)
    except ValueError as e:
        print('Possibly the training period is too short compared to the scoring period.')
        return False
    if not results.mle_retvals['converged']:
        print('Model for {} not converged, aborting'.format(description))
        return False
    return results

def evaluate(data,description,now_time,unity):
    training,scoring=data[0],data[1]
    training.index=pd.DatetimeIndex(training.index).to_period('H')
    scoring.index=pd.DatetimeIndex(scoring.index).to_period('H')

    file_name=description.replace("/", "")
    results=train(training,description,file_name)
    steps=(TRAINING_END_HOURS+FUTURE_HOURS)
    if results:
        forecast_values = results.get_forecast(steps=steps)
    else:
        return False
    forecast_ci = forecast_values.conf_int()
    hasOutlier=False
    outliers = []
    toutliers = []
    #Remove negative values from forecast
    forecast_ci[forecast_ci < 0] = 0
    #For every item in the forecast confidence interval
    for index, row in forecast_ci.iterrows():
        #If there is scoring data for this date
        try:
            #If the scoring data is a high outlier
            if ROUND_UP_HIGH_CI and scoring.loc[index]['Ocorrido'] > math.ceil(row['upper Histórico']):
                hasOutlier=True
            if scoring.loc[index]['Ocorrido'] > row['upper Histórico']:
                outliers += [scoring.loc[index]['Ocorrido']]
                toutliers += [index]
            #If lower forecast is higher than zero and scoring data is a lower outlier
            elif row['lower Histórico'] > 0 and scoring.loc[index]['Ocorrido'] < row['lower Histórico']:
                outliers += [scoring.loc[index]['Ocorrido']]
                toutliers += [index]
                if ALERT_LOWER_CI:
                    hasOutlier=True
        #Future forecasts will throw a KeyError exception
        except KeyError:
            pass
    #If there is at least one outlier or if graphs should always be generated
    if ALWAYS_GRAPH or hasOutlier:
        outliers_df = pd.DataFrame({'index':toutliers,'Anomalias':outliers})
        outliers_df = outliers_df.set_index('index')
        plot(description,training,scoring,forecast_values,forecast_ci,outliers_df,unity)

def plot(description,training_data,scoring_data,forecast_values,forecast_ci,outliers_df,unity):
    if SHOW_TRAINING:
        ax = training_data.plot()
        ax = scoring_data.plot(ax=ax)
    else:
        ax = scoring_data.plot()
    #Remove predicted values that are lower than zero
    forecast_df=forecast_values.predicted_mean
    forecast_df[forecast_df < 0] = 0
    forecast_df.plot(ax=ax, label='Previsões')
    ax.fill_between(forecast_ci.index,
            forecast_ci.iloc[:, 0],
            forecast_ci.iloc[:, 1],color='g', alpha=.5)
    if len(outliers_df) > 0 :
        ax = outliers_df.plot(ax=ax,color='red',marker='o', linestyle='None',label='Anomalia')
    ax.set_xlabel('Tempo')
    ax.set_ylabel(unity)
    ax.set_title(description)
    plt.legend()
    file_name=description.replace("/", "")
    plt.savefig('output/'+file_name+'.png', bbox_inches="tight",dpi=200)
    if PLOT_GRAPH:
        plt.show()
    plt.close()

for key,value in targets.items():
    now_time=datetime.now()
    data=getIpDstByteSum(key,now_time,TRAINING_START_HOURS=TRAINING_START_HOURS,TRAINING_END_HOURS=TRAINING_END_HOURS)
    evaluate(data,'Bytes com destino '+key+' '+value,now_time,'Bytes')
    data=getIpDstFlowSum(key,now_time,TRAINING_START_HOURS=TRAINING_START_HOURS,TRAINING_END_HOURS=TRAINING_END_HOURS)
    evaluate(data,'Conexões com destino '+key+' '+value,now_time,'Conexões')
