# Train delays and weather correlation in Germany

## Project Idea
Every day, millions of people in Germany rely on DB trains. Frequent delays raise the question of underlying causes. This project investigates a specific hypothesis: **Is there a correlation between bad weather conditions (specifically heavy rain) and an increase in train delays?**

The analysis focuses on:
*   **Train Data**: Collected via DB Timetables API (planned and changed departures of long distance trains) as well as geographical station data from the station API. As DB doesn't provide historical delay data, I had to record the data myself over a limited time (several month of data recorded). Only main train stations are included.
*   **Weather Data**: Collected via Meteostat API (precipitation, temperature).
*   **Correlation**: Matching train stations with the nearest weather stations to analyze the relationship between rainfall and delay minutes/counts.

## Analysis

The analysis is divided into two parts, each implemented in a dedicated Jupyter notebook:

1.  [Delay over time and spatial distribution](delay_time_spatial_analysis.ipynb)
    -  Analysis of delay over time (average delay per train per day or per hour)
    -  Spatial map plot showing normalized delay per station (over time).



    Analysis of train delays shows that, when averaged across trains running per hour, the mean delay is approximately 40 minutes, with a pronounced increase observed in mid-August.

    Examining delays per train over the course of a day reveals that most delays occur during the morning hours. Although fewer trains operate at that time, the proportion of delayed trains is higher compared to midday, when train frequency is significantly greater but delays are less frequent.

    A comparison across the week indicates a slight decrease in delays on Saturdays.

    Finally, an analysis of delays at major train stations across Germany shows that the highest average delays occur near the borders with Poland and the Czech Republic.

2.  [Delay and Rain Correlation](weather_correlation_analysis.ipynb)
    -   Analysis of correlation between delay and rain per station with nearest weather station data (over time).
    - Time-series plot showing the correlation of delay and rain over the recorded period.
    
    By matching train stations with nearby weather stations, potential correlations between precipitation and train delays were examined. Most stations show no correlation or even negative correlation. Only one day in July showed both high precipitation and particularly large delays.

    Overall, no meaningful correlation between precipitation and delays was identified. A logical next step would be to investigate the relationship between snowfall and delays; however, this would require a larger and more detailed dataset on train delays. It should also be noted that correlation does not imply causation.

## Project Structure
```
db-delay-analysis
├── data
│   ├── amse.sqlite         # Sqlite database with all the data
│   └── pipeline.py         # Script to collect data from APIs
├── README.md               # Project documentation
├── reports
│   └── plots               # Image and Video files of plots
├── requirements.txt
└── src
    ├── delay_time_spatial_analysis.ipynb
    └── weather_correlation_analysis.ipynb
```

## Setup Instructions

### Prerequisites
*   **Python 3.11** or higher.

### Installation
1.  Clone the repository.
2.  Install the required dependencies from 'requirements.txt' file.
3. Create a .env file with these entries in the root of the cloned repository. You get the credential by registering for the DB API Marketplace (for free).

```
CLIENT_ID=...
CLIENT_KEY=...
```

4. Run data pipeline.py to collect data from APIs.
5. Run the Analysis: Notebooks in src for analysis.

## License:
The data is offered by DB Station&Service AG and is made available under the Creative Commons Attribution 4.0 International (CC BY 4.0) license [^1]. An overview of the API can be found on the DB API Marketplace [^2].

Weather data was collected from Meteostat Developers [^3], who offers historical weather and climate data also provided under the CC BY 4.0 license.

Data Sources: [Meteostat](https://meteostat.net/en/) and [DB Station&Service AG](https://www.bahnhof.de/)

[^1]: https://creativecommons.org/licenses/by/4.0/
[^2]: https://developers.deutschebahn.com/db-api-marketplace/apis/product/timetables/api/26494#/Timetables_10213/overview 
[^3]: https://dev.meteostat.net/