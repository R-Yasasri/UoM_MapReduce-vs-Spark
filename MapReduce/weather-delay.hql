SELECT Year, avg((WeatherDelay /ArrDelay)*100) from flights GROUP BY Year