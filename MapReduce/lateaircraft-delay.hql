SELECT Year, avg((LateAircraftDelay /ArrDelay)*100) from flights GROUP BY Year