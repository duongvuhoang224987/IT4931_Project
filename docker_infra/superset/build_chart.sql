-- Big Number
SELECT count(*) as num_trip from yellow_taxi_raw ;

-- Line Chart
Select window_start, trip_count from yellow_window10min;

-- Pie Chart
SELECT vendorid, COUNT(*) AS trip_count FROM yellow_taxi_raw group by vendorid;

-- Bar Chart
select cast(dolocationid as varchar) as doid, sum(total_amount) as total_revenue from mongodb.taxi.yellow_taxi_raw group by cast(dolocationid as varchar);

-- Scatter Plot:
select trip_distance, tip_amount from mongodb.taxi.yellow_taxi_raw;