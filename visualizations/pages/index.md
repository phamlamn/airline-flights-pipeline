---
title: 2015 US Domestic Flights Analysis
---

<Accordion>
  <AccordionItem title="Disclaimer:">
    This project is for demonstration purposes only and may not be accurate or complete. Do not use it for real-world business decisions. Always consult official sources for reliable information.
</AccordionItem>
</Accordion>

<LinkButton url='https://github.com/phamlamn/airline-flights-pipeline'>
    Source Code
</LinkButton>


```sql total_stats_2015
select
    total_flights,
    delayed_flights,
    cancelled_flights
from agg_fact_flights.data
where time_agg_level = 'year'
    AND agg_level = 'all'
    AND year = 2015
```
<Grid cols=3>
    <BigValue 
        data={total_stats_2015} 
        value=total_flights
    />
    <BigValue
        data={total_stats_2015}
        value=delayed_flights
    />
    <BigValue
        data={total_stats_2015}
        value=cancelled_flights
    />
</Grid>


```sql delay_statistics
SELECT 
    year, 
    month,
    airline,
    origin_airport,
    delayed_rate,
    avg_delay_time
FROM 
    agg_fact_flights.data
WHERE 
    time_agg_level = 'year_month'
    AND agg_level = 'all'
```
<Dropdown
    data={delay_statistics}
    name=airline_dropdown
    value=airline
    title="Select a Category"
    defaultValue="null"
/>

<LineChart
    data={delay_statistics}
    x=month
    y=delayed_rate
    y2=avg_delay_time
    title="Delay Rate and Average Delay Time by Month"
    markers=true
    xAxisTitle=Month
    yAxisTitle="Delayed Rate (%)"
    y2AxisTitle="Avg Delay Time (Minutes)"
    xTickMarks=true
/>






<!-- -- ```sql agg_fact_flights
-- select
--     *
-- from agg_fact_flights.data
-- ```
-- <DataTable data={agg_fact_flights}/> -->


```sql flight_volume_by_month
SELECT
    year,
    month,
    total_flights
FROM agg_fact_flights.data
WHERE time_agg_level = 'year_month'
    AND agg_level = 'all'
ORDER BY month;
```

<Dropdown data={flight_volume_by_month} name=time_agg_level value=column_name/>

<BarChart
    data={flight_volume_by_month}
    x=month
    y=total_flights
    title="Total Flights by Month"
    xAxisTitle=Month
    yAxisTitle=Volume
    xTickMarks=true
/>


```sql flight_volume_by_day_of_week
SELECT
    year,
    day_of_week,
    total_flights
FROM agg_fact_flights.data
WHERE time_agg_level = 'day_of_week'
    AND agg_level = 'all'
ORDER BY day_of_week;
```

<Dropdown data={flight_volume_by_day_of_week} name=time_agg_level value=column_name/>

<BarChart
    data={flight_volume_by_day_of_week}
    x=day_of_week
    y=total_flights
    title="Total Flights by Day of Week"
    xAxisTitle=Month
    yAxisTitle=Volume
    xTickMarks=true
/>







