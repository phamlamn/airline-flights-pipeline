---
title: Delay Analysis
queries:
  - airlines.sql
---



<!-- Delay Stats by Month -->
```sql delay_statistics
SELECT 
    year, 
    month,
    avg(delayed_rate) as delayed_rate,
    avg(avg_delay_time) as avg_delay_time
FROM 
    agg_fact_flights.data
WHERE 
    time_agg_level = 'year_month'
    AND agg_level = 'airline'
    AND airline like ('${inputs.airline_dropdown2.value}')
GROUP BY year, month
```
<Dropdown data={airlines} name=airline_dropdown2 value=airline defaultValue='%' title="Select an Airline">
    <DropdownOption value='%' valueLabel='All'/>
</Dropdown>

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
    yFmt=pct2
    xTickMarks=true
/>



<!-- TODO How does the % of delayed flights vary throughout the year? What about for flights leaving from Boston (BOS) specifically?
```sql delay_statistics_by_origin_airport
SELECT 
    year, 
    month,
    origin_airport,
    delayed_rate,
    avg_delay_time
FROM 
    agg_fact_flights.data
WHERE 
    time_agg_level = 'year_month'
    AND agg_level = 'origin_airport'
```
<Dropdown
    data={delay_statistics_by_origin_airport}
    name=origin_airport_dropdown
    value=origin_airport
    title="Select a Category"
    defaultValue="PHL"
/>
<LineChart
    data={delay_statistics_by_origin_airport}
    x=month
    y=delayed_rate
    y2=avg_delay_time
    title="Delay Rate and Average Delay Time by Month"
    markers=true
    xAxisTitle=Month
    yAxisTitle="Delayed Rate (%)"
    y2AxisTitle="Avg Delay Time (Minutes)"
    xTickMarks=true
/> -->


<!-- ## Which airlines seem to be most and least reliable, in terms of on-time departure? -->
```sql airline_reliability
SELECT
    airline,
    delayed_rate,
    avg_delay_time
FROM agg_fact_flights.data
WHERE agg_level = 'airline'
    AND time_agg_level = 'all'
ORDER BY airline
```
<BarChart
    data={airline_reliability}
    x=airline
    y=delayed_rate
    yFmt=pct2
    y2=avg_delay_time
    title="Airline Reliability"
    subtitle="in terms of on-time departure"
    xAxisTitle=Airline
    yAxisTitle="Delayed Rate (%)"
    y2AxisTitle="Avg Delay Time (Minutes)"
    xTickMarks=true
/>
<!-- Airline code to airline name... -->

<!-- Delay stats by airline, airport? -->