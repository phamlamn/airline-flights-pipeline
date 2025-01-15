---
title: 2015 US Domestic Flights Analysis
queries:
  - airlines.sql
---




<Details title='Show SQL'>

    Click on the menu icon `...` in the top right corner of the page and select `Show Queries`

</Details>



<Grid cols=4>
    <!-- Selection for airline -->
    <Group>
        <!-- Linebreak for formatting -->
        <LineBreak lines=1/>
        <Dropdown data={airlines} name=airline_dropdown value=airline defaultValue='%' title="Select an Airline">
            <DropdownOption value='%' valueLabel='All'/>
        </Dropdown>
    </Group>

    <!-- Blank groups to force button in the fourth column -->
    <Group/>
    <Group/>

    <!-- Source code button -->
    <LinkButton url='https://github.com/phamlamn/airline-flights-pipeline'>
        Source Code
    </LinkButton>
</Grid>




<!-- Big Value Primary stats: Total flights, delayed flights, cancelled flights -->
```sql total_stats_2015
select
    sum(total_flights) as total_flights,
    sum(delayed_flights) as delayed_flights,
    avg(delayed_rate) as delayed_rate,
    avg(avg_delay_time) as avg_delay_time,
    sum(cancelled_flights) as cancelled_flights,
    avg(cancelled_rate) as cancelled_rate,
from agg_fact_flights.data
where time_agg_level = 'year'
    AND agg_level = 'airline'
    AND airline like ('${inputs.airline_dropdown.value}')
    AND year = 2015
group by year
```
<Grid cols=3>
    <BigValue 
        data={total_stats_2015} 
        value=total_flights
        fmt=num0
    />
    <BigValue
        data={total_stats_2015}
        value=delayed_flights
        fmt=num0
    />
    <BigValue
        data={total_stats_2015}
        value=cancelled_flights
        fmt=num0
    />
    <BigValue
        data={total_stats_2015}
        value=avg_delay_time
        title='Avg Delay Time (Minutes)'
    />
    <BigValue
        data={total_stats_2015}
        value=delayed_rate
        fmt=pct1
        title='Total Delayed Rate'
    />
    <BigValue
        data={total_stats_2015}
        value=cancelled_rate
        fmt=pct1
        title='Total Cancelled Rate'
    />
</Grid>




<!-- Stacked chart for: Total flights, delayed flights, cancelled flights -->
```sql unpivoted_flight_volume_monthly
WITH volume AS (
  SELECT
    year,
    month,
    sum(total_flights) as total_flights,
    sum(delayed_flights) as delayed_flights,
    sum(cancelled_flights) as cancelled_flights,
FROM agg_fact_flights.data
WHERE time_agg_level = 'year_month'
    AND agg_level = 'airline'
    AND airline like ('${inputs.airline_dropdown.value}')
GROUP BY year, month
ORDER BY month
)

UNPIVOT volume
ON total_flights, delayed_flights, cancelled_flights
INTO
	NAME metric
	VALUE volume
```

```sql unpivoted_flight_volume_day_of_week
WITH volume AS (
  SELECT
    year,
    day_of_week,
    sum(total_flights) as total_flights,
    sum(delayed_flights) as delayed_flights,
    sum(cancelled_flights) as cancelled_flights,
FROM agg_fact_flights.data
WHERE time_agg_level = 'day_of_week'
    AND agg_level = 'airline'
    AND airline like ('${inputs.airline_dropdown.value}')
GROUP BY year, day_of_week
ORDER BY day_of_week
)

UNPIVOT volume
ON total_flights, delayed_flights, cancelled_flights
INTO
	NAME metric
	VALUE volume
```
<BarChart
    data={unpivoted_flight_volume_monthly}
    x=month
    y=volume
    series=metric
    seriesOrder={['total_flights', 'delayed_flights', 'cancelled_flights']}
    title="Total Flights by Month"
    xAxisTitle=Month
    yAxisTitle=Volume
    xTickMarks=true
    type=grouped
/>

<BarChart
    data={unpivoted_flight_volume_day_of_week}
    x=day_of_week
    y=volume
    series=metric
    seriesOrder={['total_flights', 'delayed_flights', 'cancelled_flights']}
    title="Total Flights by Day of Week"
    xAxisTitle="Day of Week"
    yAxisTitle=Volume
    xTickMarks=true
    type=grouped
/>



<!-- Individual Breakdown charts -->
```sql flight_volume_by_month
SELECT
    year,
    month,
    sum(total_flights) as total_flights,
    sum(delayed_flights) as delayed_flights,
    sum(cancelled_flights) as cancelled_flights,
FROM agg_fact_flights.data
WHERE time_agg_level = 'year_month'
    AND agg_level = 'airline'
    AND airline like ('${inputs.airline_dropdown.value}')
GROUP BY ALL
ORDER BY month;
```

```sql flight_volume_by_day_of_week
SELECT
    year,
    day_of_week,
    sum(total_flights) as total_flights,
    sum(delayed_flights) as delayed_flights,
    sum(cancelled_flights) as cancelled_flights,
FROM agg_fact_flights.data
WHERE time_agg_level = 'day_of_week'
    AND agg_level = 'airline'
    AND airline like ('${inputs.airline_dropdown.value}')
GROUP BY ALL
ORDER BY day_of_week;
```
<Grid cols=2>
    <!-- Total flights -->
    <BarChart
        data={flight_volume_by_month}
        x=month
        y=total_flights
        title="Total Flights by Month"
        xAxisTitle=Month
        yAxisTitle=Volume
        xTickMarks=true
    />
    <BarChart
        data={flight_volume_by_day_of_week}
        x=day_of_week
        y=total_flights
        title="Total Flights by Day of Week"
        xAxisTitle=Month
        yAxisTitle=Volume
        xTickMarks=true
    />

    <!-- Delayed flights -->
    <BarChart
        data={flight_volume_by_month}
        x=month
        y=delayed_flights
        title="Total Delayed Flights by Month"
        xAxisTitle=Month
        yAxisTitle=Volume
        xTickMarks=true
    />
    <BarChart
        data={flight_volume_by_day_of_week}
        x=day_of_week
        y=delayed_flights
        title="Total Delayed Flights by Day of Week"
        xAxisTitle=Month
        yAxisTitle=Volume
        xTickMarks=true
    />

    <!-- Cancelled flights -->
    <BarChart
        data={flight_volume_by_month}
        x=month
        y=cancelled_flights
        title="Total cancelled Flights by Month"
        xAxisTitle=Month
        yAxisTitle=Volume
        xTickMarks=true
    />
    <BarChart
        data={flight_volume_by_day_of_week}
        x=day_of_week
        y=cancelled_flights
        title="Total cancelled Flights by Day of Week"
        xAxisTitle=Month
        yAxisTitle=Volume
        xTickMarks=true
    />
</Grid>



<!-- Full table -->
<!-- -- ```sql agg_fact_flights
-- select
--     *
-- from agg_fact_flights.data
-- ```
-- <DataTable data={agg_fact_flights}/> -->


<Accordion>
  <AccordionItem/>
  <AccordionItem title="Disclaimer:">
    This project is for demonstration purposes only and may not be accurate or complete. Do not use it for real-world business decisions. Always consult official sources for reliable information.
</AccordionItem>
</Accordion>

