---
title: 2015 US Domestic Flights Analysis
---


<Grid cols=4>
    <!-- Blank groups to force button in the fourth column -->
    <Group/>
    <Group/>
    <Group/>
    <LinkButton url='https://github.com/phamlamn/airline-flights-pipeline'>
        Source Code
    </LinkButton>
</Grid>


---


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
    <!-- TODO Overall Delayed Rate -->
    <!-- Overall Cancelled Rate -->
    <!-- Average Delay Time -->
</Grid>


---


```sql airlines
SELECT
    airline
FROM agg_fact_flights.data
WHERE agg_level = 'airline'
    AND time_agg_level = 'year'
```

```sql flight_volume_by_month
SELECT
    year,
    month,
    sum(total_flights) as total_flights
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
    sum(total_flights) as total_flights
FROM agg_fact_flights.data
WHERE time_agg_level = 'day_of_week'
    AND agg_level = 'airline'
    AND airline like ('${inputs.airline_dropdown.value}')
GROUP BY ALL
ORDER BY day_of_week;
```
<Dropdown data={airlines} name=airline_dropdown value=airline defaultValue='%' title="Select an Airline">
    <DropdownOption value='%' valueLabel='All'/>
</Dropdown>


<Grid cols=2>
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
</Grid>


<!-- TODO add delayed and cancelled flights -->







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

