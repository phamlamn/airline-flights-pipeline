---
title: Cancellation Analysis
---

<!-- TODO allow interactivity based on select dimension (eg. airlines) -->

<!-- Cancellation Stats by Month -->
```sql cancellation_statistics
SELECT 
    year, 
    month,
    cancelled_rate,
    percent_cancellations_A,
    percent_cancellations_B,
    percent_cancellations_C,
    percent_cancellations_D
FROM 
    agg_fact_flights.data
WHERE 
    time_agg_level = 'year_month'
    AND agg_level = 'all'
```
<LineChart
    data={cancellation_statistics}
    x=month
    y=cancelled_rate
    title="Cancelled Rate by Month"
    markers=true
    xAxisTitle=Month
    yAxisTitle="Cancelled Rate (%)"
    yFmt=pct2
    xTickMarks=true
/>


```sql unpivoted_cancellations
WITH cancel_rates AS (
    SELECT 
        year, 
        month,
        cancellations_A as airline_carrier,
        cancellations_B as weather,
        cancellations_C as national_air_system,
        cancellations_D as security
    FROM 
        agg_fact_flights.data
    WHERE 
        time_agg_level = 'year_month'
        AND agg_level = 'all'
)

unpivot cancel_rates
on
    airline_carrier,
    weather,
    national_air_system,
    security
into
	name reason
	value volume
```
<AreaChart
    title="Cancellation Breakdown by Month"
    data={unpivoted_cancellations}
    x=month
    y=volume
    series=reason
    seriesOrder={["security", "national_air_system", "airline_carrier", "weather"]}
/>


<!-- How many flights were cancelled in 2015? What % of cancellations were due to weather? What % were due to the Airline/Carrier? -->
```sql cancellation_data
SELECT 'Security' as name, cancellations_D as count
FROM agg_fact_flights.data
WHERE agg_level = 'all'
    AND time_agg_level = 'year'

UNION ALL

SELECT 'National Air System' as name, cancellations_C as count
FROM agg_fact_flights.data
WHERE agg_level = 'all'
    AND time_agg_level = 'year'

UNION ALL

SELECT 'Airline/Carrier' as name, cancellations_A as count
FROM agg_fact_flights.data
WHERE agg_level = 'all'
    AND time_agg_level = 'year'

UNION ALL

SELECT 'Weather' as name, cancellations_B as count
FROM agg_fact_flights.data
WHERE agg_level = 'all'
    AND time_agg_level = 'year'
```

```sql pie_data
SELECT
    name,
    count as value
FROM ${cancellation_data}
```
<ECharts config={
    {
        title: {
            text: 'Total Cancellations Breakdown by Cancellation Reason',
            left: 'left'
        },
        tooltip: {
            trigger: 'item'
        },
        legend: {
            orient: 'vertical',
            left: 'right'
        },
        tooltip: {
            formatter: '{b}: {c} ({d}%)'
        },
        series: [
        {
          type: 'pie',
          data: [...pie_data],
          itemStyle: {
            shadowBlur: 10,
            shadowOffsetX: 0,
            shadowColor: 'rgba(0, 0, 0, 0.5)'
          }
        }
      ]
      }
    }
/>




<!-- Cancelation breakdown by month, winter month effects? -->
<!-- Cancelation breakdown by airport, do airports have any underlaying issues? -->


