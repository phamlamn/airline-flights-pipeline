---
title: Cancellation Analysis
queries:
  - airlines.sql
---


<!-- Cancellation Stats by Month -->
```sql cancellation_statistics
SELECT 
    year, 
    month,
    AVG(cancelled_rate) as cancelled_rate,
    AVG(percent_cancellations_A),
    AVG(percent_cancellations_B),
    AVG(percent_cancellations_C),
    AVG(percent_cancellations_D)
FROM 
    agg_fact_flights.data
WHERE 
    time_agg_level = 'year_month'
    AND agg_level = 'airline'
    AND airline like ('${inputs.airline_dropdown3.value}')
GROUP BY year, month
```
<Dropdown data={airlines} name=airline_dropdown3 value=airline defaultValue='%' title="Select an Airline">
    <DropdownOption value='%' valueLabel='All'/>
</Dropdown>

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
        SUM(cancellations_A) as airline_carrier,
        SUM(cancellations_B) as weather,
        SUM(cancellations_C) as national_air_system,
        SUM(cancellations_D) as security
    FROM 
        agg_fact_flights.data
    WHERE 
        time_agg_level = 'year_month'
        AND agg_level = 'airline'
        AND airline like ('${inputs.airline_dropdown3.value}')
    GROUP BY year, month
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
WITH cancellations_breakdown AS (
    SELECT
        year,
        SUM(cancellations_A) as airline_carrier,
        SUM(cancellations_B) as weather,
        SUM(cancellations_C) as national_air_system,
        SUM(cancellations_D) as security
    FROM agg_fact_flights.data
    WHERE
        time_agg_level = 'year'
        AND agg_level = 'airline'
        AND airline like ('${inputs.airline_dropdown3.value}')
    GROUP BY year
)

SELECT 'Security' as name, security as count
FROM cancellations_breakdown

UNION ALL

SELECT 'National Air System' as name, national_air_system as count
FROM cancellations_breakdown

UNION ALL

SELECT 'Airline/Carrier' as name, airline_carrier as count
FROM cancellations_breakdown

UNION ALL

SELECT 'Weather' as name, weather as count
FROM cancellations_breakdown
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


