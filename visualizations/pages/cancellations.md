---
title: Cancellation Analysis
---


<!-- How many flights were cancelled in 2015? What % of cancellations were due to weather? What % were due to the Airline/Carrier? -->
```sql cancellation_data
SELECT 'Airline/Carrier' as name, cancellations_A as count
FROM agg_fact_flights.data
WHERE agg_level = 'all'
    AND time_agg_level = 'year'

UNION ALL

SELECT 'Weather' as name, cancellations_B as count
FROM agg_fact_flights.data
WHERE agg_level = 'all'
    AND time_agg_level = 'year'

UNION ALL

SELECT 'National Air System' as name, cancellations_C as count
FROM agg_fact_flights.data
WHERE agg_level = 'all'
    AND time_agg_level = 'year'

UNION ALL

SELECT 'Security' as name, cancellations_D as count
FROM agg_fact_flights.data
WHERE agg_level = 'all'
    AND time_agg_level = 'year'
```

```sql pie_data
SELECT
    name,
    count as value
FROM ${cancellation_data}
ORDER BY name
```
<ECharts config={
    {
        title: {
            text: 'Total Cancellations Breakdown by Cancellation Reason',
            left: 'center'
        },
        tooltip: {
            trigger: 'item'
        },
        legend: {
            orient: 'vertical',
            left: 'left'
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
    y2=percent_cancellations_A
    title="Cancelled Rate by Month"
    markers=true
    xAxisTitle=Month
    yAxisTitle="Cancelled Rate (%)"
    y2AxisTitle="(Minutes)"
    xTickMarks=true
/>



<!-- Cancelation breakdown by month, winter month effects? -->
<!-- Cancelation breakdown by airport, do airports have any underlaying issues? -->


