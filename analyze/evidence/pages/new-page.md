
## Hello Evidence

This is a new page in Evidence.

### This is a lower level header
This is some *italic* and **bold** text.

```my_query_summary
select * from needful_things.my_query
```

<DataTable
    data={my_query_summary}
    title="My Query Summary"
/>



```my_query_summary_top100
select
   order_datetime,
   first_name,
   last_name,
   email
from needful_things.my_query
order by order_datetime desc
limit 100
```


```orders_by_month
select order_month, count(*) as orders from needful_things.my_query
group by order_month order by order_month desc
limit 12
```

<BarChart
    data={orders_by_month}
    x=order_month
    y=orders
	xFmt="mmm yyyy"
	xAxisTitle="Month"
	yAxisTitle="Orders"
/>