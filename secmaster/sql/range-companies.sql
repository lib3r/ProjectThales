SELECT symbol_id, ticker, max(price_date) as end ,min(price_date) 
FROM security_master.daily_price
JOIN symbol
where symbol_id = symbol.id
group by symbol_id 
order by end asc limit 1000