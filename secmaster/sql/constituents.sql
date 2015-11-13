select id, ticker from 
	(select symbol.id, ticker, max(daily_price.price_date) as end from symbol 
	join daily_price on (symbol.id = daily_price.symbol_id)
	where symbol.id in(select symbol_id from daily_price) and ticker like '%.hk%' 
	group by daily_price.symbol_id) as table1
where end > '2015-10-01'
