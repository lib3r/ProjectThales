SELECT max(dp.price_date)as end, min(dp.price_date) as start
FROM symbol AS sym
INNER JOIN daily_price AS dp
ON dp.symbol_id = sym.id
WHERE sym.ticker = '0170.HK'