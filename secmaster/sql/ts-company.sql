SELECT dp.price_date, dp.adj_close_price, dp.created_date
         FROM symbol AS sym
         INNER JOIN daily_price AS dp
         ON dp.symbol_id = sym.id
         WHERE sym.ticker = '0321.HK'
         ORDER BY dp.price_date ASC;