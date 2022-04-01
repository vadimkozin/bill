/* клиенты с оплачиваемой местной связью */
SELECT cid, city, bsec FROM tarif.tariff_tel WHERE city>0;

/* сумма к оплате для клиентов с оплачиваемой местной связью*/
SELECT cid, c.CustAlias customer, sum(min) sum_min,  '0.43' AS `cost_1min`, sum(min)*0.43 AS `summa` FROM bill.Y2022M03 d JOIN customers.Cust c ON d.cid=c.CustID WHERE cid IN (470,1303,1309,1310,1314) AND stat='G' GROUP BY cid;

SELECT cid, c.CustAlias customer, sum(min) sum_min,  '0.43' AS `cost_1min`, sum(min)*0.43 AS `summa` FROM bill.Y2022M03 d JOIN customers.Cust c ON d.cid=c.CustID WHERE cid IN (SELECT cid FROM tarif.tariff_tel WHERE city>0) AND stat='G' GROUP BY cid;


/* весь местный трафик клиентов с оплачиваемой местной связью*/
SELECT cid, pid, uf, dt, fm, `to`, min FROM bill.Y2022M03 WHERE cid IN (470,1303,1309,1310,1314) AND stat='G';

/* весь местный трафик клиентов с оплачиваемой местной связью*/
SELECT cid, dt 'date', fm numberA, `to` numberB, min FROM bill.Y2022M03 WHERE cid IN (1310) AND stat='G';

// один запрос - сумма к оплате для клиентов с оплачиваемой местной связью
SELECT cid, c.CustAlias customer, sum(min) sum_min,
replace((SELECT x.city FROM tarif.tariff_tel x WHERE x.cid=d.cid),'.',',') cost_1min,
replace((SELECT x.city FROM tarif.tariff_tel x WHERE x.cid=d.cid)*sum(min), '.',',') summa
FROM bill.Y2022M03 d JOIN customers.Cust c ON d.cid=c.CustID WHERE cid IN (SELECT cid FROM tarif.tariff_tel WHERE city>0) AND stat='G' GROUP BY cid;

cid	    customer	    sum_min	    cost_1min	summa
470 	КМС-ЦЕНТР	    417	        0,43	    179,31
1309	РЕСО-Гарантия	13	        0,43	    5,59
1310	МОРБИЗНЕСЦЕНТР	74	        0,43	    31,82
1320	Хухтамаки	    948	        0,70	    663,60

