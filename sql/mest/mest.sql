/* клиенты с оплачиваемой местной связью */
SELECT cid, city, bsec FROM tarif.tariff_tel WHERE city>0;

/* сумма к оплате для клиентов с оплачиваемой местной связью*/
SELECT cid, c.CustAlias customer, sum(min) sum_min,  '0.43' AS `cost_1min`, sum(min)*0.43 AS `summa` FROM bill.Y2022M01 d JOIN customers.Cust c ON d.cid=c.CustID WHERE cid IN (470,1303,1309,1310,1314) AND stat='G' GROUP BY cid;

SELECT cid, c.CustAlias customer, sum(min) sum_min,  '0.43' AS `cost_1min`, sum(min)*0.43 AS `summa` FROM bill.Y2022M01 d JOIN customers.Cust c ON d.cid=c.CustID WHERE cid IN (SELECT cid FROM tarif.tariff_tel WHERE city>0) AND stat='G' GROUP BY cid;


/* весь местный трафик клиентов с оплачиваемой местной связью*/
SELECT cid, pid, uf, dt, fm, `to`, min FROM bill.Y2022M01 WHERE cid IN (470,1303,1309,1310,1314) AND stat='G';

/* весь местный трафик клиентов с оплачиваемой местной связью*/
SELECT cid, dt 'date', fm numberA, `to` numberB, min FROM bill.Y2022M01 WHERE cid IN (1310) AND stat='G';

470	КМС-ЦЕНТР	46	0.43	19.78
1309	РЕСО-Гарантия	1	0.43	0.43
1310	МОРБИЗНЕСЦЕНТР	24	0.43	10.32