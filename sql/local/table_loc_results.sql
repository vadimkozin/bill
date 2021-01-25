CREATE TABLE `_TABLE_CREATE_` (
  `id` int(4) NOT NULL AUTO_INCREMENT COMMENT 'id record',
  `account` int(4) unsigned NOT NULL DEFAULT '0' COMMENT 'номер счёта -> loc_book.account',
  `cid` int(4) unsigned NOT NULL DEFAULT '0' COMMENT 'код клиента',
  `period` char(7) NOT NULL DEFAULT '-' COMMENT 'период: напр. 2021_01',
  `number` varchar(10) NOT NULL DEFAULT '-' COMMENT 'номер телефона',
  `min` int(4) unsigned NOT NULL DEFAULT '0' COMMENT 'всего минут за месяц по номеру number',
  `abmin` int(4) unsigned NOT NULL DEFAULT '0' COMMENT 'абонентских минут, входящих в абон_плату',
  `cost1min` double(9,2) NOT NULL DEFAULT '0.00' COMMENT 'плата за 1 минуту превышения',
  `sum` double(9,2) NOT NULL DEFAULT '0.00' COMMENT 'сумма за превышающие минуты = (min-abmin)*cost1min',
  `prim` varchar(200) DEFAULT NULL COMMENT 'примечание',
  `ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'штамп доб/изм. записи',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_account_cid_period_number` (`account`,`cid`,`period`,`number`)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='Итоговые данные местной связи (loc_results)';