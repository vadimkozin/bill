CREATE TABLE _TABLE_CREATE_ (
  `id` int(4) NOT NULL AUTO_INCREMENT COMMENT 'id record',
  `account` int(4) unsigned NOT NULL DEFAULT '0' COMMENT 'условный номер счёта',
  `period` char(7) NOT NULL DEFAULT '-' COMMENT 'период: напр. 2022_01',
  `cid` int(4) unsigned NOT NULL DEFAULT '0' COMMENT 'код клиента',
  `uf` char(1) NOT NULL DEFAULT '-' COMMENT 'тип клиента: u|f',
  `dt` date NOT NULL COMMENT 'дата выписки счета',
  `min` int(4) unsigned NOT NULL DEFAULT '0' COMMENT 'количество городских минут',
  `cost1min` double(9,2) NOT NULL DEFAULT '0.00' COMMENT 'стоимость 1-й минуты',
  `sum` double(9,2) NOT NULL DEFAULT '0.00' COMMENT 'сумма за местную связь без НДС',
  `prim` varchar(80) DEFAULT NULL COMMENT 'примечание',
  `ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'штамп доб/изм. записи',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_account` (`account`),
  UNIQUE KEY `uk_account_cid_period` (`account`,`cid`,`period`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COMMENT='Книга счетов местной/городской связи (mest_book)';