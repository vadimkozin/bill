CREATE TABLE `_TABLE_CREATE_` (
  `id` int(4) NOT NULL AUTO_INCREMENT COMMENT 'id record',
  `number` char(10) NOT NULL DEFAULT '-' COMMENT 'Тефонный 7-значный номер (626xxxx)',
  `xnumber` char(11) DEFAULT NULL COMMENT 'полный номер: 7495xxxxxxx  для 626-х и 642-х',
  `cid` int(5) unsigned DEFAULT '0' COMMENT 'Код клиента',
  `pid` int(5) unsigned DEFAULT '0' COMMENT 'Код клиента квартирного сектора (для cid=549)',
  `prim` varchar(80) DEFAULT NULL COMMENT 'примечание',
  `ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'штамп добавления/изменения записи',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_number_cid` (`number`,`cid`)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='Номера А2 (a2_numbers)';