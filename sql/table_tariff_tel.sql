CREATE TABLE `_TABLE_CREATE_` (
  `id` int(4) NOT NULL AUTO_INCREMENT COMMENT 'id record',
  `tid` int(4) unsigned NOT NULL DEFAULT '0' COMMENT 'Код тарифа (tid=tariff_id)',
  `name` varchar(80) NOT NULL DEFAULT '-' COMMENT 'Название тарифа',
  `report` varchar(80) NOT NULL DEFAULT '-' COMMENT 'Название для заголовка в отчёте про тариф',
  `prim` varchar(200) DEFAULT NULL COMMENT 'примечание',
  `ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'штамп добавления/изменения записи',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_tar_id` (`tid`)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='Тарифные планы телефонии (tariff_tel)';