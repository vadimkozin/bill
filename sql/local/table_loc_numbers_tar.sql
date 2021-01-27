CREATE TABLE `_TABLE_CREATE_` (
  `id` int(4) NOT NULL AUTO_INCREMENT COMMENT 'id record',
  `tid` int(4) unsigned NOT NULL DEFAULT '0' COMMENT 'Код тарифа в тарифах местной связи',
  `name` varchar(80) NOT NULL DEFAULT '-' COMMENT 'Название тарифа',
  `abmin` int(4) unsigned NOT NULL DEFAULT '0' COMMENT 'абонентских минут (то что входит в абон_плату)',
  `cost1min` double(9,2) NOT NULL DEFAULT '0.00' COMMENT 'плата за 1 минуту  превышения',
  `prim` varchar(200) DEFAULT NULL COMMENT 'примечание',
  `ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'штамп добавления/изменения записи',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_tar_id` (`tid`)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='Тарифные планы местной связи по номерам (loc_numbers_tar)';