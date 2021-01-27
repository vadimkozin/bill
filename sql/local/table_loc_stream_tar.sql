CREATE TABLE `_TABLE_CREATE_` (
  `id` int(4) NOT NULL AUTO_INCREMENT COMMENT 'id record',
  `cid` int(4) unsigned NOT NULL DEFAULT '0' COMMENT 'код клиента',
  `lines` int(4) unsigned NOT NULL DEFAULT '0' COMMENT 'кол-во линий',
  `freemin` int(5) NOT NULL DEFAULT '0' COMMENT 'бесплатных минут на 1 линию',
  `prev` double(9,2) NOT NULL DEFAULT '0.00' COMMENT 'плата за 1 минуту  превышения',
  `reg` varchar(32) NOT NULL DEFAULT '-' COMMENT 'шаблон(ы) на отбор номеров  из таблицы из поля Fm',
  `period_on` varchar(8) NOT NULL DEFAULT '-' COMMENT 'период начала рассчета местных связей по потоку (в виде 2012_01) по этому шаблону',
  `period_off` varchar(8) NOT NULL DEFAULT '-' COMMENT 'период окончания рассчета местных связей по потоку (в виде 2013_09) по этому шаблону',
  `dt_on` date DEFAULT NULL COMMENT 'начала рассчета местных связей по потоку (в виде 2012_01) по этому шаблону',
  `dt_off` date DEFAULT NULL COMMENT 'период окончания рассчета местных связей по потоку (в виде 2013_09) по этому шаблону',
  `prim` varchar(80) DEFAULT NULL COMMENT 'примечание',
  `ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'штамп добавления/изменения записи',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_cid` (`cid`)
) ENGINE=MyISAM AUTO_INCREMENT=7 DEFAULT CHARSET=utf8 COMMENT='Тарифы местной связи по потоку(loc_stream_tar)';
