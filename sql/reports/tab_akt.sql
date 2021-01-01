CREATE TABLE `_TABLE_CREATE_` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT 'уникальный код записи',
  `year` int(4) NOT NULL DEFAULT '0' COMMENT 'год',
  `month` int(4) NOT NULL DEFAULT '0' COMMENT 'месяц',
  `period` char(8) NOT NULL DEFAULT '-' COMMENT 'период, за который выставили счет(извещение): Y2015_11',
  `stat` char(4) NOT NULL DEFAULT '-' COMMENT 'услуги MWS (МГ+МН+СФ) или Z (внутризоновых) вызовов или 800(free call)',
  `min` int(4) NOT NULL DEFAULT '0' COMMENT 'количество минут',
  `min2` int(4) NOT NULL DEFAULT '0' COMMENT 'общее кол-во секунд переведенное в минуты (min2=0 для stat=800)',
  `min3` int(4) NOT NULL DEFAULT '0' COMMENT 'общее кол-во секунд переведенное в минуты (min3=реальное число для stat=800)',

  `sum` double(8,2) NOT NULL DEFAULT '0.00' COMMENT 'начислено (без НДС)',
  `us` double(8,2) NOT NULL DEFAULT '0.00' COMMENT 'условная стоимость услуг (без НДС)',
  `pi` double(8,2) NOT NULL DEFAULT '0.00' COMMENT 'платеж за инициирование вызова(без НДС)',
  `av` double(8,2) NOT NULL DEFAULT '0.00' COMMENT 'агентское вознаграждение (без НДС)',
  `nds` double(8,2) NOT NULL DEFAULT '0.00' COMMENT 'НДС вознаграждения',
  `ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'штам времени',
  PRIMARY KEY (`id`),
  UNIQUE KEY `year_month_stat` (`year`,`month`,`stat`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COMMENT='акт сдачи-приёмки, подтверждающего выполнение обязательств оператором';
