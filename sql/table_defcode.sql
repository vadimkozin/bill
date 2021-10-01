CREATE TABLE `_TABLE_CREATE_` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `abc` char(3) NOT NULL DEFAULT '-' COMMENT 'abc/def',
  `fm` char(7) NOT NULL DEFAULT '-' COMMENT 'от (начало диапазона)',
  `to` char(7) NOT NULL DEFAULT '-' COMMENT 'до (конец диапазона)',
  `capacity` int(9) NOT NULL COMMENT 'ёмкость (всего номеров)',
  `zona` int(4) NOT NULL DEFAULT '-1' COMMENT 'тарифная зона = defRegion.zona',
  `stat` char(2) NOT NULL DEFAULT '-' COMMENT 'stat (vz;mg;sp) vz-внутризоновая;mg-межгород; sp-спутник',
  `oper` varchar(150) NOT NULL DEFAULT '' COMMENT 'оператор связи',
  `region` varchar(120) NOT NULL DEFAULT '' COMMENT 'регион',
  `area` varchar(100) NOT NULL DEFAULT '-' COMMENT 'пространство включающее в себя регион',
  `ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'штамп времени',
  PRIMARY KEY (`id`),
  KEY `abc` (`abc`),
  KEY `fm` (`fm`),
  KEY `to` (`to`)
) ENGINE=MyISAM AUTO_INCREMENT=7981 DEFAULT CHARSET=utf8;