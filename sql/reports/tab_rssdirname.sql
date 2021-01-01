DROP TABLE IF EXISTS `_TAB_CREATE_`;
CREATE TABLE `_TAB_CREATE_` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `year` int(4) NOT NULL DEFAULT '0' COMMENT 'год',
  `month` int(4) NOT NULL DEFAULT '0' COMMENT 'месяц',
  `serv` char(2) NOT NULL DEFAULT '-' COMMENT 'Вид услуги: МГ, МН, ВЗ',
  `dir` char(50) NOT NULL DEFAULT '-' COMMENT 'Наименование направления',
  `code` char(10) NOT NULL DEFAULT '-' COMMENT 'Код направления, например, 7495, 8812',
  `min` int(5) NOT NULL DEFAULT '0' COMMENT 'Минуты',
  `sum` double(8,2) NOT NULL DEFAULT '0.00' COMMENT 'Сумма без НДС',
  `nds` double(8,2) NOT NULL DEFAULT '0.00' COMMENT 'НДС',
  `account` int(4) NOT NULL DEFAULT '0' COMMENT 'Лицевой счёт биллинга МТС',
  PRIMARY KEY (`id`),
  UNIQUE KEY `yearmonthdir` (`year`,`month`,`dir`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;
