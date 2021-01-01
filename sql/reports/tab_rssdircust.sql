DROP TABLE IF EXISTS `_TAB_CREATE_`;
CREATE TABLE `_TAB_CREATE_` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `year` int(4) NOT NULL DEFAULT '0' COMMENT 'год',
  `month` int(4) NOT NULL DEFAULT '0' COMMENT 'месяц',
  `cid` int(11) NOT NULL DEFAULT '0' COMMENT 'код клиента',
  `uf` char(1) NOT NULL DEFAULT '-' COMMENT 'тип клиента: u-юрлица, f-физлица',
  `serv` char(2) NOT NULL DEFAULT '-' COMMENT 'вид услуги: MG,MN,VZ',
  `sum` double(8,2) NOT NULL DEFAULT '0.00' COMMENT 'Сумма без НДС',
  `nds` double(8,2) NOT NULL DEFAULT '0.00' COMMENT 'НДС',
  `sumoper` double(8,2) NOT NULL DEFAULT '0.00' COMMENT 'Агентская сумма без НДС по тарифам оператора',
  PRIMARY KEY (`id`),
  UNIQUE KEY `yearmonthcidserv` (`year`,`month`,`cid`,`serv`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;
