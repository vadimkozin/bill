CREATE TABLE `_TABLE_CREATE_` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `year` int(4) NOT NULL DEFAULT '0' COMMENT 'год',
  `month` int(4) NOT NULL DEFAULT '0' COMMENT 'месяц',
  `cid` int(11) NOT NULL DEFAULT '0' COMMENT 'код клиента',
  `pid` int(11) NOT NULL DEFAULT '0' COMMENT 'код клиента квартирного сектора',
  `uf` char(1) NOT NULL DEFAULT '-' COMMENT 'тип клиента: u-юрлица, f-физлица',
  `serv` char(2) NOT NULL DEFAULT '-' COMMENT 'вид услуги: MG, MN, VZ (MG=M+S, MN=W, VZ=Z)',
  `stat` char(2) NOT NULL DEFAULT '-' COMMENT 'вид услуги: M, W, S, Z (M+S=MG, W=MN, Z=VZ)',
  `calls` int(4) NOT NULL DEFAULT '0' COMMENT 'вызовов(звонков) по направлению',
  `dir` char(80) NOT NULL DEFAULT '-' COMMENT 'направление',
  `sumraw` double(8,2) NOT NULL DEFAULT '0.00' COMMENT 'Сумма без НДС',
  `sumcust` double(8,2) NOT NULL DEFAULT '0.00' COMMENT 'Сумма клиенту (для физ-лиц с НДС)',
  `sumoper` double(8,2) NOT NULL DEFAULT '0.00' COMMENT 'Агентская сумма без НДС по тарифам оператора',
  `sumsec` int(4) NOT NULL DEFAULT '0' COMMENT 'Общее кол-во сееунд',
  `summin` int(4) NOT NULL DEFAULT '0' COMMENT 'Общее кол-во минут',
  PRIMARY KEY (`id`),
  UNIQUE KEY `year_month_cid_pid_stat_dir` (`year`,`month`,`cid`,`pid`,`stat`,`dir`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COMMENT='исходная таблица из данных YxxxxMxx';
