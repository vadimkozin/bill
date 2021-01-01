CREATE TABLE `_TABLE_CREATE_` (
  `id` int(11) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'Уникальный код записи',
  `year` int(4) NOT NULL DEFAULT '0' COMMENT '(Year) Год',
  `month` int(4) NOT NULL DEFAULT '0' COMMENT '(Mon) Месяц',
  `period` char(8) NOT NULL DEFAULT '-' COMMENT 'Период, за который выставили счет(извещение): Y2015_11',
  `account` int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'Номер счета(извещения)',
  `cid` int(4) NOT NULL DEFAULT '0' COMMENT 'Код клиента с 01-10-2015 uf=f',
  `pid` int(4) NOT NULL DEFAULT '0' COMMENT 'Код код клиента квартирного сектора-CustKS!PID (c 01-10-2015 pid=0)',
  `date` date NOT NULL COMMENT 'Дата выставления счёта(извещения)',
  `calls` int(4) NOT NULL DEFAULT '0' COMMENT 'Количество связей(разговоров',
  `sum` double(8,2) NOT NULL DEFAULT '0.00' COMMENT 'Сумма к оплате (НДС включён)',
  `paydate` date NOT NULL DEFAULT '1111-11-11' COMMENT 'Оплачено - дата',
  `paysum` double(8,2) NOT NULL DEFAULT '0.00' COMMENT 'Оплачено - сумма',
  `paydebt` double(8,2) NOT NULL DEFAULT '0.00' COMMENT 'Долг = sum - paysum',
  `fa` char(2) NOT NULL DEFAULT '*' COMMENT 'Флажок: + счет актуален; - счет не нужен (удален)',
  `ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Штамп времени создания записи',
  PRIMARY KEY (`id`),
  UNIQUE KEY `year_month_account` (`year`,`month`,`account`)
) ENGINE=MyISAM  DEFAULT CHARSET=utf8 COMMENT='Книга извещений физлиц для access-форм с 01-11-2015';
