CREATE TABLE `_TABLE_CREATE_` (
  `id` int(11) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'уникальный код записи',
  `year` int(4) NOT NULL DEFAULT '0' COMMENT 'год',
  `month` int(4) NOT NULL DEFAULT '0' COMMENT 'месяц',
  `account` int(11) UNSIGNED NOT NULL DEFAULT '0' COMMENT '(Счет) номер счёта',
  `serv_id` int(11) NOT NULL DEFAULT '0' COMMENT '(IDУслуга) код услуги',
  `serv` char(2) NOT NULL DEFAULT '-' COMMENT 'вид услуги: MG, MN, VZ (MG=M+S, MN=W, VZ=Z)',
  `nn` int(4) NOT NULL DEFAULT '0' COMMENT '(NN) порядковый номер услуги для данного счета',
  `sum` double(8,2) NOT NULL DEFAULT '0.00' COMMENT '(Сумма) Price:( Цена )',
  `unit` char(1) NOT NULL DEFAULT '-' COMMENT "(Unit) единица измерения: s=штук; m-метры; k-комплект; u-услуги; z-км.; q",
  `amount` int(4) NOT NULL DEFAULT '0' COMMENT '(Amount) количество штук ',
  `nds` double(8,2) NOT NULL DEFAULT '0.00' COMMENT '(НДС) ндс',
  `vsego` double(8,2) NOT NULL DEFAULT '0.00' COMMENT '(Всего) сумма платежа  ( sum*amount ) + nds',
  `prim` char(50) NOT NULL DEFAULT '' COMMENT '(SrvAdd)содержимое этого поля добавляется к Названию услуги',
  `ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP COMMENT '(DTime) штам времени',
  PRIMARY KEY (`id`),
  UNIQUE KEY `year_month_account_servid` (`year`,`month`,`account`, `serv_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COMMENT='подробно услуги для книги продаж для access-форм';