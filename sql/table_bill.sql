CREATE TABLE `_TABLE_CREATE_` (
  `id` int(7) unsigned NOT NULL AUTO_INCREMENT,
  `cid` int(5) unsigned DEFAULT '0' COMMENT 'код клиента',
  `uf` char(1) NOT NULL DEFAULT '-' COMMENT 'Тип клиента: u-Юр.лицо; f-Физ.лицо',
  `pid` int(5) DEFAULT '0' COMMENT 'код частного клиента для cid=549',
  `dt` datetime NOT NULL COMMENT 'дата/время звонка',
  `dnw` char(1) NOT NULL DEFAULT '-' COMMENT 'DNW: D-день; N-ночь; W-выходной',
  `b` char(1) NOT NULL DEFAULT '-' COMMENT '''+''/''-'' - звонок нужно/не нужно биллинговать',
  `ok` char(1) NOT NULL DEFAULT '-' COMMENT '''+''/''-'' - звонок обработан/не обработан',
  `fm` char(12) NOT NULL DEFAULT '-' COMMENT 'от кого (с аппаратуры: вход. на порт)',
  `fmX` char(16) NOT NULL DEFAULT '-' COMMENT 'от кого (с аппаратуры: исход. с порта)',
  `fm2` char(12) NOT NULL DEFAULT '-' COMMENT 'от кого (для клиента)',
  `fm3` char(12) NOT NULL DEFAULT '-',
  `to` char(25) NOT NULL DEFAULT '-' COMMENT 'к кому (с аппаратуры: вход. на порт)',
  `toX` char(25) NOT NULL DEFAULT '-' COMMENT 'к кому (с аппаратуры: исход. c порта)',
  `to2` char(16) NOT NULL DEFAULT '-' COMMENT 'к кому (для клиента)',
  `sec` int(7) NOT NULL DEFAULT '0' COMMENT 'прод. разговора в секундах',
  `min` int(7) NOT NULL DEFAULT '0' COMMENT 'прод. разгоаора в минутах',
  `sts` char(3) NOT NULL DEFAULT '-' COMMENT '[mg;mn;vz;gd;mgs] - из источника (mgs=S)',
  `stat` char(2) NOT NULL DEFAULT '-' COMMENT '[GMWSZ]: G-город;M-МГ;W-МН;S-сотовая Россия;Z-сотовая Москва',
  `st` char(2) NOT NULL DEFAULT '-' COMMENT 'MG(M+S); MN(W); VZ(Z); GD(G)',
  `st2` char(2) NOT NULL DEFAULT '-',
  `vid` int(4) NOT NULL DEFAULT '0' COMMENT '=tabCodeVM!id для stat=V',
  `code` char(8) NOT NULL DEFAULT '-' COMMENT 'код направления',
  `zona` int(3) DEFAULT '0' COMMENT 'зона (0-6 Российские зоны МГ)',
  `tid` int(4) NOT NULL DEFAULT '0' COMMENT 'код тарифного плана',
  `org` char(1) NOT NULL DEFAULT '-' COMMENT 'организ.[RGI]: R-клиенты ООО РСС; G-ФГУП РСИ;I-внутрент расходы',
  `org2` char(1) NOT NULL DEFAULT '-',
  `sum` double(8,3) unsigned DEFAULT '0.000' COMMENT 'стоим. для клиента',
  `sum2` double(8,3) unsigned DEFAULT '0.000' COMMENT 'стоим. для клиента (без НДС)',
  `sum626` double(10,5) DEFAULT '0.00000' COMMENT 'кл. стоим.626(дог. ФГУП РСИ и МТС на 626xxxx)',
  `sum642` double(10,5) DEFAULT '0.00000' COMMENT 'кл. стоим.642(дог. РСС и МТС на 642xxxx)',
  `x_sumTal` double(8,3) unsigned DEFAULT '0.000' COMMENT 'стоим. по тарифам Транзитек для агента',
  `x_sumTal2` double(8,3) unsigned DEFAULT '0.000',
  `x_sumTalX` double(8,3) unsigned DEFAULT '0.000',
  `x_sumTalX2` double(8,3) unsigned DEFAULT '0.000',
  `x_sumTz` double(9,4) unsigned DEFAULT '0.0000' COMMENT 'стоим.ПоТарифамТЗ',
  `eq` char(12) NOT NULL DEFAULT '-' COMMENT 'оборудование',
  `eqid` int(6) NOT NULL DEFAULT '0' COMMENT 'код записи в таблице  откуда скопирована запись (->eq!id)',
  `x_codeSov` char(12) NOT NULL DEFAULT '-' COMMENT 'код.Совинтел',
  `x_sumSov` double(9,3) DEFAULT '0.000' COMMENT 'стоим.Совинтел.ДляАгента',
  `x_sumSovCust` double(9,3) DEFAULT '0.000' COMMENT 'стоим.Совинтел.ДляКлиента',
  `x_naprSov` varchar(40) NOT NULL DEFAULT '-' COMMENT 'напр.Совинтел',
  `x_regSov` varchar(16) NOT NULL DEFAULT '-' COMMENT 'регион.Совинтел',
  `x_codeMTT` char(12) NOT NULL DEFAULT '-' COMMENT 'код.МТТ',
  `x_sumMTT` double(10,5) DEFAULT '0.00000' COMMENT 'стоим.МТТ для агента',
  `codeKOM` char(12) NOT NULL DEFAULT '-' COMMENT 'код.Комстар',
  `naprKOM` char(40) NOT NULL DEFAULT '-' COMMENT 'напр.Комстар',
  `regKOM` char(16) NOT NULL DEFAULT '-' COMMENT 'регион.Комстар',
  `sumKOM` double(10,5) DEFAULT '0.00000' COMMENT 'стоим.Комстар.ДляАгента',
  `sumKOMMAX` double(10,5) DEFAULT '0.00000' COMMENT 'стоим.КомстарМакс.ДляКлиенто',
  `l1` char(7) NOT NULL DEFAULT '-' COMMENT 'm200: входящая линия',
  `l2` char(7) NOT NULL DEFAULT '-' COMMENT 'm200: исходящая линия',
  `link` char(6) NOT NULL DEFAULT '-' COMMENT 'm200: вход-исход, напр. m5-11',
  `p` char(1) NOT NULL DEFAULT '-' COMMENT 'Провайдер',
  `op` char(2) NOT NULL DEFAULT '-' COMMENT 'Оператор с 01-10-13',
  `x_p2` char(1) NOT NULL DEFAULT '-' COMMENT 'Пров.Транзитек.РеестрДляСовинтел',
  `pr` int(3) unsigned DEFAULT NULL COMMENT 'Причина отбоя',
  `f` char(1) NOT NULL DEFAULT '-' COMMENT 'флажок',
  `rule` char(4) NOT NULL DEFAULT '-' COMMENT 'правило (таблица), по которому был определен клиент(cid)',
  `rid` int(4) DEFAULT '0' COMMENT 'ссылка внутри таблицы из rule',
  `ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `_f` char(1) NOT NULL DEFAULT '-' COMMENT '# флажок: + запись пробиллингована',
  `_cid` int(4) NOT NULL DEFAULT '0' COMMENT '# код клиента фактический',
  `_uf` char(1) NOT NULL DEFAULT '-' COMMENT 'Тип клиента: u-Юр.лицо; f-Физ.лицо',
  `_pid` int(5) DEFAULT '0' COMMENT 'код частного клиента для cid=549',
  `_cidr` int(4) NOT NULL DEFAULT '0' COMMENT '# код клиента по бизнес-правилам',
  `_cidb` int(4) NOT NULL DEFAULT '0' COMMENT '# код клиента из тарифного плана (по нему считаем)',
  `_org` char(1) NOT NULL DEFAULT '-' COMMENT '# организация ',
  `_orgr` char(1) NOT NULL DEFAULT '-' COMMENT '# организация по бизнес-правилам',
  `_orgb` char(1) NOT NULL DEFAULT '-' COMMENT '# организация ппара для _cidb',
  `_stat` char(1) NOT NULL DEFAULT '-' COMMENT '# тип звонка: M,W,Z,S,G,V',
  `_sts` char(3) NOT NULL DEFAULT '-' COMMENT '# тип звонка: mg,mn,vz,mgs,gd,vm,ab,kz',
  `_zona` int(1) NOT NULL DEFAULT '0' COMMENT '# тарифная зона',
  `_code` char(12) NOT NULL DEFAULT '-' COMMENT '# тел_код',
  `_nid` int(4) NOT NULL DEFAULT '0' COMMENT '# код направления',
  `_desc` varchar(80) NOT NULL DEFAULT '-' COMMENT '# направление',
  `_dnw` char(1) NOT NULL DEFAULT '-' COMMENT 'DNW: D-день; N-ночь; W-выходной',
  `_tid` int(4) NOT NULL DEFAULT '0' COMMENT 'код тарифного плана',
  `_tar` double(8,3) NOT NULL DEFAULT '0.000' COMMENT '# клиент_тариф 1 мин',
  `_tara` double(8,3) NOT NULL DEFAULT '0.000' COMMENT '# агент_тариф 1 мин',
  `_sum` double(8,3) NOT NULL DEFAULT '0.000' COMMENT '# клиент_сумма, для физлиц НДС включён',
  `_sum2` double(8,3) NOT NULL DEFAULT '0.000' COMMENT '# клиент_сумма без НДС',
  `_suma` double(8,3) NOT NULL DEFAULT '0.000' COMMENT '# агент_сумма',
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;
