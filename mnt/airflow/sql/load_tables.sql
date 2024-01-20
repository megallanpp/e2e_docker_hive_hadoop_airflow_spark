Select top 20
	C.CODFIL FILCON,
	C.SERCON SERCON,
	C.CODCON CODCON,
	C.CODMOT Cod_Motorista,
	C.TIPDOC Tipo_Documento,
	C.TIPCON Tipo_Cte,
	C.CTE_ID CTE_ID,
	Upper(C.PLACA) Placa,
	C.CODPAG Cod_Cliente_Pagador,
	C.CODREM Cod_Cliente_Remetente,
	C.CODDES Cod_Cliente_Destinatario,
	C.TERENT Cod_Cliente_Terminal_Entrega,
	C.CODLIN Cod_Linha,
	C.TABPAD Cod_Tabela_Frete,
	C.ID_RODIIF Cod_Item_Tarifa,
	Convert(Money, C.FREPES) Valor_Frete_Peso,
	Convert(Money, C.VLRTAR) Valor_Tarifa,
	Convert(Money, C.ICMVLR) Valor_ICMS,
	Convert(Money, C.PISINT) Valor_PIS,
	Convert(Money, C.COFINT) Valor_COFINS,
	Convert(Money, C.TOTFRE) Valor_Total_Frete,
	Convert(Money, C.OUTROS) Valor_Outros,
	Convert(Money, C.VLRPED) Valor_Pedagio,
	Convert(Money, C.VLRCAR) Valor_Carga,
	Convert(Money, C.DESCAR) Valor_Descarga,
	Convert(Money, C.VLRLIQ) Valor_Liquido,
	Convert(Money, C.FREVAL) Valor_ADV,
	Convert(Money, C.VLRGRI) Valor_GRIS,
	C.USUINC Usuario_Inclusao,
	C.DATINC Dt_Inclusao,
	C.CTEDTA Dt_Autorizacao,
	C.DATCAD Dt_Emissao,
	C.SITDUP Cod_Situacao_Financeira,
	C.SITUAC Cod_Situacao, 
	C.OBSCON,
	C.NATOPE AS CFOP,
	C.BASCAL AS Valor_Base_ICMS,
	case  when ca.codanu is not null then 'S'
						else 'N'
					END AS Anulação
From [10.0.1.78,1352].DB_VISUAL_COOTRAVALE.DBO.RODCON C
Left Join [10.0.1.78,1352].DB_VISUAL_COOTRAVALE.DBO.TMP_RODCON_ANULACAO CA	on 
	CA.CODCON = C.CODCON 
	and CA.SERCON = C.SERCON 
	and CA.CODFIL = C.CODFIL
