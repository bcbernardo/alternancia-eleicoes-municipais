#################################### PREPARACAO ###############################

# instalar e carregar pacote data.table
if (!require("data.table")) {install.packages("data.table")}
library(data.table)

if (!require("electionsBR")) {install.packages("electionsBR")}

setDTthreads(8)
setwd("D:/alternancia-eleicoes-municipais/etc/TSE")

# obter correspondencias entre codigos de municipio do TSE e do IBGE
mun_codes <- fread("http://basedosdados.org/dataset/b5878df7-e259-44bc-8903-4feb8d56945a/resource/c1deb363-ffba-4b1e-95dc-c5e08311852e/download/municipios.csv")


###################### PERFIL DO ELEITORADO POR MUNICIPIO #####################

consolidado <- data.table()
for (year in c(2012, 2016)) {
    eleitorado <- as.data.table(electionsBR::voter_profile(year))
    setnames(eleitorado, c("Referencia", "UF", "Municipio", "id_TSE",
                           "NR_ZONA", "SEXO", "FAIXA_ETARIA", "ESCOLARIDADE",
                           "QTD_ELEITORES"))
    # incorporar codigo de sete digitos do ibge
    eleitorado <- eleitorado[mun_codes[, .(id_TSE, GEOCOD_IBGE = id_munic_7)], 
                             on = "id_TSE"]
    # Reduzir numero de categorias de faixas etarias
    eleitorado[!(FAIXA_ETARIA %in% c("SUPERIOR A 79 ANOS", "INVÁLIDA")), 
                  IDADE := as.integer(substr(FAIXA_ETARIA, 1, 3))]
    eleitorado[IDADE > 15 & IDADE < 35, FAIXA_ETARIA := "de16a34"]
    eleitorado[IDADE > 34 & IDADE < 59, FAIXA_ETARIA := "de35a59"]
    eleitorado[IDADE > 59 | FAIXA_ETARIA == "SUPERIOR A 79 ANOS", 
                  FAIXA_ETARIA := "mais60"]
    eleitorado[FAIXA_ETARIA == "INVÁLIDA", FAIXA_ETARIA := NA_character_]
    # marcar registros relativoas a eleitores acima dos 25 anos, para analise da
    # escolaridade
    eleitorado[IDADE > 24 | FAIXA_ETARIA == "mais60", ACIMA_25 := TRUE]
    # Encurtar e agrupar categorias de escolaridade
    eleitorado[ESCOLARIDADE == "ANALFABETO", ESCOLARIDADE := "ANALF"]
    eleitorado[ESCOLARIDADE == "LÊ E ESCREVE" | 
                  ESCOLARIDADE == "ENSINO FUNDAMENTAL INCOMPLETO", 
                  ESCOLARIDADE := "EFi"]
    eleitorado[ESCOLARIDADE == "ENSINO FUNDAMENTAL COMPLETO" | 
                  ESCOLARIDADE == "ENSINO MÉDIO INCOMPLETO", 
                  ESCOLARIDADE := "EF"]
    eleitorado[ESCOLARIDADE == "ENSINO MÉDIO COMPLETO" | 
                  ESCOLARIDADE == "SUPERIOR INCOMPLETO", ESCOLARIDADE := "EM"]
    eleitorado[ESCOLARIDADE == "SUPERIOR COMPLETO", ESCOLARIDADE := "ES"]
    eleitorado[ESCOLARIDADE == "NÃO INFORMADO", ESCOLARIDADE := NA_character_]
    # encurtar nomes das categorias de sexo
    eleitorado[SEXO == "FEMININO", SEXO := "F"]
    eleitorado[SEXO == "MASCULINO", SEXO := "M"]
    eleitorado[SEXO == "NÃO INFORMADO", SEXO := NA_character_]
    # agregar por municipio
    eleitorado <- eleitorado[, .(QTD_ELEITORES = sum(QTD_ELEITORES)), 
                              by = .(Referencia, GEOCOD_IBGE, SEXO, 
                                     FAIXA_ETARIA, ESCOLARIDADE, ACIMA_25)]
    # guardar total de eleitores por municipio
    eleitorado_total <- eleitorado[, .(Referencia = year,
                                       TOTAL_ELEITORES = sum(QTD_ELEITORES)), 
                                   by = "GEOCOD_IBGE"]
    eleitorado_total[TOTAL_ELEITORES > 200000, TURNO_2 := 1L]
    eleitorado_total[GEOCOD_IBGE %in% c(1400100, 1721000, 3205309), 
                     TURNO_2 := 1L]
    eleitorado_total[TOTAL_ELEITORES <= 200000, TURNO_2 := 0L]
    # variaveis para colunas
    eleitorado_sexo <- dcast(eleitorado, GEOCOD_IBGE ~ SEXO, 
                             value.var = "QTD_ELEITORES", fun.agg = sum)
    eleitorado_faixaetaria <- dcast(eleitorado, GEOCOD_IBGE ~ FAIXA_ETARIA, 
                                     value.var = "QTD_ELEITORES", fun.agg = sum)
    eleitorado_escolaridade <- dcast(eleitorado[ACIMA_25 == T,], 
                                     GEOCOD_IBGE ~ ESCOLARIDADE, 
                                     value.var = "QTD_ELEITORES", fun.agg = sum)
    # variaveis como proporcoes
    eleitorado_sexo <- eleitorado_sexo[,.(GEOCOD_IBGE, prop_sexos = F/M)]
    eleitorado_faixaetaria <- eleitorado_faixaetaria[,.(
      GEOCOD_IBGE, prop_16a34 = de16a34 / rowSums(.SD), 
      prop_35a59 = de35a59 / rowSums(.SD), prop_60mais = mais60 / rowSums(.SD)),
      .SDcols = c("de16a34", "de35a59", "mais60")]
    eleitorado_escolaridade <- eleitorado_escolaridade[,
      .(GEOCOD_IBGE, prop_analf = ANALF/rowSums(.SD), 
        prop_EF = EF/rowSums(.SD), prop_EM = ES/rowSums(.SD)),
      .SDcols = c("ANALF", "EFi", "EF", "EM", "ES")]
    
    # reincorporar total de eleitores
    eleitorado <- eleitorado_total[eleitorado_sexo, on = "GEOCOD_IBGE"][
      eleitorado_faixaetaria, on = "GEOCOD_IBGE"][
        eleitorado_escolaridade, on = "GEOCOD_IBGE"]
    # juntar anos
    consolidado <- rbind(consolidado, eleitorado) 
    # remover arquivos intermediarios
    remove(eleitorado, eleitorado_escolaridade, eleitorado_faixaetaria, 
           eleitorado_sexo, eleitorado_total, year)
}
saveRDS(consolidado, "./dados-tratados-eleitorado.rds")
