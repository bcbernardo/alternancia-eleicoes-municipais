################################# PREPARACAO ###################################

# (instalar e) carregar  pacote microdadosBrasil
if (!require("devtools")) {install.packages("devtools")}
if (!require("microdadosBrasil")) {
  install.packages("Rcpp")
  devtools::install_github("lucasmation/microdadosBrasil")}
library(microdadosBrasil)

if (!require("electionsBR")) install.packages("electionsBR")

if (!require("archive")) {
  devtools::install_github('jimhester/archive')}

# instalar e carregar pacote ipeadatar
if (!require("ipeadatar")) install.packages("ipeadatar")
library(ipeadatar)

# instalar e carregar pacote GmAMisc (para deteccao de outliers)
if (!require("outliers")) install.packages("outliers")
library(outliers)

# (instalar e) carregar pacote data.table
if (!require("data.table")) install.packages("data.table")
library(data.table)

setwd("D:/alternancia-eleicoes-municipais/etc/RAIS")
setDTthreads(8)


######################### DOWNLOAD E PRE-PROCESSAMENTO #########################

states <- electionsBR::uf_br()
mun_codes <- fread("http://basedosdados.org/dataset/b5878df7-e259-44bc-8903-4feb8d56945a/resource/c1deb363-ffba-4b1e-95dc-c5e08311852e/download/municipios.csv")

consolidado <- data.table(GEOCOD_IBGE = integer(), ANO = integer(),
                          Servidor_Pref = logical(), massa_salarial = numeric(),
                          remun_hora = numeric(), num_empregos = integer())

for (year in c(2005:2007, 2009:2011, 2013:2015)) {
  # baixar os arquivos fonte, se necessario
  for (state in states) {
    folder <- paste0("./", as.character(year), "/", state, as.character(year))
    compressed <- paste0(folder, ".7z")
    if (!file.exists(folder)) {
      print(paste0(folder, " not found."))
      if (!file.exists(compressed)) {
        print(paste0(compressed, " not found."))
        download_sourceData("RAIS", i = year, unzip = T, replace = F)}
      archive_extract(compressed, dir = folder)
      file.remove(compressed)}}
  # ler os arquivos
  vinculos <- read_RAIS(ft = "vinculos", i = year, root_path = ".", 
                             vars_subset = c("Município", "Natureza Jurídica",
                                             "Mês Admissão", "Mês Desligamento",
                                             "Qtd Hora Contr",
                                             "Vl Remun Média Nom"))
  vinculos <- as.data.table(na.omit(vinculos))
  # incorporar codigo de sete digitos do IBGE
  vinculos <- vinculos[mun_codes[,.(GEOCOD_IBGE = id_munic_7, 
                                    Município = id_munic_6)], 
                       on = "Município", nomatch = 0]
  # transformar colunas
  vinculos[, `:=`(id = 1:nrow(vinculos), ANO=year, 
                  Remun=as.numeric(sub(",", ".", `Vl Remun Média Nom`, fixed=T)),
                  Servidor_Pref = is.element(`Natureza Jurídica`, 
                                             c(1031, 1120, 1155, 1180)),
                  Mes_Adm = as.integer(`Mês Admissão`),
                  Mes_Desl = as.integer(`Mês Desligamento`))]
  # verificar se foi admitido ou demitido no ano, e o numero de meses trabalhados
  vinculos[, `:=`(Admitido_Ano = (Mes_Adm != 0 & !is.na(Mes_Adm)), 
                  Deslig_Ano = (Mes_Desl != 0 & !is.na(Mes_Desl)))]
  vinculos[Mes_Adm == 0 | is.na(Mes_Adm), Mes_Adm := 1]
  vinculos[Mes_Desl == 0 | is.na(Mes_Desl), Mes_Desl := 12]
  vinculos[, Meses_Trab := max(Mes_Adm, Mes_Desl) - min(Mes_Adm, Mes_Desl) + 1]
  # calcular massa salarial e numero empregos (ponderado pelo numero de meses e
  # carga de trabalho) por municipio, ano e se ligados ou nao a Prefeitura
  vinculos[, Remun_Ano := Remun * Meses_Trab]
  vinculos[, Prop_Period_Integral := round(`Qtd Hora Contr`/42, 1)]
  consolidado_ano <- vinculos[, .(massa_salarial = sum(Remun_Ano), 
                                  num_empregos = round(sum(Prop_Period_Integral 
                                                           * Meses_Trab/12))), 
                              by = c("GEOCOD_IBGE", "ANO", "Servidor_Pref")]
  # calcular remuneracao horaria, desconsiderando outliers
  remun_outliers <- vinculos[, 
    .(id, outlier = abs(outliers::scores(Remun/`Qtd Hora Contr`, 
                                         type = "mad")) > 3.5)]
  remun_hora <- vinculos[!remun_outliers[outlier==TRUE], 
                         .(remun_hora = round(sum(Meses_Trab*Remun/
                                               (4.35*`Qtd Hora Contr`))/
                                                sum(Meses_Trab), 2)),
                         by = c("GEOCOD_IBGE", "ANO", "Servidor_Pref"),
                         on = "id"] 
  consolidado_ano <- consolidado_ano[remun_hora, on = c("GEOCOD_IBGE", 
                                                        "ANO", "Servidor_Pref")]
  # incorporar resultados do ano aos demais anos
  consolidado <- rbindlist(list(consolidado, consolidado_ano), 
                           use.names = T, fill = T)
  # limpar dados intermediarios
  remove(vinculos, state, folder, compressed, year, consolidado_ano, remun_hora, 
         remun_outliers)
}

remove(mun_codes, states)

# salvar
saveRDS(consolidado, "2015-2005_RAIS_pre-processado.rds")
consolidado <- readRDS("2015-2005_RAIS_pre-processado.rds")

##################### ELIMINAR MUNICIPIOS COM POUCOS REGISTROS #################

consolidado <- consolidado[!consolidado[num_empregos < 20,], on = "GEOCOD_IBGE"]


####################### APLICAR CORREÇÃO MONETÁRIA #############################

# descricao dos conjuntos de dados do IPCA no Ipeadata
##ipea_IPCA <- search_series(terms = "IPCA", fields = c("name"))

# download e tratamento dos dados
ipca <- setDT(ipeadata("PRECOS_IPCAG", quiet = T))
ipca[,`:=`(ANO = as.integer(format(date, "%Y")), IPCA = 1 + value * 0.01)]
ipca[,c("date", "tcode", "uname", "code", "value"):=NULL]

ipca[, Acumulado := numeric()]
for (year in ipca$ANO) {
  ipca[ANO==year, Acumulado:=prod(ipca[ANO %in% c(year:max(ipca$ANO)), IPCA])]
}

consolidado <- consolidado[ipca[, .(ANO, Fator_IPCA = Acumulado)], 
                           on = c("ANO"), nomatch = 0]
consolidado[, `:=`(massa_salarial = round(massa_salarial * Fator_IPCA),
                   remun_hora = round(remun_hora * Fator_IPCA, 2))]
consolidado[, Fator_IPCA:=NULL]
remove(year, ipca)


################################## PER CAPITA ##################################

# download da populacao residente estimada por municipio por ano, via ipeadata
pop <- setDT(rbind(ipeadata("ESTIMA_PO"), ipeadata("POPTOT")))
pop <- pop[uname=="Municipality",]

# incorporar populacao estimada aos dados de emprego
consolidado <- consolidado[pop[,.(GEOCOD_IBGE=tcode, 
                                  ANO = as.integer(format(date, "%Y")),
                                  pop = value)], 
                           on = c("GEOCOD_IBGE", "ANO"), 
                           nomatch = 0]

# dividir variaveis pela populacao
consolidado[, `:=`(massa_salarial = round(massa_salarial/pop, 2),
                   num_empregos = num_empregos/pop)]
consolidado[, pop:=NULL]
remove(pop)

############################## REMOVER OUTLIERS ################################

value_cols <- c("massa_salarial", "remun_hora", "num_empregos")
consolidado[, c(paste0(value_cols, "_Posto")) := frank(-.SD, ties.method="min"), 
            by = c("Servidor_Pref", "ANO"), 
            .SDcols = value_cols]
consolidado[, outlier := any(abs(outliers::scores(.SD, type = "mad")) > 3.5), 
            by = c("GEOCOD_IBGE", "Servidor_Pref"), 
            .SDcols = paste0(value_cols, "_Posto")]
consolidado <- consolidado[!consolidado[outlier==T], on = "GEOCOD_IBGE"]

consolidado[, c(paste0(value_cols, "_Posto"), "outlier") := NULL]


########################## POR TRIENIO INICIAL DO MANDATO ######################


# trienio por eleicao seguinte
consolidado[ANO < 2008, Referencia := 2008]
consolidado[ANO > 2008 & ANO < 2012, Referencia := 2012]
consolidado[ANO > 2012, Referencia := 2016]

# media das variaveis por trienio de referencia
consolidado <- consolidado[, .(massa_salarial = round(mean(massa_salarial), 2), 
                               remun_hora = round(mean(remun_hora), 2),
                               num_empregos = mean(num_empregos)),
                           by = .(GEOCOD_IBGE, Referencia, Servidor_Pref)]


######################### COLUNAS SERVIDOR X NAO SERVIDOR ######################

consolidado <- dcast(consolidado, GEOCOD_IBGE + Referencia ~ Servidor_Pref, 
                     value.var = value_cols)
setnames(consolidado, paste0(value_cols, "_FALSE"), value_cols)
setnames(consolidado, paste0(value_cols, "_TRUE"), paste0(value_cols, "_pref"))


########################### IMPORTANCIA DA PREFEITURA ##########################

consolidado[, `:=`(prop_massa_sal_pref = massa_salarial_pref/
                     (massa_salarial+massa_salarial_pref),
                   prop_empregos_pref = num_empregos_pref/
                     (num_empregos+num_empregos_pref))]


############################### COMPARAR TRIENIOS ##############################

anterior <- copy(consolidado)[, Referencia := Referencia + 4]
consolidado <- merge.data.table(consolidado, anterior, 
                                by = c("GEOCOD_IBGE", "Referencia"),
                                suffixes = c("", "_ult"))
consolidado <- consolidado[, 
  `:=`(massa_salarial_ult = massa_salarial - massa_salarial_ult,
       remun_hora_ult = remun_hora - remun_hora_ult, 
       num_empregos_ult = num_empregos - num_empregos_ult, 
       massa_salarial_pref_ult = massa_salarial_pref - massa_salarial_pref_ult,
       remun_hora_pref_ult = remun_hora_pref - remun_hora_pref_ult, 
      num_empregos_pref_ult = num_empregos_pref - num_empregos_pref_ult) ]

remove(value_cols, anterior)


##################################### SALVAR ###################################

consolidado <- na.omit(consolidado)

saveRDS(consolidado, "dados-tratados-rais.rds")  
remove(consolidado)
setwd("../..")
