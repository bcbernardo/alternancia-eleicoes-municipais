#################################### PREPARACAO ###############################

# instalar e carregar pacote data.table
if (!require("data.table")) install.packages("data.table")
library(data.table)

# instalar e carregar pacote ipeadatar
if (!require("ipeadatar")) install.packages("ipeadatar")
library(ipeadatar)

# instalar e carregar pacote GmAMisc (para deteccao de outliers)
if (!require("outliers")) install.packages("outliers")

setDTthreads(8)
setwd("D:/alternancia-eleicoes-municipais/etc/STN")


##################### DOWNLOAD DOS DADOS VIA IPEADATA #########################

# descricao dos conjuntos de dados de interesse no Ipeadata
ipea_financasMun <- search_series(terms = "municipal", fields = c("name"))
setDT(ipea_financasMun)
ipea_financasMun <- ipea_financasMun[theme=="Regional",]

if (file.exists("./2017-1985_STN_FinancasMunicipais.rds")) { 
  financasMun <- readRDS("./2017-1985_STN_FinancasMunicipais.rds")
  } else {
  financasMun <- data.table(code=character(), date=as.Date(character()), 
                            value = numeric(), uname = factor(), tcode = integer())
  for (dataset_code in ipea_financasMun$code) {
    dataset <- setDT(ipeadata(code = dataset_code, quiet = TRUE))
    dataset <- dataset[uname=="Municipality",]
    financasMun <- rbind(financasMun, dataset) }
  financasMun[,`:=`(GEOCOD_IBGE = as.integer(tcode), 
                    ANO = as.integer(format(date, "%Y")))]
  financasMun[,c("date", "tcode", "uname"):=NULL]
  saveRDS(financasMun, "./2017-1985_STN_FinancasMunicipais.rds")
  remove(datasetm, dataset_code)
}

####################### APLICAR CORREÇÃO MONETÁRIA #############################

# descricao dos conjuntos de dados do IPCA no Ipeadata
##ipea_IPCA <- search_series(terms = "IPCA", fields = c("name"))

# download e tratamento dos dados
ipca <- setDT(ipeadata("PRECOS_IPCAG", quiet = T))
ipca[,`:=`(ANO = as.integer(format(date, "%Y")), IPCA = 1 + value * 0.01)]
ipca[,c("date", "tcode", "uname", "code", "value"):=NULL]

ipca[, Acumulado := numeric()]
for (year in ipca$ANO) {
  ipca[ANO==year, Acumulado:=prod(ipca[ANO %in% c(year:max(ipca$ANO)), IPCA])] }

financasMun <- financasMun[ipca[, .(ANO, Fator_IPCA = Acumulado)], 
                           on = c("ANO"), nomatch = 0]
financasMun[, value := value * Fator_IPCA]
financasMun[, Fator_IPCA:=NULL]
remove(year, ipca)


############################# ADICIONAR POSTOS #################################

financasMun[, Posto:=frank(-value, ties.method="min"), by=c("code", "ANO")]


############################## REMOVER OUTLIERS ################################

financasMun <- financasMun[ANO %in% c(2005:2015),]
financasMun[, outlier := (abs(outliers::scores(Posto, type = "mad")) > 3.5), 
            by = c("GEOCOD_IBGE", "code")]

financasMun <- financasMun[!financasMun[outlier == T & 
                                        code %in% c("DESPORM", "RECORRM"),], 
                           on = "GEOCOD_IBGE"]


################################# PER CAPITA ###################################

pop <- setDT(rbind(ipeadata("ESTIMA_PO"), ipeadata("POPTOT")))
pop <- pop[uname=="Municipality",]

pop[,`:=`(GEOCOD_IBGE = tcode, ANO = as.integer(format(date, "%Y")), POP = value)]

financasMun <- financasMun[pop[,.(GEOCOD_IBGE, ANO, POP)], 
                           on = c("GEOCOD_IBGE", "ANO")]
financasMun[, value := round(value/POP, 2)]
remove(pop)


####################### POR TRIENIO INICIAL DO MANDATO #########################

financasMun[ANO %in% c(2005:2007), Referencia := 2008]
financasMun[ANO %in% c(2009:2011), Referencia := 2012]
financasMun[ANO %in% c(2013:2016), Referencia := 2016]
financasMun <- financasMun[, .(value=sum(value)), 
                           by=c("GEOCOD_IBGE", "Referencia", "code")]

####################### COMPARAR TRIENIOS ######################################

financasMun <- financasMun[
  financasMun[, .(GEOCOD_IBGE, code, value_last=value, Referencia=Referencia+4)],
  on = c("GEOCOD_IBGE", "code", "Referencia"), nomatch = NULL]
financasMun[, value_last := value - value_last]
financasMun <- na.omit(financasMun)

############################### VARIAVEIS EM COLUNAS ###########################

valores <- dcast(financasMun, GEOCOD_IBGE + Referencia ~ code, 
                 value.var = "value")
valores_ult <- dcast(financasMun, GEOCOD_IBGE + Referencia ~ code, 
                     value.var = "value_last")


#################### ADICIONAR INDICADORES DE GESTAO FISCAL ####################

# adaptados de https://www.firjan.com.br/ifgf/metodologia/

valores[, `:=`(Pessoal = DESPCUPM/RECORRM, Endividamento = DFENCEM/RECORRM,
                   Investimento = (DINVESTM + DINVFINAM) / RECORRM)]
valores_ult[, `:=`(Pessoal = DESPCUPM/RECORRM, Endividamento = DFENCEM/RECORRM,
                   Investimento = (DINVESTM + DINVFINAM) / RECORRM)]


##################### CRITICAS DE CONSISTENCIA E JUNTAR TUDO ###################

financasMun <- merge.data.table(x = valores, y = valores_ult,
                                by = c("GEOCOD_IBGE", "Referencia"), 
                                suffixes = c("", "_ult"))
financasMun <- financasMun[!financasMun[(DESPORM > RECORM) | (RECORM < 1) | 
                                        (RECORRM < 1) | (RECTRIBM < 1) | 
                                        (DESPORM < 1),], 
                           on = c("GEOCOD_IBGE")]

financasMun <- na.omit(financasMun)
saveRDS(financasMun, "./dados-tratados-stn.rds")
remove(valores, valores_ult, financasMun)
setwd("../..")