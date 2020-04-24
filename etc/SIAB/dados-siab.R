#################################### PREPARACAO ###############################

# instalar e carregar pacote ipeadatar
if (!require("ipeadatar")) install.packages("ipeadatar")
library(ipeadatar)

# instalar e carregar pacote data.table
if (!require("data.table")) install.packages("data.table")
library(data.table)

setDTthreads(8)
setwd("D:/alternancia-eleicoes-municipais/etc/SIAB")


############################ DOWNLOAD E PROCESSAMENTO ##########################

# fazer download dos dados manualmente a partir da URL:
# http://tabnet.datasus.gov.br/cgi/tabcgi.exe?siab/cnv/SIABPBR.DEF
# em três conjuntos: anos 2005 a 2007, anos 2009 a 2011 e anos 2013 a 2015

# nota tecnica disponivel em: 
# http://tabnet.datasus.gov.br/cgi/siab/At_bas_prod_marca_desde_1998.pdf

# ler e consolidar os arquivos
producaoAB_2005a2007 <- fread("./2007-2005_SIAB_ProducaoAB.csv", skip = 3, 
                              na.strings = "-")
producaoAB_2009a2011 <- fread("./2011-2009_SIAB_ProducaoAB.csv", skip = 3, 
                              na.strings = "-")
producaoAB_2013a2015 <- fread("./2015-2013_SIAB_ProducaoAB.csv", skip = 3, 
                              na.strings = "-")
producaoAB <- rbindlist(list(`2008` = producaoAB_2005a2007, 
                             `2012` = producaoAB_2009a2011, 
                             `2016` = producaoAB_2013a2015), 
                        use.names = T, idcol = "Referencia")
remove(producaoAB_2005a2007, producaoAB_2009a2011, producaoAB_2013a2015)

# transformar as colunas relevantes e eliminar 
producaoAB[is.na(producaoAB)] <- 0
producaoAB[, `:=`(id_munic_6 = as.integer(substr(Município, 1, 6)), 
                  Referencia = as.integer(Referencia))]
producaoAB <- na.omit(producaoAB)

# apensar geocodigo de 7 digitos do IBGE
mun_codes <- fread("http://basedosdados.org/dataset/b5878df7-e259-44bc-8903-4feb8d56945a/resource/c1deb363-ffba-4b1e-95dc-c5e08311852e/download/municipios.csv")
producaoAB <- producaoAB[mun_codes[, .(id_munic_6, GEOCOD_IBGE = id_munic_7)],
                         on = "id_munic_6"]
remove(mun_codes)


############################## RESUMIR COLUNAS #################################

producaoAB <- producaoAB[, .(GEOCOD_IBGE, Referencia, 
                             Consultas = (`Cons.Res.ForaÁrea` + `Cons.<1_ano` +
                                          `Cons.1a4anos` + `Cons.5a9anos` +
                                          `Cons.10a14anos` + `Cons.15a19anos` +
                                          `Cons.20a39anos` + `Cons.40a49anos` +
                                          `Cons.50a59anos` + `Cons.60_e_mais`),
                             Visitas = (`Visita_Médico` + `Visita_Enferm.` + 
                                        `Vis_Outr.Prof.NS.` + 
                                        `Vis_Prof.Niv.Médio`),
                             Aten_Indiv = (`Atend.Indiv.Enferm` + 
                                             `Atend.Ind.Prof.NS.`),
                             Ativ_Col = (`Atend.Grupo` + `Proc.Coletiv(PC1)` +
                                        `Reuniões`))]

################################# PER CAPITA ###################################

# download da populacao residente estimada por municipio por ano, via ipeadata
pop <- setDT(rbind(ipeadata("ESTIMA_PO"), ipeadata("POPTOT")))
pop <- pop[uname=="Municipality",]
pop [, ANO := as.integer(format(date, "%Y"))]

# incorporar media de populacao residente no municipio em cada um dos periodos
pop_2005a2007 <- pop[ANO > 2004 & ANO < 2008, 
                     .(Referencia = 2008, pop_med = mean(value)), by = "tcode"] 
pop_2009a2011 <- pop[ANO > 2008 & ANO < 2012, 
                     .(Referencia = 2012, pop_med = mean(value)), by = "tcode"] 
pop_2013a2015 <- pop[ANO > 2012 & ANO < 2016, 
                     .(Referencia = 2016, pop_med = mean(value)), by = "tcode"] 
producaoAB <- producaoAB[rbind(pop_2005a2007, pop_2009a2011, pop_2013a2015),
                         on = c("Referencia", GEOCOD_IBGE = "tcode")]

# dividir producao pela populacao residente estimada no periodo
producaoAB[, `:=`(Consultas = Consultas/pop_med, Visitas = Visitas/pop_med,
                  Ativ_Col = Ativ_Col/pop_med, Aten_Indiv = Aten_Indiv/pop_med)]

# limpar colunas e datatables intermediarios
producaoAB[, pop_med := NULL]
remove(pop, pop_2005a2007, pop_2009a2011, pop_2013a2015)


######################## COMPARACAO ENTRE PERIODOS #############################

anterior <- copy(producaoAB)[, Referencia := Referencia + 4]
producaoAB <- merge.data.table(producaoAB, anterior, 
                               by = c("GEOCOD_IBGE", "Referencia"),
                               suffixes = c("", "_ult"), all = F)
remove(anterior)

producaoAB[, `:=`(Consultas_ult = Consultas - Consultas_ult, 
                  Visitas_ult = Visitas - Visitas_ult,
                  Ativ_Col_ult = Ativ_Col - Ativ_Col_ult)]

producaoAB <- na.omit(producaoAB)
saveRDS(producaoAB, "./dados-tratados-siab.rds")
remove(producaoAB)
setwd("../..")