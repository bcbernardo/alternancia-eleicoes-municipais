#################################### PREPARACAO ###############################

# instalar e carregar pacotes para puxar dados do TSE
if (!require("devtools")) install.packages("devtools")
if (!require("cepespR")) devtools::install_github("bcbernardo/cepesp-r")

if (!require("electionsBR")) install.packages("electionsBR")

# instalar e carregar pacote data.table
if (!require("data.table")) install.packages("data.table")
library(data.table)

library(magrittr)

setDTthreads(8)
setwd("D:/alternancia-eleicoes-municipais/etc/TSE")


############################ DOWNLOAD E PROCESSAMENTO ##########################

parties <- c(AVANTE = "PT do B", CIDADANIA = "PPS", DC = "PSDC", DEM = "DEM", 
             MDB = "PMDB", NOVO = "NOVO", PATRIOTA = "PEN", PCB = "PCB", 
             PCDOB = "PC do B", PCO = "PCO", PDT = "PDT", PHS = "PHS", 
             PL = "PR", PMB = "PMB", PMN = "PMN", PRTB = "PRTB", PP = "PP", 
             PODE = "PTN", PPL = "PPL", PROS = "PROS", REPUBLICANOS = "PRB", 
             PSB = "PSB", PSC = "PSC", PSD = "PSD", PSDB = "PSDB", 
             PSL = "PSL", PSOL = "PSOL", PSTU = "PSTU", PT = "PT", 
             PTB = "PTB", PTC = "PTC", PV = "PV", REDE = "REDE", PRP = "PRP", 
             SOLIDARIEDADE = "SD")

mun_codes <- fread("http://basedosdados.org/dataset/b5878df7-e259-44bc-8903-4feb8d56945a/resource/c1deb363-ffba-4b1e-95dc-c5e08311852e/download/municipios.csv")

candidates <- rbindlist(list(setDT(electionsBR::candidate_local(2008)), 
                             setDT(electionsBR::candidate_local(2012)), 
                             setDT(electionsBR::candidate_local(2016))),
                        use.names = TRUE, fill = TRUE)

# vincular codigos ibge
candidates <- candidates[mun_codes[, .(SIGLA_UE=as.character(id_TSE), 
                                       GEOCOD_IBGE=id_munic_7)], 
                         on="SIGLA_UE"]

# remover eleicoes suplementares
candidates <- candidates[is.element(DESCRICAO_ELEICAO, 
                                   c("Eleições 2008", "ELEIÇÃO MUNICIPAL 2012", 
                                     "Eleições Municipais 2016")),] 
saveRDS(candidates, "2016-2008_TSE_CandidatosLocais_BR.rds")
candidates <- readRDS("2016-2008_TSE_CandidatosLocais_BR.rds")

#################### ATUAIS VEREADORES APOIANDO O INCUMBENTE ###################

# situacao na ultima eleicao
candidates <- merge.data.table(
  x = candidates, y = candidates[, .(ANO_ELEICAO = ANO_ELEICAO+4, 
                                     NUM_TITULO_ELEITORAL_CANDIDATO, 
                                     DESC_CARG_ULT_ELEI = DESCRICAO_CARGO,
                                     DESC_SIT_ULT_ELEI = DESC_SIT_TOT_TURNO)],
  by = c("ANO_ELEICAO", "NUM_TITULO_ELEITORAL_CANDIDATO"), all.x = T, all.y = F)

# candidatos que sao vereadores, por partido e municipio
councilors_disputing <- candidates[
  DESCRICAO_OCUPACAO == "VEREADOR" | (DESC_CARG_ULT_ELEI == "VEREADOR" & 
  is.element(DESC_SIT_ULT_ELEI, c("ELEITO", "ELEITO POR QP", "ELEITO POR MÉDIA",
  "MÉDIA"))), .N, by = c("GEOCOD_IBGE", "ANO_ELEICAO", "SIGLA_PARTIDO")]

# obter legenda dos candidatos da situacao
incumbents <- readRDS("dados-tratados-representante-situacao.rds")
councilors_disputing <- councilors_disputing[
  incumbents[, .(GEOCOD_IBGE, ANO_ELEICAO, LEGENDA_SITUACAO = COMPOSICAO_LEGENDA)],
  on = c("GEOCOD_IBGE", "ANO_ELEICAO"), nomatch = 0]

# verificar se o partido do candidato esta na legenda da situacao
councilors_disputing <- councilors_disputing[, 
  COM_INCUMB := mapply(function(pt, leg){grepl(paste0(" ?", pt, " ?"), 
                                                 leg, perl = T)},
                         SIGLA_PARTIDO, LEGENDA_SITUACAO)]


############### VEREADORES ALIADOS POR ELEITOR E SOBRE O TOTAL #################

# total de candidatos vereadores em cada municipio
councilors_disputing %<>% 
  .[, .N, by = c("GEOCOD_IBGE", "ANO_ELEICAO", "COM_INCUMB")] %>%
  .[, TOTAL_VEREADORES := sum(N), by = c("GEOCOD_IBGE", "ANO_ELEICAO")] %>%
  .[COM_INCUMB == TRUE, !c("COM_INCUMB")]

# eleitorado por municipio e por ano
electorate <- readRDS("dados-tratados-eleitorado.rds")
councilors_disputing <- electorate[councilors_disputing, 
                                   on = c("GEOCOD_IBGE", 
                                          Referencia = "ANO_ELEICAO"), 
                                   nomatch = NULL]

# obter o percentual dos vereadores que sao aliados, e a quantidade de 
# vereadores aliados por eleitor
councilors_disputing <- councilors_disputing[, 
  .(GEOCOD_IBGE, Referencia,
    prop_veread_c_inc = round(N/TOTAL_VEREADORES, 2), 
    veread_por_eleit = N/TOTAL_ELEITORES)]

saveRDS(councilors_disputing, "dados-tratados-apoios.rds")
remove(candidates, councilors_disputing, electorate, incumbents, mun_codes)
setwd("../..")
