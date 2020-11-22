#################################### PREPARACAO ###############################

# instalar e carregar pacotes para puxar dados do TSE
if (!require("devtools")) install.packages("devtools")
if (!require("cepespR")) devtools::install_github("Cepesp-Fgv/cepesp-r")
if (!require("electionsBR")) {install.packages("electionsBR")}

# instalar e carregar pacote data.table
if (!require("data.table")) install.packages("data.table")
library(data.table)

library(magrittr)

setDTthreads(8)
setwd("D:/alternancia-eleicoes-municipais/etc/TSE")

# obter correspondencias entre codigos de municipio do TSE e do IBGE
mun_codes <- fread("http://basedosdados.org/dataset/b5878df7-e259-44bc-8903-4feb8d56945a/resource/c1deb363-ffba-4b1e-95dc-c5e08311852e/download/municipios.csv")


########################### BAIXAR E PREPROCESSAR ##############################

# obter votacoes no primeiro turno das eleicoes municipais de 2008 e 2012
results <- setDT(cepespR::get_votes(year = "2008, 2012", position = "Mayor", 
                                    cached = TRUE)) %>%
  .[DESCRICAO_ELEICAO %in% c("ELEIÇÕES 2008", "ELEIÇÃO MUNICIPAL 2012"),] %>%
  .[NUM_TURNO == 1,]

# obter candidatos vencedores
candidates <- cepespR::get_candidates(year = "2008, 2012", 
                                      position = "Mayor", cached = TRUE) %>%
  .[DESCRICAO_ELEICAO %in% c("Eleições 2008", "ELEIÇÃO MUNICIPAL 2012"),] %>%
  .[COD_SIT_TOT_TURNO == 1,]

# obter apenas candidatos eleitos
results <- candidates[results, on = c("ANO_ELEICAO", "SIGLA_UE", 
                                      NUMERO_PARTIDO = "NUMERO_CANDIDATO"), 
                      nomatch = NULL]

# vincular dados do eleitorado
results[, SIGLA_UE := as.integer(SIGLA_UE)]
voters <- electionsBR::voter_profile(2008) %>% 
  rbind(electionsBR::voter_profile(2012)) %>% 
  setDT() %>%
  .[, .(TOTAL_ELEITORES = sum(QTD_ELEITORES_NO_PERFIL)), 
    by = .(PERIODO, SIGLA_UE)]

results <- results[voters, on = c(ANO_ELEICAO = "PERIODO", 
                                  SIGLA_UE = "COD_MUNICIPIO_TSE"),
                   nomatch = NULL]

# preparar colunas de interesse, incluindo votacao percentual
results <- results[, .(Referencia = ANO_ELEICAO + 4, GEOCOD_IBGE = COD_MUN_IBGE, 
                       ULT_ELEI_VOTOS = QTDE_VOTOS / TOTAL_ELEITORES)]

saveRDS(results, "dados-tratados-votacaoanterior.rds")
