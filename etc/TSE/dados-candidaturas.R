#################################### PREPARACAO ###############################

# instalar e carregar pacotes para puxar dados do TSE
if (!require("devtools")) install.packages("devtools")
if(!require("cepespR")) devtools::install_github("Cepesp-Fgv/cepesp-r")

# instalar e carregar pacote data.table
if (!require("data.table")) install.packages("data.table")
library(data.table)

setDTthreads(8)
setwd("D:/alternancia-eleicoes-municipais/etc/TSE")

# obter correspondencias entre codigos de municipio do TSE e do IBGE
mun_codes <- fread("http://basedosdados.org/dataset/b5878df7-e259-44bc-8903-4feb8d56945a/resource/c1deb363-ffba-4b1e-95dc-c5e08311852e/download/municipios.csv")


############################## CANDIDATURAS ###################################

# obter lista de candidatos as eleicoes de 2008, 2012 e 2016
candidates <- cepespR::get_candidates(year="2008, 2012, 2016", 
                                      position="Mayor", cached = TRUE)

# processar planilha de candidatos 
candidates[, SIGLA_UE:=as.integer(SIGLA_UE)]
candidates[, Referencia := as.integer(ANO_ELEICAO)]

# vincular codigos ibge
candidates <- candidates[mun_codes[, .(id_TSE, GEOCOD_IBGE = id_munic_7)], 
                         on=c(SIGLA_UE="id_TSE")]

# candidatura por um so partido
candidates[COMPOSICAO_LEGENDA=="#NE#", COMPOSICAO_LEGENDA := SIGLA_PARTIDO] 

# calcular idade
candidates[, IDADE := Referencia - as.numeric(substr(DATA_NASCIMENTO, 7, 11))]

# unificar codigo do resultado eleitoral quando há segundo turno
candidates_1t <- candidates[NUM_TURNO==1,]
candidates_2t <- candidates[NUM_TURNO==2, .(NUM_TITULO_ELEITORAL_CANDIDATO, 
                                            Referencia, 
                                            COD_SIT_TOT_TURNO, 
                                            DESC_SIT_TOT_TURNO)]
candidates <- merge.data.table(x = candidates_1t, y = candidates_2t, 
                               by = c("NUM_TITULO_ELEITORAL_CANDIDATO", 
                                      "Referencia"), 
                               suffixes = c("", "2"), all.x = T, all.y = F)
candidates[COD_SIT_TOT_TURNO == 5 & COD_SIT_TOT_TURNO2 == 1,  
           `:=` (COD_SIT_TOT_TURNO = 1, 
                 DESC_SIT_TOT_TURNO = "ELEITO")]
candidates[, `:=` (COD_SIT_TOT_TURNO2 = NULL, DESC_SIT_TOT_TURNO2 = NULL)]
remove(candidates_1t, candidates_2t)

# remover municipios que tiveram eleicoes suplementares no periodo
candidates <- candidates[!candidates[
  !(DESCRICAO_ELEICAO %in% c("Eleições 2008", "ELEIÇÃO MUNICIPAL 2012", 
                             "Eleições Municipais 2016" )),], 
  on = "GEOCOD_IBGE"]

# remover candidaturas indeferidas antes da eleicao
candidates <- candidates[DESC_SIT_TOT_TURNO %in% 
                           c("ELEITO", "NÃO ELEITO", "#NULO#", "2º TURNO"),]
candidates <- candidates[DES_SITUACAO_CANDIDATURA %in% c("DEFERIDO", "DEFERIDO COM RECURSO")]


# remover municipios com candidaturas que deram problema apos alguma das eleicoes
candidates <- candidates[!candidates[DESC_SIT_TOT_TURNO == "#NULO#",], 
                         on = c("GEOCOD_IBGE")]
# selecionar apenas colunas relevantes
candidates <- candidates[, .(GEOCOD_IBGE, Referencia, 
                             NUM_TITULO =  NUM_TITULO_ELEITORAL_CANDIDATO, 
                             SITUACAO = DESC_SIT_TOT_TURNO,
                             SIGLA_PARTIDO, COMPOSICAO_LEGENDA, 
                             OCUPACAO = DESCRICAO_OCUPACAO, 
                             SEXO = DESCRICAO_SEXO, 
                             GRAU_INSTRUCAO = DESCRICAO_GRAU_INSTRUCAO,
                             IDADE)]

########################### SITUACAO NA ULTIMA ELEICAO ########################

# situacao do candidato em 2008 e 2012
candidates <- merge.data.table(x = candidates, 
                               y = candidates[Referencia == 2008, 
                                          .(NUM_TITULO, SITUACAO)],
                               by = "NUM_TITULO", all.x = TRUE, all.y = FALSE,
                               suffixes = c("", "_2008"))
candidates <- merge.data.table(candidates, 
                               candidates[Referencia == 2012, 
                                          .(NUM_TITULO, SITUACAO)],
                               by = "NUM_TITULO", all.x = TRUE, all.y = FALSE,
                               suffixes = c("", "_2012"))

# situacao do candidato no ultimo pleito
candidates[Referencia == 2012, `:=` (SIT_ULT_ELEI = SITUACAO_2008)]
candidates[Referencia == 2016, `:=` (SIT_ULT_ELEI = SITUACAO_2012)]
candidates[Referencia > 2008 & is.na(SIT_ULT_ELEI), 
           c("SIT_ULT_ELEI") := "NÃO CONCORREU"]
candidates[, c("SITUACAO_2008", "SITUACAO_2012") := NULL]


############################### APOIO DA SITUACAO #############################

# o partido vencedor do ultimo pleito está na coalizao vencedora no pleito seguinte? 
last_party_2008x2012 <- candidates[Referencia == 2008 & SITUACAO == "ELEITO",
                                   .(GEOCOD_IBGE, Referencia = 2012, 
                                     ULT_PART_ELEIT = SIGLA_PARTIDO)]
last_party_2012x2016 <- candidates[Referencia == 2012 & SITUACAO == "ELEITO",
                                   .(GEOCOD_IBGE, Referencia = 2016, 
                                     ULT_PART_ELEIT = SIGLA_PARTIDO)]
last_party <- rbind(last_party_2008x2012, last_party_2012x2016)
candidates <- merge.data.table(x=candidates, y=last_party, 
                               by=c("GEOCOD_IBGE", "Referencia"), all.x = T)
candidates[!is.na(ULT_PART_ELEIT), 
           ULT_PART_NA_LEG := mapply(function(x, y){x %in% 
                                                    unlist(strsplit(y, " / "))}, 
                                     ULT_PART_ELEIT, COMPOSICAO_LEGENDA)]
#candidates[, c("ULT_PART_ELEIT") := NULL]
remove(last_party_2008x2012, last_party_2012x2016, last_party)


############################# SALVAR CANDIDATURAS #############################

# salvar planilha de candidatos
saveRDS(candidates, "./dados-tratados-candidatos.rds")
candidates <- readRDS("dados-tratados-candidatos.rds")


############################ CANDIDATURAS INCUMBENTES ##########################

# candidatos em busca de reeleicao
target1 <- candidates[Referencia %in% c(2012, 2016) & SIT_ULT_ELEI == "ELEITO"]

# provaveis representantes da situacao
target2 <- candidates[, .(ULT_PREF_CONC = any(SIT_ULT_ELEI == "ELEITO")), 
                      by=c("GEOCOD_IBGE", "Referencia")]
target2 <- candidates[target2[ULT_PREF_CONC==F, .(GEOCOD_IBGE, Referencia)],
                      on = c("GEOCOD_IBGE", "Referencia"), nomatch =NULL]
target2 <- target2[Referencia %in% c(2012, 2016) & ULT_PART_NA_LEG == TRUE & 
                     SIT_ULT_ELEI == "NÃO CONCORREU"]
target <- rbind(target1, target2)
remove(target1, target2)

# visualizar e remover municipio com registro inconsistente
View(target[, .N, by = c("GEOCOD_IBGE", "Referencia")][
  candidates, on=c("GEOCOD_IBGE", "Referencia"), nomatch = NULL][N>1])
target <- target[!(GEOCOD_IBGE==2903201 & Referencia==2012)]

# remover candidaturas com problemas
target <- target[SITUACAO !="#NULO#",]

# igualar quem foi ao segundo turno e não foi eleito aos demais não eleitos
target <- target[SITUACAO=="2º TURNO", SITUACAO:="NÃO ELEITO"]

# selecionar colunas desejadas
target <- target[SITUACAO !="#NULO#",.(GEOCOD_IBGE, Referencia, SITUACAO, 
                                       SIT_ULT_ELEI)]

# transformar coluna de participacao na ultima eleicao em dummy
target[SIT_ULT_ELEI == "ELEITO", SIT_ULT_ELEITO := 1]
target[SIT_ULT_ELEI == "NÃO CONCORREU", SIT_ULT_ELEITO := 0]
target[, SIT_ULT_ELEI := NULL]

# salvar e limpar
saveRDS(target, "./dados-tratados-representante-situacao.rds")
remove(target, candidates, mun_codes, parties)
setwd("../..")
