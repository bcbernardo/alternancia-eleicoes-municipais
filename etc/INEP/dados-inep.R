################################# PREPARACAO ###################################

# instalar pacote readxl
if (!require("readxl")) install.packages("readxl")

# (instalar e) carregar pacote data.table
if (!require("data.table")) install.packages("data.table")
library(data.table)

setwd("D:/alternancia-eleicoes-municipais/etc/INEP")
setDTthreads(8)


##################################### LER DADOS ################################

# nota tecnica sobre o IDEB: <http://download.inep.gov.br/educacao_basica/portal_ideb/o_que_e_o_ideb/Nota_Tecnica_n1_concepcaoIDEB.pdf>
# nota sobre as projecoes: <http://download.inep.gov.br/educacao_basica/portal_ideb/o_que_sao_as_metas/Artigo_projecoes.pdf>

# IDEB - anos iniciais do Ensino Fundamental
if (!file.exists("INEP_2017-2005_IDEB_AI_Municipios.zip")) {
  curl::curl_download("http://download.inep.gov.br/educacao_basica/portal_ideb/planilhas_para_download/2017/divulgacao_anos_iniciais_municipios2017-atualizado-Jun_2019.zip",
                      destfile = "INEP_2017-2005_IDEB_AI_Municipios.zip") }
unzip("INEP_2017-2005_IDEB_AI_Municipios.zip", exdir = ".")
IDEB_AnosIniciais <- readxl::read_excel(Sys.glob("*iniciais*xlsx"), 
                                        skip = 9, na = "-")
setDT(IDEB_AnosIniciais)

# IDEB - anos finais do Ensino Fundamental
if (!file.exists("INEP_2017-2005_IDEB_AF_Municipios.zip")) {
  curl::curl_download("http://download.inep.gov.br/educacao_basica/portal_ideb/planilhas_para_download/2017/divulgacao_anos_finais_municipios2017-atualizado-Jun_2019.zip",
                    destfile = "INEP_2017-2005_IDEB_AF_Municipios.zip") }
unzip("INEP_2017-2005_IDEB_AF_Municipios.zip", exdir = ".")
IDEB_AnosFinais <- readxl::read_excel(Sys.glob("*finais*xlsx"), 
                                      skip = 9, na = "-")
setDT(IDEB_AnosFinais)

IDEB <- IDEB_AnosIniciais[IDEB_AnosFinais, on = c("COD_MUN", "REDE")]
IDEB_RedeMun <- IDEB[REDE == "Municipal",]

remove(IDEB_AnosIniciais, IDEB_AnosFinais, IDEB)


################################ IDEB 2011 x IDEB 2007 #########################

IDEB_2012 <- IDEB_RedeMun[, .(GEOCOD_IBGE = `COD_MUN`, Referencia = 2012, 
                              IDEB14 = IDEB14_11, IDEB58 = IDEB58_11,
                              P14 = P14_11, P58 = P58_11, 
                              PAD14 = PAD14_11, PAD58 = PAD58_11,
                              IDEB14_ult = IDEB14_11 - IDEB14_07,
                              IDEB58_ult = IDEB58_11 - IDEB58_07,
                              P14_ult = P14_11 - P14_07, 
                              P58_ult = P58_11 - P58_07,
                              PAD14_ult = PAD14_11 - PAD14_07,
                              PAD58_ult = PAD58_11 - PAD58_07,
                              IDEB14_dif_proj = IDEB14_11 - PROJ14_11,
                              IDEB58_dif_proj = IDEB58_11 - PROJ58_11)]
IDEB_2012 <- na.omit(IDEB_2012)

IDEB_2016 <- IDEB_RedeMun[, .(GEOCOD_IBGE = `COD_MUN`, Referencia = 2016, 
                              IDEB14 = IDEB14_15, IDEB58 = IDEB58_15,
                              P14 = P14_15, P58 = P58_15, 
                              PAD14 = PAD14_15, PAD58 = PAD58_15,
                              IDEB14_ult = IDEB14_15 - IDEB14_11,
                              IDEB58_ult = IDEB58_15 - IDEB58_11, 
                              P14_ult = P14_15 - P14_11, 
                              P58_ult = P58_15 - P58_11,
                              PAD14_ult = PAD14_15 - PAD14_11,
                              PAD58_ult = PAD58_15 - PAD58_11,
                              IDEB14_dif_proj = IDEB14_15 - PROJ14_15,
                              IDEB58_dif_proj = IDEB58_15 - PROJ58_15)]
IDEB_2016 <- na.omit(IDEB_2016)

# juntar os dois periodos
IDEB_RedeMun <- rbind(IDEB_2012, IDEB_2016)

# remover municipios que nao aparecem em ambos
bad <- setdiff(IDEB_2012$GEOCOD_IBGE, IDEB_2016$GEOCOD_IBGE)
IDEB_RedeMun <- IDEB_RedeMun[!(GEOCOD_IBGE %in% bad), ]

saveRDS(IDEB_RedeMun, "./dados-tratados-ideb.rds")
remove(IDEB_2012, IDEB_2016, bad)
