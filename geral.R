################################# PREPARACAO ###################################


# instalalar e carregar pacote data.table
if (!require("data.table")) install.packages("data.table")
library(data.table)

# instalalar e carregar pacote caret
if (!require("caret")) install.packages("caret")
if (!require("randomForest")) install.packages("randomForest")
if (!require("kernlab")) install.packages("kernlab")
if (!require("adaboost")) install.packages("adaboost")
if (!require("gbm")) install.packages("gbm")
if (!require("MASS")) install.packages("MASS")
library(caret)

# instalar e carregar pacote para paralelizar operacoes no caret
if (!require("doParallel")) install.packages("doParallel")
library(doParallel)

library(magrittr)

setDTthreads(10)
cls <- makePSOCKcluster(10)
registerDoParallel(cls)

setwd("D:/alternancia-eleicoes-municipais")

library(doParallel)


############################## LER DADOS TRATADOS ##############################

# # candidaturas que representam a situacao no municipio
# candidaturas <- readRDS("./etc/TSE/dados-tratados-representante-situacao.rds")
# 
# # vereadores candidatos a reeleicao pertencentes a legenda da situacao
# apoios <- readRDS("./etc/TSE/dados-tratados-apoios.rds")
# 
# # perfil do eleitorado
# eleitorado <- readRDS("./etc/TSE/dados-tratados-eleitorado.rds")
# 
# # votacao anterior ('capital eleitoral')
# votacao <- readRDS("./etc/TSE/dados-tratados-votacaoanterior.rds")
# 
# # desempenho no IDEB
# ideb <- readRDS("./etc/INEP/dados-tratados-ideb.rds")
# 
# # indicadores de produtividade da atencao basica em saude
# siab <- readRDS("./etc/SIAB/dados-tratados-siab.rds")
# 
# # mercado formal
# rais <- readRDS("./etc/RAIS/dados-tratados-rais.rds")
# 
# # situacao fiscal, receitas e despesas publicas municipais
# stn <- readRDS("./etc/STN/dados-tratados-stn.rds")
# 
# 
# comm_cols = c("GEOCOD_IBGE", "Referencia")
# DT <- candidaturas
# DT %<>%
#   .[apoios, on = comm_cols, nomatch = NULL] %>% 
#   .[eleitorado, on = comm_cols, nomatch = NULL] %>% 
#   .[votacao, on = comm_cols, nomatch = NULL] %>%
#   .[ideb, on = comm_cols, nomatch = NULL] %>% 
#   .[siab, on = comm_cols, nomatch = NULL] %>%
#   .[rais, on = comm_cols, nomatch = NULL] %>%
#   .[stn, on = comm_cols, nomatch = NULL]
# 
# remove(candidaturas, eleitorado, votacao,apoios, ideb, siab, stn, rais, 
#        comm_cols)
# saveRDS(DT, "dados-geral.rds")
DT <- readRDS("dados-geral.rds")

############################ PRE PROCESSAMENTO #################################

# transformar variável resposta em fator
DT[, SITUACAO := as.factor(SITUACAO)]

# remover variaveis com variancia proxima de zero
DT[, setdiff(nearZeroVar(DT, names = T), c("TURNO_2")) := NULL]

# remover variaveis redundantes
correlationMatrix <- cor(DT[,!c("GEOCOD_IBGE", "Referencia", "SITUACAO", 
                                "SIT_ULT_ELEITO")])
highlyCorrelated <- findCorrelation(correlationMatrix, cutoff = 0.9, names = T)
DT[, c(highlyCorrelated) := NULL]

# criar conjunto de validacao com 30% das observacoes, estratificada 
# entre cada decil de numero de eleitores
set.seed(42)
inTrain <- createDataPartition(y = DT$TOTAL_ELEITORES, p = 2/3, 
                               groups = 10, list = F)
training <- DT[inTrain, -c("Referencia", "GEOCOD_IBGE")]

# seleção de variáveis com modelo GLM
training_glm <- glm(SITUACAO ~ ., data = training, family = binomial)
training <- stepAIC(training_glm, trace = F)$model
setDT(training)


############################ TREINAR MODELO GLM ################################

ctrl <- trainControl(method = "repeatedcv", number = 6, repeats = 5)

set.seed(42)
model_GLM <- train(SITUACAO ~ ., data = training, method = "glm", 
                   trControl = ctrl)
saveRDS(model_GLM, "modelo_GLM.rds")


########################### PADRONIZAR VARIAVEIS ###############################

# separar variaveis dummy para nao passar pelo pre processamento
dummy <- training[, .(SIT_ULT_ELEITO)]
training <- training[, -c("SIT_ULT_ELEITO")]

# centralizar pela média e dividir pelo desvio-padrao
normalization <- preProcess(training, method = c("center", "scale", "YeoJohnson"))
training <- predict(normalization, training)
training <- cbind(dummy, training)

# Support Vector Machine
set.seed(42)
model_SVM <- train(SITUACAO ~ ., data = training, method = "svmLinear", 
                   trControl = ctrl, tuneGrid = expand.grid(C = c(0.001,0.01,
                                                                  0.1,1,10,100)))
set.seed(42)
model_SVM <- train(SITUACAO ~ ., data = training, method = "svmLinear", 
                   trControl = ctrl, preProcess = c("center", "scale"),
                   tuneGrid = expand.grid(C = c(0.002,0.004,0.006,0.008)))
set.seed(42)
model_SVM <- train(SITUACAO ~ ., data = training, method = "svmLinear", 
                   trControl = ctrl, preProcess = c("center", "scale"),
                   tuneGrid = expand.grid(C = c(0.002,0.003,0.004)))
saveRDS(model_SVM, "modelo_SVM.rds")

# k-Nearest Neighbors
set.seed(42)
model_KNN <- train(SITUACAO ~ ., data = training, method = "knn", 
                   trControl = ctrl, 
                   preProcess = c("center", "scale"), 
                   tuneLength = 10)
saveRDS(model_KNN, "modelo_KNN.rds")

# Bagging
set.seed(42)
model_ADABAG <- train(SITUACAO ~ . - GEOCOD_IBGE - Referencia, 
                      data = training, method = "AdaBag", trControl = ctrl, 
                      boos = TRUE, coeflearn = "Breiman", 
                      preProcess = c("center", "scale"), tuneLength = 10)
saveRDS(model_ADABAG, "modelo_ADABAG.rds")

# Gradient Boosting Machines
set.seed(42)
model_GBM <- train(SITUACAO ~ . - GEOCOD_IBGE - Referencia, 
                   data = training, method = "gbm", trControl = ctrl, 
                   preProcess = c("center", "scale"), tuneLength = 10)
saveRDS(model_GBM, "modelo_GBM.rds")

# Extreme Gradient Boosting
set.seed(42)
model_XGB <- train(SITUACAO ~ . - GEOCOD_IBGE - Referencia, 
                   data = training, method = "xgbTree", trControl = ctrl, 
                   preProcess = c("center", "scale"), tuneLength = 10)
saveRDS(model_XGB, "modelo_XGB.rds")


################################ ANALISAR RESULTADOS ###########################

model_GLM <- readRDS("modelo_GLM.rds")
model_SVM <- readRDS("modelo_SVM.rds")
model_KNN <- readRDS("modelo_KNN.rds")
model_ADABAG <- readRDS("modelo_ADABAG.rds")
model_GBM <- readRDS("modelo_GBM.rds")
model_XGB <- readRDS("modelo_XGB.rds")


# marcar conjunto de treino e validacao
DT[!inTrain, Conjunto := "Validacao"]
DT[inTrain, Conjunto := "Treino"]

# analise entre Folds de treinamento
results <- resamples(list(GLM = model_GLM, SVM = model_SVM, KNN = model_KNN,
                          ADABAG = model_ADABAG, GBM = model_GBM, XGB = model_XGB))
scales <- list(x=list(relation="free"), y=list(relation="free"))
bwplot(results, scales=scales)
dev.print(png, "resamples_bwplot.png", width=654, height=538)
parallelplot(results)
dev.print(png, "resamples_parallelplot.png", width=654, height=538)
splom(results)
dev.print(svg, "resamples_splom.svg", width=654, height=538)

differences <- diff(results, metric = "Accuracy", alternative = "greater")

# valores preditos pelos diferentes modelos
DT[, `:=`(predito_GLM = predict(model_GLM, newdata = DT),
          predito_SVM = predict(model_SVM, newdata = DT),
          predito_KNN = predict(model_KNN, newdata = DT),
          predito_ADABAG = predict(model_ADABAG, newdata = DT),
          predito_GBM = predict(model_GBM, newdata = DT),
          predito_XGB = predict(model_XGB, newdata = DT))]

performance <- data.table()
for (method in c("GLM", "SVM", "KNN", "ADABAG", "GBM", "XGB")) {
  for (conj in c("Treino", "Validacao")) {
    confusion <- confusionMatrix(table(DT[Conjunto == conj, 
                                          c("SITUACAO", paste0("predito_", 
                                                               ..method))]))
    confusion <- cbind(data.table(Method = method, Dataset = conj),
                       as.data.table(as.list(confusion$overall)),
                       as.data.table(as.list(confusion$byClass)))
    performance <- rbind(performance, confusion) 
  }
}

performance_resamples <- rbindlist(
  list(GLM = model_GLM$results, SVM = model_SVM$results, 
       KNN = model_KNN$results, Adabagging = model_ADABAG$results,
       GBM = model_GBM$results, Xgboosting = model_XGB$results), 
  use.names = T, idcol = "Method", fill = T)[, 
    .SD[which.max(Accuracy)], by= "Method", 
    .SDcols = c("Accuracy", "AccuracySD", "Kappa", "KappaSD")]


saveRDS(DT, "resultados.rds")
saveRDS(performance, "performance-modelos.rds")
saveRDS(performance_resamples, "performance-resamples.rds")
#openxlsx::write.xlsx(transpose(performance[Dataset=="Treino", !c("Dataset")],
#                               keep.names = "Métrica", make.names = "Method"), 
#                     "performance-treino.xlsx")
#openxlsx::write.xlsx(transpose(performance[Dataset=="Validacao", !c("Dataset")],
#                               keep.names = "Métrica", make.names = "Method"), 
#                     "performance-validacao.xlsx")
openxlsx::write.xlsx(transpose(performance_resamples,
                               keep.names = "Métrica", make.names = "Method"), 
                     "performance-resamples.xlsx")

#############
stopCluster(cls)
