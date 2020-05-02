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
library(caret)

# instalar e carregar pacote para paralelizar operacoes no caret
if (!require("doParallel")) install.packages("doParallel")
library(doParallel)

library(magrittr)

setDTthreads(10)
cls <- makeCluster(10) 
registerDoParallel(cls)

setwd("D:/alternancia-eleicoes-municipais")


############################## LER DADOS TRATADOS ##############################

# candidaturas que representam a situacao no municipio
candidaturas <- readRDS("./etc/TSE/dados-tratados-representante-situacao.rds")

# vereadores candidatos a reeleicao pertencentes a legenda da situacao
apoios <- readRDS("./etc/TSE/dados-tratados-apoios.rds")

# perfil do eleitorado
eleitorado <- readRDS("./etc/TSE/dados-tratados-eleitorado.rds")

# desempenho no IDEB
ideb <- readRDS("./etc/INEP/dados-tratados-ideb.rds")

# indicadores de produtividade da atencao basica em saude
siab <- readRDS("./etc/SIAB/dados-tratados-siab.rds")

# mercado formal
rais <- readRDS("./etc/RAIS/dados-tratados-rais.rds")

# situacao fiscal, receitas e despesas publicas municipais
stn <- readRDS("./etc/STN/dados-tratados-stn.rds")


comm_cols = c("GEOCOD_IBGE", "Referencia")
DT <- candidaturas
DT %<>%
  .[apoios, on = comm_cols, nomatch = NULL] %>% 
  .[eleitorado, on = comm_cols, nomatch = NULL] %>% 
  .[ideb, on = comm_cols, nomatch = NULL] %>% 
  .[siab, on = comm_cols, nomatch = NULL] %>%
  .[rais, on = comm_cols, nomatch = NULL] %>%
  .[stn, on = comm_cols, nomatch = NULL]

remove(candidaturas, eleitorado, apoios, ideb, siab, stn, comm_cols, rais)


############################ PRE PROCESSAMENTO #################################

# remover variaveis com variancia proxima de zero
DT[, nearZeroVar(DT) := NULL]

# remover variaveis redundantes
correlationMatrix <- cor(DT[,!c("GEOCOD_IBGE", "Referencia", "SITUACAO", 
                                "SIT_ULT_ELEITO")])
highlyCorrelated <- findCorrelation(correlationMatrix, cutoff = 0.9, names = T)
DT[, c(highlyCorrelated) :=NULL]

# criar conjunto de validacao com 30% das observacoes, estratificada 
# entre cada decil de numero de eleitores
set.seed(42)
inTrain <- createDataPartition(y = DT$TOTAL_ELEITORES, p=2/3, 
                               groups = 10, list = F)

# marcar registros para treino e validacao
DT[!inTrain, Conjunto := "Validacao"]
DT[inTrain, Conjunto := "Treino"]
training <-  DT[inTrain,]

############################## TREINAR MODELOS #################################

ctrl <- trainControl(method = "repeatedcv", number = 10, repeats = 3)

# Modelos Lineares Generalizados
set.seed(42)
model_GLM <- train(SITUACAO ~ . - GEOCOD_IBGE - Referencia, data = training, 
                   method = "glm", trControl = ctrl, 
                   preProcess = c("center", "scale"))
predict()
saveRDS(model_GLM, "modelo_GLM.rds")

# Support Vector Machine
set.seed(42)
model_SVM <- train(SITUACAO ~ . - GEOCOD_IBGE - Referencia, data = training, 
                  method = "svmLinear", trControl = ctrl, 
                  preProcess = c("center", "scale"))
saveRDS(model_SVM, "modelo_SVM.rds")

# k-Nearest Neighbors
set.seed(42)
model_KNN <- train(SITUACAO ~ . - GEOCOD_IBGE - Referencia, data = training, 
                   method = "knn", trControl = ctrl, 
                   preProcess = c("center", "scale"), tuneLength = 10)
saveRDS(model_KNN, "modelo_KNN.rds")

# Bagging
set.seed(42)
model_ADABAG <- train(SITUACAO ~ . - GEOCOD_IBGE - Referencia, data = training, 
                  method = "AdaBag", trControl = ctrl, boos = TRUE, 
                  coeflearn = "Breiman", preProcess = c("center", "scale"), 
                  tuneLength = 10)
saveRDS(model_ADABAG, "modelo_ADABAG.rds")

# Extreme Gradient Boosting
set.seed(42)
model_GBM <- train(SITUACAO ~ . - GEOCOD_IBGE - Referencia, data = training, 
                      method = "gbm", trControl = ctrl, #distribution = "gaussian",
                      preProcess = c("center", "scale"), tuneLength = 10)
saveRDS(model_GBM, "modelo_GBM.rds")

# Random Forests
set.seed(42)
ctrl <- trainControl(method = "oob")
model_RF <- train(SITUACAO ~ . - GEOCOD_IBGE - Referencia, data = training, 
                   method = "rf", trControl = ctrl, 
                   preProcess = c("center", "scale"), tuneLength = 10)
saveRDS(model_RF, "modelo_RF.rds")


################################ ANALISAR RESULTADOS ###########################

# analise entre Folds de treinamento
results <- resamples(list(GLM = model_GLM, SVM = model_SVM, KNN = model_KNN,
                          ADABAG = model_ADABAG, GBM = model_GBM))
scales <- list(x=list(relation="free"), y=list(relation="free"))
bwplot(results, scales=scales)
densityplot(results, scales)
parallelplot(results)
xyplot(results, models = c("SVM", "GLM"))
differences <- diff(results)

# valores preditos pelos diferentes modelos
DT[, `:=`(predito_GLM = predict(model_GLM, newdata = DT),
          predito_SVM = predict(model_SVM, newdata = DT),
          predito_KNN = predict(model_KNN, newdata = DT),
          predito_ADABAG = predict(model_ADABAG, newdata = DT),
          predito_GBM = predict(model_GBM, newdata = DT),
          predito_RF = predict(model_RF, newdata = DT))]
confusionMatrix(table(DT$SITUACAO, DT$predito_SVM))

performance <- data.table()
for (method in c("GLM", "SVM", "KNN", "ADABAG", "GBM", "RF")) {
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

saveRDS(DT, "resultados.rds")
saveRDS(performance, "performance-modelos.rds")
fwrite(performance[Dataset == "Treino", !c("Dataset")], 
       "performance-treino.csv")
fwrite(performance[Dataset == "Validacao", !c("Dataset")], 
       "performance-validacao.csv")

parallelplot(~ performance[Dataset == "Validacao",c(4:21)], 
             performance[Dataset == "Validacao"], groups = Method, 
             scales = list(x=list(relation="free")), xlim = c(0, 1))
#############
stopCluster(cls)