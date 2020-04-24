# Alternância de poder em eleições municipais brasileiras

Repositório com o código-fonte do trabalho "**Alternância de poder em eleições municipais brasileiras (2012-2016): retroprojeções com aprendizado de máquinas**" (BARON, *no prelo*), elaborado como [Trabalho de Conclusão de Curso da Especialização em Ciência de Dados da Universidade Federal Fluminense](http://www.automata.uff.br/index.php/especializacao/15-processo-seletivo/36-processo-seletivo-ciencia-de-dados-2-2018).


## Instruções

Atualmente, o pré-processamento dos dados depende de rodar manualmente todos os arquivos com extensão **.R** existentes nas subpastas do diretório **/etc**. Esses arquivos instalarão todas as dependências necessárias e baixarão todas os dados das fontes oficiais (pode levar horas, dependendo da conexão com a internet).

Por fim, basta rodar o arquivo **geral.R**, no diretório raíz. Esse script treina os modelos a partir dos dados pré-processados. Ao final do processo, são salvos na pasta raíz um total de seis modelos, cada um a partir de um algortimo distinto de aprendizado de máquinas: 
- Generalized Linear Models (GLM);
- Support Vector Machines (SVM);
- k-Nearest Neighbors (KNN);
- Random Forests (RF);
- Adabagging (ADABAG);
- Extreme Gradient Boosting (GBM).

Os dados utilizados para treinamento do modelo, com a classificação original, as variáveis utilizadas no treinamento e o valor predito por cada um dos seis modelos é gravado na pasta raís, no arquivo **resultados.rds**.

Para convêniência, também são gerados os arquivos **performance-treino.csv** e **performance-validacao.csv**. Cada um desses arquivos traz métricas sobre o desempenho dos diferentes modelos nos conjuntos de treinamento e de validação criados no processo de modelagem.