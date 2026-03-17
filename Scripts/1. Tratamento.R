library(arrow)
library(dplyr)
library(lubridate)
library(tidyr)
library(here)
library(readr)
library(tidyverse)
library(purrr)

pasta <- "C:/Users/Patrick/Desktop/Capitalização - MAG/Base de dados/Dados"
base <- arrow::open_dataset(pasta)

base_tratada <- base %>%
  
  # 1. Filtro de datas
  filter(
    dataInicioVigenciaCobertura >= as.Date("2015-06-01"),
    dataFimStatus >= as.Date("2020-06-01")
  ) %>%
  
  # 2. Seleção de colunas de interesse
  select(
    sexoCliente,
    categoriaCliente,
    formaCobranca,
    periodicidadeCobranca,
    tipoOrgaoConsignante,
    situacaoCompetencia,
    situacaoPremioPrevisto,
    valorCapitalSegurado,
    valorPremioPrevisto,
    carteira,
    dataInicioVigenciaCobertura, # CORRIGIDO: Esta é a data que precisamos para o cálculo!
    dataFimStatus
  ) %>%
  
  # 3. Criação da coluna "status"
  mutate(
    status = case_when(
      situacaoCompetencia == "Ativo" ~ "Censura",
      situacaoCompetencia == "Cancelado" ~ "Cancelado",
      situacaoCompetencia == "Cancelado por beneficio" ~ "Cancelado",
      TRUE ~ "Outros"
    )
  ) %>%
  
  collect() %>%
  
  # 4. Cálculo de tempo (meses) e criação da variável delta
  mutate(
    data_final_calculo = case_when(
      status == "Censura" ~ as.Date("2026-03-01"),
      TRUE ~ as.Date(dataFimStatus) 
    ),
    tempo_dias = as.integer(data_final_calculo - as.Date(dataInicioVigenciaCobertura)),
    tempo_mes = as.integer(tempo_dias / 30),
    delta = ifelse(status == "Censura", 0, 1)
  ) %>%
  
  # 5. Exclui datas com tempo_mes negativo
  filter(tempo_mes > 0)

caminho_destino <- "C:/Users/Patrick/Desktop/Capitalização - MAG/base_tratada_final.rds"

saveRDS(base_tratada, file = caminho_destino)
