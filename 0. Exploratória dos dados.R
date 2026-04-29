library(arrow)
library(dplyr)
library(lubridate)
library(tidyr)
library(here)
library(readr)
library(tidyverse)
library(purrr)

pasta <- "C:/Users/Patrick/Desktop/Capitalizacao - MAG 2025/Mongeral - 2026-02-11 - Persistência - Capitalização VI - v3"
arquivos <- list.files(pasta, pattern = "parquet$", full.names = TRUE)
base <- map_dfr(arquivos, read_parquet)
write_parquet(base, "C:/Users/Patrick/Desktop/Capitalizacao - MAG 2025/base_original.parquet")

base <- read_parquet("C:/Users/Patrick/Desktop/Capitalizacao - MAG 2025/base_original.parquet")

# 6.154.928 observações na base não tratada

# 1) Verificar o número de linhas na base sem nenhum tratamento

total_linhas <- nrow(base)
cat("Total de linhas:", total_linhas)

# 2) Remover as duplicatas com distinct e entender se tem alguma linha totalmente igual na base
base_unica <- base %>% 
  distinct()

# Verificar quantas linhas foram removidas
linhas_removidas <- nrow(base) - nrow(base_unica)
cat("Linhas duplicadas removidas:", linhas_removidas)

# Não existem mais duplicatas na base.

# 3) Ver o nome das colunas da Base

colunas <- names(base)
print(colunas)

# 4) Ver quantos valores únicos temos por coluna na base

valores_unicos <- base %>%
  summarise(across(everything(), n_distinct)) %>%
  collect()

valores_unicos_longo <- valores_unicos %>% 
  pivot_longer(everything(), names_to = "coluna", values_to = "qtd_unicos")

print(n = 33,  valores_unicos_longo)

# Então, ao tomar o maior tempoCasa para cada codigoItemContratado da nossa base queremos ter 181.185 linhas

# 5) Ver qual é o mínimo e o máximo da coluna tempoCasa

min_max_tempo <- base %>%
  summarise(
    min_tempo = min(tempoCasa, na.rm = TRUE),
    max_tempo = max(tempoCasa, na.rm = TRUE)
  ) %>%
  collect()

print("Mínimo e Máximo de tempoCasa:")
print(min_max_tempo)

# 6) Obter o maior valor de tempoCasa para cada codigoItemContratado (mantendo empates)

base_max_tempo <- base_unica %>%
  group_by(codigoItemContratado) %>%
  slice_max(order_by = tempoCasa, n = 1) %>% # with_ties = TRUE é o comportamento padrão
  ungroup()

linhas_finais <- nrow(base_max_tempo)
cat("Total de linhas após o filtro do maior tempoCasa:", linhas_finais, "\n")

# Chegamos no número previsto de 181.185 linhas com o maior tempoCasa
# Isso significa que os codigoItemContratado não tem mais de uma linha com o mesmo valor máximo de tempoCasa

# 7) Contagem de linhas com tempoCasa <= 120 ou <= 121 para considerar apenas os últimos 10 anos

# <= 121

qtd_121 <- base_max_tempo %>%
  filter(tempoCasa <= 121) %>%
  nrow()

# <= 120

qtd_120 <- base_max_tempo %>%
  filter(tempoCasa <= 120) %>%
  nrow()

cat("Total até 121 meses:", qtd_121)
cat("Total até 120 meses:", qtd_120)
cat("Registros com exatos 121 meses:", qtd_121 - qtd_120)

# 8) Contar itens com dataInicioVigenciaCobertura >= 2015-06-01

qtd_pos_2015 <- base_max_tempo %>%
  filter(dataInicioVigenciaCobertura >= as.Date("2015-06-01")) %>%
  nrow()

cat("Quantidade de itens com vigência a partir de 01/06/2015:", qtd_pos_2015, "\n")

# O número é diferente quando fazemos esse tratamento e quando fazemos o tratamento de até 121 meses ou até 120 meses

# Salvando a base filtrada para verificar o sumário da variável tempoCasa com a condição dataInicioVigenciaCobertura >= 2015-06-01
base_max_tempo_filtrada <- base_max_tempo %>%
  filter(dataInicioVigenciaCobertura >= as.Date("2015-06-01"))

min_max_tempo_filtro <- base_max_tempo_filtrada %>%
  summarise(
    min_tempo = min(tempoCasa, na.rm = TRUE),
    max_tempo = max(tempoCasa, na.rm = TRUE)
  ) %>%
  collect()

print("Mínimo e Máximo de tempoCasa:")
print(min_max_tempo_filtro)

# Vamos usar a base_max_tempo por enquanto e definir qual tratamento será utilizado na reunião
# Vamos definir entre tempoCasa <= 120 ou 121 ou dataInicioVigenciaCobertura <= 01/06/2015

# 9) Tratando a variável de statusCompetencia para transformar ela em Cancelado ou Censurado

# Definindo as listas de mapeamento (conforme enviado)
status_remove <- c(
  "TRANSFERÊNCIA - C15", "CONTRATO ANULADO - C16", "PAGAMENTO ÚNICO - D01",
  "REMIDO - D02", "PRAZO CUMPRIDO - C06", "ENCERRAMENTO APÓLICE - C13"
)

status_ativos <- c(
  "ATIVA EM DIA - A00", "ATIVA 1 ATRASO - A01", "ATIVA 2 ATRASOS - A02",
  "ATIVA 3 ATRASOS - A03", "ATIVA 4 ATRASOS - A04", "ATIVA 5 ATRASOS - A05"
)

status_sinistros <- c(
  "ÓBITO - C03", "AUXÍLIO FUNERAL - B07", "DIT/DMH - B10", "DOENÇAS GRAVES - B21",
  "INVALIDEZ (PAGAMENTO ÚNICO IPA) - B11", "INVALIDEZ (PAGAMENTO ÚNICO IPD) - B09",
  "LIQUIDAÇÃO ESPECIAL BENEFÍCIO - B05", "PECÚLIO - B03", "PENSÃO - B01",
  "PERDA DE HABILITAÇÃO DE VÔO / PAGAMENTO ÚNICA - B16", "RENDA INVALIDEZ - B18",
  "BENEFÍCIO NEGADO - C10", "RESGATE - B02"
)

status_cancelados <- c(
  "CANCELADO PELA REGULAÇÃO - C22", "CANCELADO POR DEPENDÊNCIA - C21",
  "INADIMPLÊNCIA - C01", "DESISTÊNCIA - C05", "ÓRGÃO CONSIGNANTE CANCELADO"
)

# Aplicando os filtros e a criação da coluna 'status' na base final
base_modelagem_v0 <- base_max_tempo %>%
  # Remove status que não entram na conta de persistência
  dplyr::filter(!statusCompetencia %in% status_remove) %>%
  dplyr::mutate(
    status = dplyr::case_when(
      statusCompetencia %in% c(status_ativos, status_sinistros) ~ "Censura",
      statusCompetencia %in% status_cancelados                 ~ "Cancelado",
      TRUE                                                     ~ "Cancelado" # Catch-all para outros status de saída
    )
  )

# Retornando o novo número de linhas e a distribuição do Status
novo_total_linhas <- nrow(base_modelagem_v0)

cat("Número de linhas após filtros (base_modelagem):", novo_total_linhas, "\n")

# Total de linhas caiu de 181.185 para 171.155 com os status que deveriam ser removidos

# 10) Verificando a porcentagem de Censurados / Cancelados
cat("Distribuição Censurados x Cancelados (status):\n")
base_modelagem_v0 %>% 
  count(status) %>% 
  mutate(percentual = round(n / sum(n) * 100, 2)) %>% 
  print()

# Próximos passos
# 11) Definir qual abordagem vai ser utilizada (tempoCasa (120 ou 121) ou dataInicioVigenciaCobertura)
# 12) Criar uma nova coluna transformando status em 0 ou 1
# 13) Observar nova porcentagem de Censurados / Cancelados a partir do tratamento
# 14) Escolher quais colunas vão ser observadas no modelo
# 15) Realizar análise exploratória dos dados com gráficos + persistência empírica