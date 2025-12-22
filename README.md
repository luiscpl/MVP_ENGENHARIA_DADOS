# MVP Engenharia de Dados

# 1️⃣ Objetivo
___

### 1.1 Descrição do MVP ###

Este MVP tem como objetivo desenvolver um pipeline de dados em nuvem para analisar a evolução do custo médio dos atendimentos ambulatoriais (relativos a determinados procedimentos) e do custo médio das internações (total e por regime de internação), conforme registros do padrão TISS – Troca de Informações em Saúde Suplementar, no período de 2015 a 2024.
Na etapa inicial de extração e implementação, foram utilizadas ferramentas como Jupyter-Python e SQL Postgres para coleta e organização dos dados. Em seguida, o processo foi refinado com tecnologias em nuvem, empregando Databricks e Delta Lake, abrangendo desde a ingestão até a disponibilização de dados prontos para análise.
O resultado esperado é uma base estruturada e confiável que permita identificar tendências, variações e fatores determinantes nos custos de saúde suplementar ao longo do período estudado.


### 1.2 Problema central ###

Atualmente, não há uma visão integrada e de fácil acesso sobre os custos de internação e atendimento ambulatorial no Brasil. Essa ausência dificulta a identificação de tendências, anomalias e dos principais fatores que influenciam a gestão da saúde suplementar.
Este MVP tem como propósito suprir essa necessidade, disponibilizando uma base de dados confiável e estruturada, capaz de sustentar análises tanto operacionais quanto estratégicas, e apoiar a tomada de decisões mais informadas no setor.



### 1.3 Escopo e etapas do pipeline ###

- Busca, extração dos arquivos TISS, cruzamento das bases consolidadas e detalhadas para montagem única das tabelas  mensais/anuais e seleção dos procedimentos dos atendimentos ambulatoriais e a base total de internação para o período 2015–2024 será realizada pelo SQL Postgres (criação da camada bronze parte 1).
- Ingestão e armazenamento: carregamento em Delta Lake para garantir governança, versionamento e performance.
- Modelagem: padronização, limpeza e harmonização dos dados (identificadores, datas, valores, procedimentos).
- Carga e transformação: criação de camadas (bronze parte 2, silver, gold) para suportar diferentes níveis de consumo.
- Análise e visualização: geração de métricas, dashboards e relatórios para identificar padrões de gasto, outliers e drivers de custo.

### 1.4 Perguntas que o projeto pretende responder ###

- Quais são as tendências temporais de custo médio por tipo de atendimento ambulatorial (por alguns procedimentos) e Internação (total de internação e regime de internação)?


### 1.5 Entregáveis esperados ###

- Base de dados consolidada em Delta Lake cobrindo 2015 – 2024.
- Conjunto de pipelines automatizados para ingestão, transformação e atualização dos dados.
- Dashboards e relatórios analíticos com indicadores-chave e insights acionáveis.
- Documentação técnica e guia de uso para replicação e manutenção.
Ao final do projeto, espera-se dispor de uma plataforma confiável que permita à gestão pública e a analistas compreender melhor os determinantes dos custos em saúde no Brasil e apoiar decisões baseadas em dados.

