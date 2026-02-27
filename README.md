# 🌟 Celestial: NASA Data Pipeline

Este projeto de Engenharia de Dados consiste na construção de uma infraestrutura escalável para coleta, processamento, armazenar e visualizar eventos astronômicos e objetos próximos à Terra (NEOs). Utilizando APIs oficiais da NASA, o projeto integra dois fluxos de dados distintos: eventos de bólidos (bolas de fogo) e rastreamento de asteroides (NEOs).

## 🚀 Objetivo do Projeto
...

## 📐 Arquitetura

IMG

## 💎 Padrão de Design de Dados

### `O projeto segue o padrão Medallion Architecture`

### Bronze 🥉
- Dados brutos
- Sem perda de informação
- Formato JSON
- Estrutura de origem

### Silver 🥈
- Dados tratados e padronizados
- Conversão de tipos
- Aplicação de regras de negócio
- Formato Parquet

### Gold 🥇
- Modelagem Dimensional (Fato e Dimenssão)
- Dados prontos para análise

| Dataset          | Valor Estratégico (O que o seu chefe perguntaria)
|------------------|-------------------------------------------------
| Fireball         | "Onde os meteoros estão caindo com mais energia? Nossos satélites estão cobrindo essa área?"  
| NeoWs            | "Quais asteroides passarão perto da Terra nos próximos 7 dias e qual o risco real?"  
| CAD              | "Qual a frequência de aproximações de grande porte por década?" 

## 📂 Estrutura do Projeto
...

## 🛠️ Tecnologias Utilizadas
- **Python**
  - Requests
  - Pandas
  - Dotenv
  - Parquet
- **Airflow**
- **PostgreSQL**
- **Docker**

## 🔄 Pipeline ETL

### 1️⃣ Extração (Extract) 📥
...
### 2️⃣ Transformação (Transform) ⚙️
...
### 3️⃣ Carga (Load) 📤
...

## 📊 Modelagem de Dados (GOLD)

## ▶️ Como Executar o Projeto
### 🐳 Ambiente Docker (RECOMENDADO)

```bash
...
```

### 🐍 Ambiente Local

```bash
...
```

### `Em Construção...`