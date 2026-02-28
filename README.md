# 🌟 Celestial - NASA Data Pipeline

Este projeto de Engenharia de Dados consiste na construção de uma infraestrutura de dados escalável para coleta, processamento e modelagem analítica de eventos astronômicos e objetos próximos à Terra (NEOs). 
O projeto consolida dados das APIs **NeoWs**, **Fireball** e **CAD (JPL)** em um Data Warehouse estruturado.

As APIs da NASA fornecem dados incríveis, mas eles vêm em um formato "bagunçado" para análise (JSON aninhado). Este projeto automatiza o trabalho de buscar esses dados todos os dias e transformá-los em informações úteis, como:
- Asteroides: Quais estão passando perto da Terra hoje?
- Bólidos: Onde caíram meteoros brilhantes recentemente?
- Histórico: Como foi o movimento espacial nos últimos 6 meses?

---

## 🏗️ Arquitetura do Pipeline

<img width="1609" height="872" alt="Image" src="https://github.com/juniorsilvacc/astrotrack-etl-dw/blob/master/arquitetura.png" />

## Para garantir que a informação seja confiável, usamos o padrão de Medalhão:

- Bronze 🥉
  - Guardamos o dado exatamente como ele veio da NASA. Se precisarmos conferir algo no futuro, o original está lá.

- Silver 🥈
  - Removemos o que não serve, corrigimos formatos de data, calculamos o tamanho médio dos asteroides e salvamos de forma organizada e rápida (Parquet).

- Gold 🥇
  - Organizamos os dados em tabelas especiais (Fatos e Dimensões) que facilitam a criação de gráficos e relatórios profissionais.

---

## 📂 Estrutura do Projeto
```text
src/
├── shared/                 # Ferramentas e Infraestrutura Reutilizável
│   ├── storage/            # Handlers para Local Files (IO) e Banco de Dados (DB)
│   ├── drivers/            # Motores de conexão (SQLAlchemy/Postgres) e Request
│   └── integrations/       # Clientes de comunicação com APIs NASA
├── bronze/                 # Scripts de Ingestão (Extract)
├── silver/                 # Scripts de Limpeza e Padronização (Transform)
├── gold/                   # Modelagem SQL e Lógica Analítica
│   └── analytics/          # Queries SQL organizadas por domínio FCT(Fato) e DIM(Dimensão) (CAD, NeoWS, Fireball)
└── main.py                 # Ponto de entrada para execução manual
```

---

## 🛠️ Tecnologias Utilizadas
### Core
- **Orquestração:** Apache Airflow (TaskFlow API com processamento paralelo).
- **Linguagem:** Python 3.12 (Pandas para processamento, SQLAlchemy para persistência).
- **Banco de Dados:** PostgreSQL (Data Warehouse).
- **Infraestrutura:** Docker & Docker Compose (Isolamento de ambiente).
- **Armazenamento Colunar:** Apache Parquet.
  
### Bibliotecas Python
- **pandas:** Manipulação e transformação de dados.
- **requests:** Requisições HTTP para a API.
- **SQLAlchemy:** ORM para interação com o banco de dados.
- **psycopg2:** Driver PostgreSQL.
- **python-dotenv:** Gerenciamento de variáveis de ambiente.

---

## ⚙️ Detalhamento das Etapas de Transformação (Silver Layer)
O coração do projeto reside na camada Silver, onde os dados brutos e aninhados das APIs da NASA são submetidos a um rigoroso processo de limpeza e normalização.

### ☄️ API Fireball (Bólidos)
Os dados de impactos de meteoros possuem coordenadas geográficas e componentes de velocidade vetorial.

- **Tratamento de Coordenadas:** Conversão de direções cardeais (N/S, E/W) em valores numéricos decimais para plotagem em mapas.

- **Cálculo de Energia:** Padronização das unidades de energia radiada (Joules) e impacto total estimado (kt).

- **Normalização:** Extração de dados aninhados para uma estrutura tabular limpa.

### 🛰️ API NeoWs (Asteroides)
A API de Objetos Próximos à Terra é a mais complexa devido à estrutura de datas e aproximações múltiplas.

- **Normalização Recursiva:** Extração de asteroides listados por data dentro da chave `near_earth_objects`.

- **Deduplicação Inteligente:** Remoção de duplicatas baseada no `asteroide_id` e `data_aproximacao`.

- **Cálculo de Diâmetro:** Criação da métrica `diameter_avg_km` baseada na média entre o diâmetro mínimo e máximo estimado pela NASA.

- **Conversão de Tipos:** Transformação de strings de velocidade (`km/h`) e distância (`km`) em tipos numéricos (`loat64`) para cálculos analíticos.

### 🔭 API CAD (JPL - Close Approach Data)
Focada em aproximações calculadas, requer filtros específicos para evitar sobrecarga de dados.

- **Filtros de Data:**  Implementação de lógica para capturar apenas aproximações em janelas temporais relevantes.

- **Velocidade Relativa:**  Separação e limpeza de dados de velocidade relativa em relação à Terra.

- **Enriquecimento:**  Preparação dos dados para o Join na camada Gold com as características físicas dos asteroides vindas do NeoWs.

---

## 💎 O Processo de Transformação (Lógica do Código)
Para garantir a qualidade, todas as transformações seguem este fluxo programático:

1. **Leitura Colunar:** Carregamento eficiente dos arquivos JSON originais.
2. **Drop de Redundâncias:** Remoção de colunas irrelevantes (links internos da NASA, unidades de medida duplicadas como milhas/pés).
3. **Renomeação Técnica:** Tradução e padronização dos nomes das colunas para o português e para o padrão snake_case.
4. **Persistência Colunar:** Salvamento em formato Parquet com compressão snappy, reduzindo o espaço em disco em até 70% comparado ao CSV/JSON.
5. **Upsert no Database:** Utilização da função customizada save_dataframe (no db_handler.py) que gerencia a criação automática de tabelas e a atualização dos dados no PostgreSQL.

---

## 📊 Modelagem Dimensional (GOLD)
O Data Warehouse segue o modelo Star Schema:

- Fatos: `fct_impactos (Fireball)`, `fct_aproximacoes (CAD)`, `fct_monitoramento_neows (NeoWS)`.
- Dimensões: `dim_localizacao (Fireball)`, `dim_tempo (Fireball)`, `dim_objeto_espacial (CAD)`, `dim_asteroide_perfil_neows (NeoWS)`.

---

## 🔄 Orquestração (Airflow)
A pipeline é inteligente e resiliente:
1. **Paralelismo**: As ingestões de NeoWs, Fireball e CAD ocorrem simultaneamente para otimizar o tempo.
2. **Carga Histórica Automática**: O sistema detecta a ausência de dados históricos (Backfill) e provisiona o banco automaticamente na primeira execução.
3. **Integridade**: A camada Gold possui dependência estrita do sucesso de todas as camadas Silver anteriores.

---

## ⛓️ Fluxo da DAG no Airflow
**Arquivo:** [astrotrack_dag.py](https://github.com/juniorsilvacc/astrotrack-etl-dw/blob/master/dags/astrotrack_dag.py)

### Configuração da DAG
```bash
@dag(
  dag_id='astrotrack_etl',
  default_args={
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
  },
  description='Pipeline ETL: NeoWS, Fireball e CAD',
  schedule='0 4 * * *', # Executa às 04:00 AM (Todos os dias)
  start_date=datetime(2026, 2, 1),
  catchup=False,
  tags=['nasa', 'cad', 'neows', 'fireball', 'etl']
) 
```

### Tasks Defininas
```bash
@task
  def process_fireball():
    run_extract_fireball()
    run_transform_fireball()
      
  @task
  def process_neows():
    run_extract_neows()
    run_neows_historical()
    run_neows_daily()

  @task
  def process_cad():
    run_extract_cad()
    run_transform_cad()

  @task
  def build_gold():
    build_gold_layer()
  
  [process_fireball(), process_neows(), process_cad()] >> build_gold()
```

### Por que usar Parquet entre transform e load?
- Formato binário eficiente
- Preserva tipos de dados (datetime, float, etc.)
- Evita problemas com serialização do Airflow

---

## 🚀 Instalação e Configuração
### 1️⃣ Clone o repositório:
```bash
# 1. Clone o repositório:
git clone [https://github.com/seu-usuario/astrotrack-etl.git](https://github.com/seu-usuario/astrotrack-etl.git)
```

### 2️⃣ Obtenha sua API Key NeoWS - NASA
1. Acesse [APIs Nasa](https://api.nasa.gov/) 
2. Crie uma conta gratuita
3. Gere sua API Key e recebe por email
4. Guarde sua chave para o próximo passo
   
### 3️⃣Configure as variáveis no .env
```bash
DB_HOST=postgres
DB_NAME=airflow
DB_USER=airflow
DB_PASSWORD=airflow
DB_PORT=5432

AIRFLOW_UID=501

# APIs Integrations
API_FIREBALL_NASA=https://ssd-api.jpl.nasa.gov/
API_CAD_NASA=https://ssd-api.jpl.nasa.gov/
API_NEOWS_NASA=https://api.nasa.gov/neo/rest/v1

# API KEY (Inserir a chave da api) 
API_KEY_NEOWS=
```

### 4️⃣ Inicialize o Ambiente Airflow
```bash
# Crie a estrutura de pastas necessária
mkdir -p ./dags ./logs ./plugins ./config ./data ./src ./notebooks

# Configure as permissões (Linux/Mac)
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

### 5️⃣ Inicie os Containers Docker
```bash
# Inicie todos os serviços
docker-compose up -d
```

Aguarde alguns minutos para todos os serviços iniciarem.

### 6️⃣ Verifique se tudo está rodando
```bash
docker-compose ps
```

---

## ▶️ Como Executar
### 1️⃣ Acesse a Interface do Airflow
Abra seu navegador em: http://localhost:8080


### Credenciais padrão:
```text
Username: airflow
Password: airflow
```

2️⃣ Ative a DAG
1. Na interface do Airflow, localize a DAG astrotruck-etl
2. Clique no botão de Acionar/Trigger para ativá-la
3. A DAG está configurada para executar a cada 1 hora

---

### 👷 Autor
[Github](https://www.linkedin.com/in/juniiorsilvadev/) 