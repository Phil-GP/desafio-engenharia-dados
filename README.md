# Desafio Técnico - Pipeline Airflow & Apache Solr

Este projeto implementa um pipeline de dados orquestrado pelo Apache Airflow para ler, higienizar e inserir dados no Apache Solr, focado em performance e tolerância a falhas.

## Pré-requisitos
- Docker e Docker Compose instalados.

## Como Executar o Projeto

1. **Clone o repositório:**
   ```bash
   git clone https://github.com/Phil-GP/desafio-engenharia-dados
   ```

2. **Inicie a infraestrutura:**
   O arquivo `docker-compose.yml` unificado configura o Airflow, PostgreSQL, Redis e Solr em uma mesma rede lógica, além de instalar as dependências de Python necessárias (`pandas` e `pysolr`).
   ```bash
   docker-compose up -d
   docker-compose exec --user airflow airflow python -m pip install pysolr pandas
   ```

3. **Acesse as Interfaces:**
   - **Airflow Web UI:** [http://localhost:8080](http://localhost:8080) (Login: `admin` / Senha: `admin`)
   - **Solr Admin UI:** [http://localhost:8983](http://localhost:8983)

4. **Execute o Pipeline:**
   - Na interface do Airflow, procure pela DAG `solr_ingestion_pipeline`.
   - Clique em **Trigger DAG**.
   - Acesse os logs das tasks `format_csv_data` e `insert_solr_data` para acompanhar o tratamento de erros e as mensagens de progresso.
