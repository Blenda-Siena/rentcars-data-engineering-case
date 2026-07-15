# Data Dictionary — Case Técnico Senior Data Engineer
**Rentcars | Processo Seletivo**
Gerado em: 2025-03-31 | Período dos dados: 01/10/2024 – 31/03/2025

---

## Visão Geral dos Datasets

| Tabela | Descrição | Linhas | Colunas |
|---|---|---|---|
| `raw_events` | Clickstream de navegação (alto volume) | 150.645 | 14 |
| `raw_transactions` | Transações financeiras com late-arriving data | 25.125 | 15 |
| `raw_partner_catalog` | Catálogo de parceiros com evolução de schema | 88 | 13 |
| `raw_payment_stream` | Eventos de pagamento simulando Kafka/Kinesis | 30.188 | 14 |
| `pipeline_runs` | Histórico de execuções de pipelines (observabilidade) | 3.000 | 18 |

> ⚠️ **Atenção:** Os dados contêm problemas propositais — late-arriving events, schema drift, duplicatas de at-least-once delivery, outliers de volume e valor, inconsistências de case e campos nulos. Detectar e tratar cada problema faz parte da avaliação.

---

## Diagrama de Fluxo de Dados

```
Fontes Externas                 Ingestão (Raw)                Transformação           Serving
─────────────────               ──────────────────────         ───────────────         ──────────
Clickstream (web/app) ────────► raw_events.csv         ─┐
                                                         ├──► staging_events     ─┐
Gateways de pagamento ────────► raw_transactions.csv   ─┤    staging_transactions │
                                raw_payment_stream.csv ─┘    staging_payments     ├──► API /metrics
                                                                                   │    API /partners
Partner APIs (REST)   ────────► raw_partner_catalog.csv ────► staging_partners   ─┘    API /transactions

Airflow / Orquestrador ───────► pipeline_runs.csv ──────────► Prometheus/Grafana (observabilidade)
```

---

## raw_events

**Descrição:** Clickstream de alto volume gerado pelo site e app da Rentcars. Cada linha representa um evento de interação do usuário: visualização de página, busca, clique em oferta, início ou conclusão de checkout. É a principal fonte para análise de funil e comportamento de usuário.

**Granularidade:** 1 linha por evento

| Coluna | Tipo esperado | Nullable | Descrição | Exemplos | Problemas conhecidos |
|---|---|---|---|---|---|
| `event_id` | VARCHAR (UUID) | NÃO | Identificador único do evento | `3f2a8b1c-...` | ⚠️ ~600 eventos duplicados (at-least-once delivery) |
| `event_type` | VARCHAR | NÃO | Tipo de evento | `page_view`, `search`, `booking_confirm` | — |
| `session_id` | VARCHAR (UUID) | NÃO | Sessão de navegação associada | `ab1c2d3e-...` | ⚠️ 1 session_id com 120 eventos em < 4 min (bot) |
| `user_id` | VARCHAR (UUID) | SIM (10%) | Usuário autenticado | `7b5c2164-...` | Nulo = visitante anônimo |
| `event_ts` | TIMESTAMP | NÃO | Timestamp do evento no cliente | `2024-11-15 14:32:10` | ⚠️ ~2% com `ingest_date` até 10 dias após `event_ts` (late-arriving) |
| `page` | VARCHAR | SIM (3%) | Página onde o evento ocorreu | `search`, `offer-detail`, `/` | — |
| `partner_id` | VARCHAR | SIM (45%) | Parceiro relacionado ao evento | `PRT0007` | Nulo = evento sem contexto de parceiro |
| `device` | VARCHAR | NÃO | Tipo de dispositivo | `desktop`, `mobile` | ⚠️ Inconsistência de case: `Mobile`, `DESKTOP` |
| `country` | VARCHAR | NÃO | País do usuário (ISO 3166-1) | `BR`, `AR` | ⚠️ Inconsistência de case: `br`, `Br` |
| `channel` | VARCHAR | SIM (7%) | Canal de aquisição | `google_cpc`, `direct` | — |
| `price_usd` | FLOAT | SIM (55%) | Valor de referência do evento em USD | `249.90` | Nulo = evento sem contexto de preço |
| `metadata_json` | VARCHAR (JSON) | SIM (10%) | Payload adicional serializado em JSON | `{"ab_test":"A","page_load_ms":450}` | ⚠️ Schema do JSON não é fixo — requer tratamento de campos dinâmicos |
| `is_bot_flag` | BOOLEAN | NÃO | Classificação de bot pelo sistema | `True`, `False` | ~3% são bots; devem ser filtrados antes de análises |
| `ingest_date` | DATE | NÃO | Data de ingestão no Data Lake | `2024-11-15` | ⚠️ Pode ser posterior ao `event_ts` (late-arriving data) |

**Regras de negócio:**
- Eventos com `is_bot_flag = True` devem ser segregados em tabela separada
- A coluna `ingest_date` deve ser usada como chave de particionamento no S3/Parquet
- Eventos de `booking_confirm` devem ser reconciliados com `raw_transactions`

---

## raw_transactions

**Descrição:** Transações financeiras geradas pelos gateways de pagamento. Contém o resultado financeiro de cada reserva — aprovação, recusa, reembolso. É a fonte de verdade para receita, métricas de pagamento e reconciliação contábil.

**Granularidade:** 1 linha por tentativa de transação

| Coluna | Tipo esperado | Nullable | Descrição | Exemplos | Problemas conhecidos |
|---|---|---|---|---|---|
| `transaction_id` | VARCHAR | NÃO | Identificador único da transação | `TXN00000001` | ⚠️ ~125 linhas duplicadas |
| `booking_ref` | VARCHAR | SIM (4%) | Referência da reserva associada | `BKG0012345` | — |
| `partner_id` | VARCHAR | NÃO | Parceiro da transação | `PRT0003` | Deve existir em `raw_partner_catalog` |
| `user_id` | VARCHAR (UUID) | SIM (6%) | Usuário que realizou a transação | `8b95538c-...` | — |
| `created_at` | TIMESTAMP | NÃO | Timestamp de criação da transação | `2025-01-10 18:45:00` | — |
| `ingest_ts` | TIMESTAMP | NÃO | Timestamp de ingestão no Data Lake | `2025-01-10 20:12:00` | ⚠️ ~3% chegam com 48-168h de atraso (late-arriving data) |
| `amount` | FLOAT | NÃO | Valor da transação | `450.00` | ⚠️ ~0.7% com valor > R$20.000 (outlier); ~0.5% com valor negativo |
| `currency` | VARCHAR | NÃO | Moeda (ISO 4217) | `BRL`, `USD` | — |
| `status` | VARCHAR | NÃO | Status da transação | `confirmed`, `failed`, `refunded` | ⚠️ Inconsistência de case: `CONFIRMED`, `Confirmed` |
| `payment_method` | VARCHAR | SIM (8%) | Forma de pagamento | `credit_card`, `pix`, `boleto` | — |
| `gateway` | VARCHAR | SIM (10%) | Gateway processador | `stripe`, `cielo`, `adyen` | — |
| `retry_count` | INTEGER | SIM (5%) | Número de tentativas até a transação | `0`, `1`, `2` | — |
| `error_code` | VARCHAR | SIM (80%) | Código de erro quando falhou | `E001`, `TIMEOUT` | Nulo = sem erro |
| `notes` | VARCHAR | SIM (25%) | Campo livre / legado | `"ok"`, `{"legacy_id":12345}` | ⚠️ ~1% contém JSON aninhado (schema drift de versão anterior da API) |
| `processing_ms` | INTEGER | SIM (4%) | Latência de processamento no gateway (ms) | `320`, `8500` | — |

**Regras de negócio:**
- Somente transações com `status = confirmed` ou `completed` entram no cálculo de receita
- `amount` deve ser > 0 para transações aprovadas
- Deduplicação por `transaction_id` é obrigatória antes de qualquer agregação financeira
- `ingest_ts` deve ser usado como chave de particionamento para garantir idempotência no pipeline incremental

---

## raw_partner_catalog

**Descrição:** Catálogo de parceiros ingerido via API REST de cada locadora. Simula um cenário real de evolução de schema ao longo do tempo: versão v1 (campos básicos), v2 (adiciona SLA e rating), v3 (adiciona endpoint de webhook). O pipeline deve ser capaz de lidar com as três versões coexistindo na mesma ingestão.

**Granularidade:** 1 linha por versão de cadastro do parceiro (histórico de mudanças)

| Coluna | Tipo esperado | Nullable | Descrição | Exemplos | Problemas conhecidos |
|---|---|---|---|---|---|
| `partner_id` | VARCHAR | NÃO | Identificador do parceiro | `PRT0001` | ⚠️ ~8 registros duplicados |
| `schema_version` | VARCHAR | NÃO | Versão do schema da API | `v1`, `v2`, `v3` | ⚠️ **Schema drift:** campos `sla_hours`, `avg_rating`, `api_endpoint`, `webhook_enabled` só existem em versões mais recentes |
| `name` | VARCHAR | NÃO | Nome do parceiro | `Partner PRT0001` | — |
| `country_code` | VARCHAR | SIM (4%) | País de operação | `BR`, `AR` | — |
| `status` | VARCHAR | NÃO | Status de ativação | `active`, `inactive`, `suspended` | ⚠️ Inconsistência de case: `ACTIVE`, `Active` |
| `tier` | VARCHAR | SIM (aprox. 15%) | Nível comercial | `gold`, `silver`, `bronze` | — |
| `commission_rate` | FLOAT | NÃO | Taxa de comissão | `0.1500` | — |
| `created_at` | TIMESTAMP | NÃO | Data de criação do registro | `2019-05-10 09:00:00` | — |
| `updated_at` | TIMESTAMP | SIM (10%) | Última atualização | `2024-12-01 14:22:00` | — |
| `sla_hours` | INTEGER | SIM | SLA de atendimento em horas (v2+) | `24`, `48` | Vazio em registros v1 |
| `avg_rating` | FLOAT | SIM | Avaliação média do parceiro (v2+) | `4.3`, `3.8` | Vazio em registros v1 |
| `api_endpoint` | VARCHAR | SIM | URL do webhook do parceiro (v3) | `https://api.partner...` | Vazio em v1 e v2 |
| `webhook_enabled` | BOOLEAN | SIM | Webhook ativo? (v3) | `True`, `False` | ⚠️ Inconsistência: `true`/`false` (string) coexiste com `True`/`False` (boolean) |

**Regras de negócio:**
- O pipeline deve consolidar as 3 versões em um schema único (schema evolution handling)
- Campos ausentes em versões anteriores devem receber valores padrão definidos no contrato
- `partner_id` + `schema_version` + `updated_at` compõem a chave de deduplicação histórica

---

## raw_payment_stream

**Descrição:** Eventos de pagamento simulando ingestão via Kafka/Kinesis (streaming). Cada linha representa um evento publicado num tópico de pagamentos, com metadados de offset e partição. Contém um pico de volume simulando uma campanha (Black Friday) e eventos fora de ordem típicos de consumo de stream.

**Granularidade:** 1 linha por evento de pagamento

| Coluna | Tipo esperado | Nullable | Descrição | Exemplos | Problemas conhecidos |
|---|---|---|---|---|---|
| `event_id` | VARCHAR (UUID) | NÃO | Identificador único do evento | `c3d4e5f6-...` | ⚠️ ~240 duplicatas (at-least-once delivery semantics) |
| `event_type` | VARCHAR | NÃO | Tipo de evento de pagamento | `payment_approved`, `payment_refused`, `chargeback` | — |
| `transaction_id` | VARCHAR | SIM (4%) | Transação associada | `TXN00012345` | Deve existir em `raw_transactions` quando preenchido |
| `user_id` | VARCHAR (UUID) | SIM (8%) | Usuário da transação | `d1e2f3a4-...` | — |
| `partner_id` | VARCHAR | NÃO | Parceiro da transação | `PRT0010` | — |
| `amount` | FLOAT | NÃO | Valor do evento | `899.50` | — |
| `currency` | VARCHAR | NÃO | Moeda | `BRL`, `USD` | — |
| `status` | VARCHAR | NÃO | Status do evento | `approved`, `refused`, `refunded` | ⚠️ Inconsistência de case: `APPROVED`, `Approved` |
| `gateway` | VARCHAR | SIM (5%) | Gateway processador | `adyen`, `cielo` | — |
| `event_ts` | TIMESTAMP | NÃO | Timestamp do evento no produtor | `2024-11-29 09:15:43` | ⚠️ Eventos fora de ordem (out-of-order) esperados em ~1.5% dos registros |
| `kafka_offset` | INTEGER | NÃO | Offset do evento no tópico Kafka | `0`, `1`, `29999` | Deve ser monotonicamente crescente por partição |
| `kafka_partition` | INTEGER | NÃO | Partição Kafka (0–7) | `0` a `7` | — |
| `processing_lag_ms` | INTEGER | SIM (6%) | Atraso de processamento em ms | `120`, `5800` | — |
| `is_spike` | BOOLEAN | NÃO | Evento originado durante pico de volume | `True`, `False` | ⚠️ ~18% dos eventos vêm de um único período de 16h (Black Friday simulado) |

**Regras de negócio:**
- Idempotência é obrigatória: `event_id` deve ser usado como chave de deduplicação
- Eventos devem ser processados em janelas de tempo (tumbling ou sliding window) para agregações de stream
- Eventos fora de ordem devem ser tratados com watermark configurável
- `kafka_offset` deve ser monotonicamente crescente por `kafka_partition`

---

## pipeline_runs

**Descrição:** Log histórico de execuções dos pipelines orquestrados pelo Airflow. Contém métricas de performance, status de execução, uso de recursos e rastreabilidade. É a fonte de dados para os dashboards de observabilidade e alertas de SLA.

**Granularidade:** 1 linha por execução de pipeline

| Coluna | Tipo esperado | Nullable | Descrição | Exemplos | Problemas conhecidos |
|---|---|---|---|---|---|
| `run_id` | VARCHAR | NÃO | Identificador único da execução | `RUN0000001` | — |
| `pipeline_name` | VARCHAR | NÃO | Nome do pipeline | `ingest_events`, `transform_funnel` | — |
| `dag_id` | VARCHAR | NÃO | ID da DAG no Airflow | `dag_ingest_events` | — |
| `run_type` | VARCHAR | NÃO | Tipo de execução | `scheduled`, `manual`, `backfill` | — |
| `started_at` | TIMESTAMP | NÃO | Timestamp de início | `2025-01-15 03:00:00` | — |
| `ended_at` | TIMESTAMP | SIM (10%) | Timestamp de fim | `2025-01-15 03:08:45` | Nulo = execução ainda em andamento ou falha abrupta |
| `duration_sec` | INTEGER | NÃO | Duração total em segundos | `525` | ⚠️ ~0.5% com duração > 2h (outlier — possível deadlock ou travamento) |
| `status` | VARCHAR | NÃO | Status final | `success`, `failed`, `timeout`, `skipped` | ~12% de taxa de falha nos dados |
| `rows_read` | INTEGER | SIM (var.) | Total de linhas lidas na execução | `125000` | Nulo em execuções falhas |
| `rows_written` | INTEGER | SIM (var.) | Total de linhas escritas com sucesso | `124300` | Nulo em execuções falhas |
| `rows_failed` | INTEGER | SIM (60%) | Linhas que falharam na validação/escrita | `0`, `350` | — |
| `error_message` | VARCHAR | SIM (88%) | Mensagem de erro quando falhou | `timeout`, `schema_mismatch` | Nulo = execução bem-sucedida |
| `retry_attempt` | INTEGER | NÃO | Número da tentativa (0 = primeira) | `0`, `1`, `2` | — |
| `executor` | VARCHAR | NÃO | Executor Airflow utilizado | `CeleryExecutor`, `KubernetesExecutor` | — |
| `memory_mb` | INTEGER | SIM (6%) | Memória consumida em MB | `2048`, `8192` | — |
| `cpu_cores` | INTEGER | SIM (6%) | CPUs utilizadas | `1`, `2`, `4`, `8` | — |
| `s3_bytes_written` | INTEGER | SIM (15%) | Bytes escritos no S3 | `104857600` | Nulo em execuções sem escrita em S3 |
| `triggered_by` | VARCHAR | SIM (5%) | Quem disparou a execução | `scheduler`, `user_marcos`, `ci_pipeline` | — |

**Regras de negócio:**
- Execuções com `status = failed` e `retry_attempt < 3` devem ser elegíveis para re-enfileiramento automático
- SLA de sucesso esperado: >= 95% das execuções `scheduled` devem completar em `success`
- `duration_sec` > 7200 deve disparar alerta de execução longa
- Execuções de `backfill` não devem ser consideradas no cálculo de taxa de sucesso do scheduler

---

## Problemas de Qualidade — Gabarito Interno

| # | Tabela | Tipo | Descrição | Impacto |
|---|---|---|---|---|
| 1 | `raw_events` | Duplicata | ~600 eventos duplicados (at-least-once delivery) | Contagem inflada de eventos e sessões |
| 2 | `raw_events` | Bot / fraude | 1 session com 120 eventos em ~4 min | Distorção em métricas de engajamento |
| 3 | `raw_events` | Late-arriving | ~2% com ingest_date até 10 dias após event_ts | Particionamento incorreto quebra queries incrementais |
| 4 | `raw_events` | Case inconsistente | `device`: desktop/DESKTOP/Mobile | Segmentação errada por device |
| 5 | `raw_events` | Case inconsistente | `country`: BR/br/Br | Agregação errada por país |
| 6 | `raw_events` | JSON variável | `metadata_json` sem schema fixo | Parsing falha sem tratamento de campos opcionais |
| 7 | `raw_transactions` | Duplicata | ~125 registros duplicados | Dupla contagem de receita |
| 8 | `raw_transactions` | Late-arriving | ~3% com atraso de 48–168h na ingestão | Dados faltantes em partições passadas |
| 9 | `raw_transactions` | Outlier de valor | ~0.7% com amount > R$20.000 | Distorção em KPIs financeiros |
| 10 | `raw_transactions` | Outlier de valor | ~0.5% com amount negativo | Erro em cálculo de receita |
| 11 | `raw_transactions` | Case inconsistente | `status`: confirmed/CONFIRMED/Confirmed | Filtros de status quebram |
| 12 | `raw_transactions` | Schema drift | ~1% de `notes` com JSON aninhado (versão legada) | Parser falha sem tratamento de schema evolution |
| 13 | `raw_partner_catalog` | Schema drift | 3 versões de schema coexistindo | Pipeline falha sem evolução de schema |
| 14 | `raw_partner_catalog` | Duplicata | ~8 registros duplicados | Joins inflados |
| 15 | `raw_partner_catalog` | Tipo inconsistente | `webhook_enabled`: True/False vs "true"/"false" | Cast erro sem normalização |
| 16 | `raw_payment_stream` | Duplicata | ~240 duplicatas (at-least-once) | Métricas financeiras dobradas |
| 17 | `raw_payment_stream` | Out-of-order | ~1.5% de eventos fora de ordem | Janelas de tempo incorretas sem watermark |
| 18 | `raw_payment_stream` | Pico de volume | ~18% dos eventos em 16h (Black Friday) | Throttling e backpressure se não tratado |
| 19 | `raw_payment_stream` | Case inconsistente | `status`: approved/APPROVED/Approved | Agrupamento errado de status |
| 20 | `pipeline_runs` | Outlier temporal | ~0.5% de execuções com duração > 2h | Falso negativo em alertas de SLA |

---

## Glossário de Engenharia de Dados

| Termo | Definição no contexto Rentcars |
|---|---|
| **Late-arriving data** | Evento cujo `event_ts` é anterior à janela atual, mas chegou após o processamento ter sido finalizado |
| **At-least-once delivery** | Garantia de entrega do Kafka onde duplicatas são possíveis e devem ser tratadas com idempotência |
| **Schema drift / evolution** | Mudança no schema de uma fonte de dados ao longo do tempo (campos adicionados, removidos ou renomeados) |
| **Out-of-order events** | Eventos que chegam ao consumer com timestamp anterior ao último evento processado |
| **Watermark** | Mecanismo para definir até quando o sistema aceita eventos atrasados numa janela de tempo |
| **Backpressure** | Situação onde o produtor gera dados mais rápido do que o consumer consegue processar |
| **Idempotência** | Propriedade de um pipeline onde reprocessar os mesmos dados múltiplas vezes produz o mesmo resultado final |
| **RPO** | Recovery Point Objective — máximo de dados que podem ser perdidos numa falha (em tempo) |
| **RTO** | Recovery Time Objective — tempo máximo para restabelecer o serviço após uma falha |
| **Partition pruning** | Otimização de query que ignora partições irrelevantes com base no predicado de filtro |
| **Compaction** | Processo de consolidar pequenos arquivos em arquivos maiores para otimizar leitura (ex: Iceberg) |
| **Data Lakehouse** | Arquitetura que combina a flexibilidade do Data Lake com as garantias ACID do Data Warehouse |

---

*Dúvidas sobre os dados? Documente suas suposições no README.md — isso faz parte da avaliação.*
