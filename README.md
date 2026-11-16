# transaction-service

Carga inicial (**backfill**) de 90 dias de histórico de transações, do banco legado para o motor de detecção. No go-live o motor começa sem histórico e regras como "valor 5× acima da média de 30 dias" ficariam cegas; este serviço lê o snapshot do Debezium e publica em `transaction-history-backfill.v1` (chave = `accountId`), sem avaliar regras. Depois da carga ele sai do caminho. Kafka Streams com exactly-once, sem Spring.

```
cdc.bank.transactions ─► só op=r (snapshot) e occurredAt em [cutoverAt − 90 d, cutoverAt) ─► campos permitidos ─► transaction-history-backfill.v1
```

**Estado:** implementado (spec 006, Fases 1 a 4). Provado com Postgres, Kafka e Debezium reais (10 mil transações, nada fora da janela, nada de dado pessoal) e, no `detection-engine`, contra o cálculo em SQL (ver "O que está provado"). Ensaiado no compose com o motor de pé (limitador a 2 mil/s, reset do motor). Não medido: o limite padrão de 20 mil registros/s contra um motor sob tráfego ao vivo e a latência do motor em ambiente representativo.

## Configuração

| Variável | Padrão | Para quê |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | brokers |
| `HTTP_PORT` | `8080` (no compose: `8086`) | saúde (`/actuator/health`) e métricas (`/actuator/prometheus`) |
| `STATUS_PORT` | `8096` | `GET /v1/backfill/status` (porta própria: o servidor de saúde é compartilhado e não aceita rotas novas) |
| `BACKFILL_RATE_PER_SECOND` | `20000` | limite de publicação; o consumo do CDC anda nessa velocidade |
| `BACKFILL_COMPLETION_QUIET_SECONDS` | `10` | silêncio sem eventos, depois do `snapshot=last`, para declarar `COMPLETED` (o tópico tem 3 partições) |
| `STATE_DIR` | `/data` | diretório de estado do Kafka Streams e do arquivo `backfill-status.json` |
| `CUTOVER_AT` | sem padrão, **obrigatório** | instante de corte, ISO-8601 em UTC, com fuso (`2026-11-15T00:00:00Z`). Sem ele, ou inválido, o processo nem sobe |

## O `cutoverAt`

É o instante em que o fluxo ao vivo assume: o `receiver` passa a mandar ao motor as transações com `occurredAt ≥ cutoverAt`, e o backfill publica só as de `[cutoverAt − 90 dias, cutoverAt)`. O início da janela entra (exatamente 90 dias antes) e o `cutoverAt` fica de fora; é isso que impede contar a mesma transação duas vezes (uma pelo backfill, outra pelo ao vivo).

Regras para escolher e mudar o valor:
- Escolha o instante **antes** de ligar o tráfego ao vivo e use o mesmo valor no backfill e na decisão de quando o `receiver` começa. Um valor mais cedo que o real deixa um buraco no histórico; mais tarde, sobrepõe o ao vivo (o motor ainda deduplica por `transactionId` por 24 h, mas depois disso não).
- Trocar o `CUTOVER_AT` muda a janela. O arquivo `backfill-status.json` guarda o corte com que a carga terminou e é ignorado se o valor mudar.

## No compose (Desafio)
```bash
sh infra/seed/transactions.sh                      # 1 milhão de transações sintéticas em 120 dias (idempotente)
sh infra/connect/register-connectors.sh transactions-cdc   # o snapshot do Debezium dispara o backfill
curl localhost:8096/v1/backfill/status             # RUNNING até o fim do snapshot, depois COMPLETED
```

O status mostra o estado, `cutoverAt`, o início da janela, os totais publicados e os descartados por motivo (`out_of_window`, `not_snapshot`, `invalid_record`) e os eventos ilegíveis. `COMPLETED` exige o último evento do snapshot (`snapshot=last`) **e** 10 s sem nenhum evento novo; um retardatário volta o estado para `RUNNING`. Os totais valem desde a partida do processo; ao completar, o resultado é gravado em `STATE_DIR/backfill-status.json` e relido num reinício.

Métricas (porta `HTTP_PORT`): `backfill_records_published_total`, `backfill_records_skipped_total{reason}` e `backfill_events_malformed_total`. O painel "Carga de histórico (spec 006)" do Grafana mostra o total publicado e a velocidade por motivo.

## Refazer a carga (H3)

Quando refazer: o motor foi recriado, o estado dele foi perdido, ou a carga terminou com erro. O que decide o caminho é o `seen-tx` do motor (deduplicação por `transactionId`, retenção de 24 h):

**A. Menos de 24 h desde a carga anterior, motor com o estado intacto.** Basta publicar de novo (passo 1 abaixo). O motor reconhece os `transactionId` e não conta duas vezes (coberto por teste do motor: "registro de backfill repetido não conta duas vezes").

**B. Mais de 24 h, ou o estado do motor foi perdido.** Republicar sem limpar contaria em dobro, porque o `seen-tx` já esqueceu os ids. Então o motor precisa recomeçar do zero e receber tudo de novo:
1. Pare as instâncias do `detection-engine`.
2. Zere a aplicação do motor: `kafka-streams-application-reset` para `application.id=detection-engine` com `--input-topics` dos tópicos de entrada, e apague o diretório de estado de cada instância. Isso apaga os buckets e o `seen-tx`; os buckets do tráfego ao vivo só voltam se o tópico de comandos ainda tiver os dados (retenção de 7 dias).
3. Refaça o snapshot: o Debezium só roda o snapshot de novo para um conector sem offset. Registre o conector com **outro nome e outro slot** (por exemplo `transactions-cdc-2` e `transactions_cdc_slot_2`) em vez de reaproveitar o antigo, e remova o antigo e o slot dele.
4. Zere o `transaction-service`: pare o processo, apague `backfill-status.json` e o estado do Streams (ou use `kafka-streams-application-reset` para `transaction-service`) e suba de novo com o **mesmo** `CUTOVER_AT`.
5. Suba o motor e acompanhe `/v1/backfill/status` até `COMPLETED`.

Limites conhecidos:
- O tópico `transaction-history-backfill.v1` guarda 7 dias. Passado isso, só refazendo o snapshot (passo 3) o motor recebe o histórico de novo.
- O passo 2 de B (reset do motor com o tópico de histórico já com cópias duplicadas) foi ensaiado no compose e bateu com o SQL em 100 contas (ver "O que está provado"); o passo 3 (conector novo com slot novo) foi exercitado à parte. Faça o primeiro ensaio num ambiente que não seja o de produção.
- Se o `transaction-service` morrer no meio de uma transação do Kafka, o aborto pode ficar inacabado (`PrepareAbort`) e travar a partição para quem lê só dados confirmados, como o motor: o atraso daquela partição para de cair. Em desenvolvimento, `kafka-transactions.sh describe-producers` mostra o início da transação aberta e `kafka-transactions.sh abort --start-offset` a fecha, descartando aquelas mensagens. Em produção, refaça a carga pelo H3.

## O que está provado

| O quê | Como |
|---|---|
| Só a janela, nada de dado pessoal, ids iguais aos do SQL (SC-002) | `TransactionServiceIT`: Postgres, Kafka e Debezium reais, 10 mil transações de 120 dias antes a 10 dias depois do corte, com as quatro bordas (90 dias exatos, 1 s antes, 1 s antes do corte, o corte) |
| Status e métricas depois da carga | mesmo teste: `COMPLETED`, `published` e `out_of_window` conferidos contra o SQL |
| `avgAmount30d` de 100 contas igual ao SQL (SC-001) | teste no `detection-engine` que alimenta a topologia real com os registros produzidos por este serviço (fixture gerada pelo `TransactionServiceIT`) |
| Transporte no compose, com Debezium real e 1 milhão de linhas | 750.082 publicadas (as da janela, iguais ao SQL), 249.918 fora, `COMPLETED`; o tópico lido com `read_committed` tem 750.082 mensagens e ids distintos, sem nome nem CPF |
| Motor sob carga, limitador a 2 mil/s, H3-B | compose com 500 mil linhas: `COMPLETED` com 374.976 publicados e 125.024 fora da janela; motor acompanhando com tráfego ao vivo; depois do reset do motor, 100 contas conferidas contra o SQL com 0 diferenças (`infra/backfill-verify.sh` no repositório do compose) |
| Janela, mapeamento, estado e topologia | testes unitários e de `TopologyTestDriver`, com fixtures reais do Debezium 3.6.3 |

## Build e testes

```bash
./mvnw verify                                      # unitários + ArchUnit + o IT com contêineres (precisa de Docker)
./mvnw verify -Dtest=TransactionServiceIT -Dsurefire.failIfNoSpecifiedTests=false \
  -DargLine=-Dfixture.dir=target/fixture           # regrava a fixture do SC-001 (reference-history.jsonl e reference-expected.json)
```

Memória: o IT sobe Postgres, Kafka e Debezium Connect. Num Docker de 4 GB, com a pilha do compose já de pé, o Connect do teste morre com código 137; pare o compose (`docker compose stop`) antes de rodar o `verify`.
