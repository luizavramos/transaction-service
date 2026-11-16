# transaction-service

Carga inicial (**backfill**) de 90 dias de histórico de transações, do banco legado para o motor de detecção. No go-live o motor começa sem histórico e regras como "valor 5× acima da média de 30 dias" ficariam cegas; este serviço lê o snapshot do Debezium e publica em `transaction-history-backfill.v1` (chave = `accountId`), sem avaliar regras. Depois da carga ele sai do caminho. Kafka Streams com exactly-once, sem Spring.

```
cdc.bank.transactions ─► só op=r (snapshot) e occurredAt em [cutoverAt − 90 d, cutoverAt) ─► campos permitidos ─► transaction-history-backfill.v1
```

**Estado:** em implementação (spec 006, Fases 1 a 3: módulo, conector, seed, janela de 90 dias, mapeamento do Debezium, topologia e status). A prova com o motor vem nas próximas fases; este README ganha o procedimento de refazer a carga (H3) ao fim.

## Configuração

| Variável | Padrão | Para quê |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | brokers |
| `HTTP_PORT` | `8080` (no compose: `8086`) | saúde (`/actuator/health`) e métricas (`/actuator/prometheus`) |
| `STATUS_PORT` | `8096` | `GET /v1/backfill/status` (porta própria: o servidor de saúde é compartilhado e não aceita rotas novas) |
| `BACKFILL_RATE_PER_SECOND` | `20000` | limite de publicação; o consumo do CDC anda nessa velocidade |
| `BACKFILL_COMPLETION_QUIET_SECONDS` | `10` | silêncio sem eventos, depois do `snapshot=last`, para declarar `COMPLETED` (o tópico tem 3 partições) |
| `STATE_DIR` | `/data` | diretório de estado do Kafka Streams |
| `CUTOVER_AT` | sem padrão, **obrigatório** | instante de corte, ISO-8601 em UTC; só entram transações com `occurredAt` antes dele |

## No compose (Desafio)
```bash
sh infra/seed/transactions.sh                      # 1 milhão de transações sintéticas em 120 dias (idempotente)
sh infra/connect/register-connectors.sh transactions-cdc   # o snapshot do Debezium dispara o backfill
```
