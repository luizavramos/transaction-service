# transaction-service

Carga inicial (**backfill**) de 90 dias de histórico de transações, do banco legado para o motor de detecção. No go-live o motor começa sem histórico e regras como "valor 5× acima da média de 30 dias" ficariam cegas; este serviço lê o snapshot do Debezium e publica em `transaction-history-backfill.v1` (chave = `accountId`), sem avaliar regras. Depois da carga ele sai do caminho. Kafka Streams com exactly-once, sem Spring.

```
cdc.bank.transactions ─► só op=r (snapshot) e occurredAt em [cutoverAt − 90 d, cutoverAt) ─► campos permitidos ─► transaction-history-backfill.v1
```

**Estado:** em implementação (spec 006, Fase 1: módulo, regra de arquitetura, conector e seed). Domínio, topologia, status e a prova com o motor vêm nas próximas fases; este README ganha o procedimento de refazer a carga (H3) ao fim.

## Configuração

| Variável | Padrão | Para quê |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | brokers |
| `HTTP_PORT` | `8080` (no compose: `8086`) | saúde, métricas e `/v1/backfill/status` |
| `STATE_DIR` | `/data` | diretório de estado do Kafka Streams |
| `CUTOVER_AT` | sem padrão, **obrigatório** | instante de corte, ISO-8601 em UTC; só entram transações com `occurredAt` antes dele |

## No compose (Desafio)
```bash
sh infra/seed/transactions.sh                      # 1 milhão de transações sintéticas em 120 dias (idempotente)
sh infra/connect/register-connectors.sh transactions-cdc   # o snapshot do Debezium dispara o backfill
```
