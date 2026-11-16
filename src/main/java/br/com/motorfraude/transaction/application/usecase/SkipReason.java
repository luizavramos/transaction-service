package br.com.motorfraude.transaction.application.usecase;

public enum SkipReason {

    /** Não é leitura do snapshot (c, u ou d): o histórico só vem da carga inicial. */
    NOT_SNAPSHOT("not_snapshot"),

    /** Leitura do snapshot fora de [cutoverAt − 90 dias, cutoverAt). */
    OUT_OF_WINDOW("out_of_window");

    private final String label;

    SkipReason(String label) {
        this.label = label;
    }

    /** Valor da tag {@code reason} da métrica backfill_records_skipped_total. */
    public String label() {
        return label;
    }
}
