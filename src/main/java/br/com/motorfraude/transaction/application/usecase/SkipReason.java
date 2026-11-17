package br.com.motorfraude.transaction.application.usecase;

public enum SkipReason {

    NOT_SNAPSHOT("not_snapshot"),

    OUT_OF_WINDOW("out_of_window"),

    INVALID_RECORD("invalid_record");

    private final String label;

    SkipReason(String label) {
        this.label = label;
    }

    public String label() {
        return label;
    }
}
