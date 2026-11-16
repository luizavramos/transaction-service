package br.com.motorfraude.transaction.application.gateway;

import br.com.motorfraude.transaction.application.usecase.SkipReason;

public interface BackfillMetrics {

    void published();

    void skipped(SkipReason reason);

    /** Evento do CDC que não deu para ler (JSON inválido ou campo obrigatório ausente). */
    void malformed();
}
