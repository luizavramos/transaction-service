package br.com.motorfraude.transaction.application.gateway;

import br.com.motorfraude.transaction.application.usecase.SkipReason;

public interface BackfillMetrics {

    void published();

    void skipped(SkipReason reason);

    void malformed();
}
