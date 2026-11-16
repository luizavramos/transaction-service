package br.com.motorfraude.transaction.adapters.controller.cdc;

public class CdcFormatException extends RuntimeException {

    public CdcFormatException(String message) {
        super(message);
    }

    public CdcFormatException(String message, Throwable cause) {
        super(message, cause);
    }
}
