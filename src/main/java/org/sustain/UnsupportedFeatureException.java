package org.sustain;

public class UnsupportedFeatureException extends Exception {
    public UnsupportedFeatureException() {
    }

    public UnsupportedFeatureException(String message) {
        super(message);
    }
}
