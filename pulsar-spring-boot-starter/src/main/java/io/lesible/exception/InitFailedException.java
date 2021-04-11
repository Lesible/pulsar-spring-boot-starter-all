package io.lesible.exception;

/**
 * <p> @date: 2021-04-08 10:27</p>
 *
 * @author 何嘉豪
 */
public class InitFailedException extends RuntimeException{

    public InitFailedException() {
    }

    public InitFailedException(String message) {
        super(message);
    }

    public InitFailedException(String message, Throwable cause) {
        super(message, cause);
    }

    public InitFailedException(Throwable cause) {
        super(cause);
    }

    public InitFailedException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
