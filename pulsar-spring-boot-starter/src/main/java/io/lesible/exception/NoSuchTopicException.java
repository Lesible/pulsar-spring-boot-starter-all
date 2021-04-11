package io.lesible.exception;

/**
 * <p> @date: 2021-04-08 10:25</p>
 *
 * @author 何嘉豪
 */
public class NoSuchTopicException extends RuntimeException {

    public NoSuchTopicException() {
    }

    public NoSuchTopicException(String topic) {
        super("no such topic [" + topic + "]");
    }

    public NoSuchTopicException(String message, Throwable cause) {
        super(message, cause);
    }

    public NoSuchTopicException(Throwable cause) {
        super(cause);
    }

    public NoSuchTopicException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
