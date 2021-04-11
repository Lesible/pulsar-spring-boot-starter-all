package io.lesible.exception;

/**
 * <p> @date: 2021-04-09 18:16</p>
 *
 * @author 何嘉豪
 */
public class EmptyTopicException extends RuntimeException {

    public EmptyTopicException() {
        super();
    }

    public EmptyTopicException(String consumerName) {
        super("the consumer [" + consumerName + "] does not have a topic");
    }

    public EmptyTopicException(String message, Throwable cause) {
        super(message, cause);
    }

    public EmptyTopicException(Throwable cause) {
        super(cause);
    }

    protected EmptyTopicException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
