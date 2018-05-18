package io.daydev.common.functional;

import static java.util.Objects.requireNonNull;
import java.io.Serializable;
import java.util.NoSuchElementException;

public abstract class Either<L, R> implements Serializable {
    private static final long serialVersionUID = -1L;

    public static <L, R> Either<L, R> left(final L left) {
        requireNonNull(left);
        return new Left<>(left);
    }

    public static <L, R> Either<L, R> right(final R right) {
        requireNonNull(right);
        return new Right<>(right);
    }

    Either() {}

    public abstract boolean isLeft();

    public abstract boolean isRight();

    public L getLeft() {
        throw new NoSuchElementException();
    }

    public R getRight() {
        throw new NoSuchElementException();
    }


    static final class Left<L, R> extends Either<L, R> {
        private static final long serialVersionUID = 1;

        private final L value;

        Left(final L value) {
            requireNonNull(value);
            this.value = value;
        }

        @Override public final L getLeft() {
            return value;
        }

        @Override public boolean isLeft() {
            return true;
        }

        @Override public boolean isRight() {
            return false;
        }

        @Override public boolean equals(final Object o) {
            return this == o || (o != null) && o instanceof Left<?, ?> && value.equals(((Left<?, ?>) o).value);
        }

        @Override public int hashCode() {
            return value.hashCode();
        }

        @Override public String toString() {
            return "Either.Left(" + value.toString() + ")";
        }
    }

    static final class Right<L, R> extends Either<L, R> {
        private static final long serialVersionUID = 2;

        private final R value;

        Right(final R value) {
            requireNonNull(value);
            this.value = value;
        }

        @Override public final R getRight() {
            return value;
        }

        @Override public boolean isRight() {
            return true;
        }

        @Override public boolean isLeft() {
            return false;
        }

        @Override public boolean equals(final Object o) {
            return this == o || (o != null) && o instanceof Right<?, ?> && value.equals(((Right<?, ?>) o).value);
        }

        @Override public int hashCode() {
            return value.hashCode();
        }

        @Override public String toString() {
            return "Either.Right(" + value.toString() + ")";
        }
    }
}
