/**
 * EdDSA-Java by str4d
 * <p>
 * To the extent possible under law, the person who associated CC0 with
 * EdDSA-Java has waived all copyright and related or neighboring rights
 * to EdDSA-Java.
 * <p>
 * You should have received a copy of the CC0 legalcode along with this
 * work. If not, see <https://creativecommons.org/publicdomain/zero/1.0/>.
 */
package org.mariadb.jdbc.internal.com.send.ed25519.math;

import java.io.Serializable;

/**
 * Note: concrete subclasses must implement hashCode() and equals()
 */
public abstract class FieldElement implements Serializable {
    private static final long serialVersionUID = 1239527465875676L;

    protected final Field f;

    public FieldElement(Field f) {
        if (null == f) {
            throw new IllegalArgumentException("field cannot be null");
        }
        this.f = f;
    }

    /**
     * Encode a FieldElement in its $(b-1)$-bit encoding.
     * @return the $(b-1)$-bit encoding of this FieldElement.
     */
    public byte[] toByteArray() {
        return f.getEncoding().encode(this);
    }

    public abstract boolean isNonZero();

    public boolean isNegative() {
        return f.getEncoding().isNegative(this);
    }

    public abstract org.mariadb.jdbc.internal.com.send.ed25519.math.FieldElement add(org.mariadb.jdbc.internal.com.send.ed25519.math.FieldElement val);

    public org.mariadb.jdbc.internal.com.send.ed25519.math.FieldElement addOne() {
        return add(f.ONE);
    }

    public abstract org.mariadb.jdbc.internal.com.send.ed25519.math.FieldElement subtract(org.mariadb.jdbc.internal.com.send.ed25519.math.FieldElement val);

    public org.mariadb.jdbc.internal.com.send.ed25519.math.FieldElement subtractOne() {
        return subtract(f.ONE);
    }

    public abstract org.mariadb.jdbc.internal.com.send.ed25519.math.FieldElement negate();

    public org.mariadb.jdbc.internal.com.send.ed25519.math.FieldElement divide(org.mariadb.jdbc.internal.com.send.ed25519.math.FieldElement val) {
        return multiply(val.invert());
    }

    public abstract org.mariadb.jdbc.internal.com.send.ed25519.math.FieldElement multiply(org.mariadb.jdbc.internal.com.send.ed25519.math.FieldElement val);

    public abstract org.mariadb.jdbc.internal.com.send.ed25519.math.FieldElement square();

    public abstract org.mariadb.jdbc.internal.com.send.ed25519.math.FieldElement squareAndDouble();

    public abstract org.mariadb.jdbc.internal.com.send.ed25519.math.FieldElement invert();

    public abstract org.mariadb.jdbc.internal.com.send.ed25519.math.FieldElement pow22523();

    public abstract org.mariadb.jdbc.internal.com.send.ed25519.math.FieldElement cmov(org.mariadb.jdbc.internal.com.send.ed25519.math.FieldElement val, final int b);

    // Note: concrete subclasses must implement hashCode() and equals()
}
