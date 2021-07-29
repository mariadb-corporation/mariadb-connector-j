package org.mariadb.jdbc.plugin.authentication.standard.ed25519.math;

public interface ScalarOps {
  /**
   * Reduce the given scalar mod $l$.
   *
   * <p>From the Ed25519 paper:<br>
   * Here we interpret $2b$-bit strings in little-endian form as integers in $\{0, 1,...,
   * 2^{(2b)}-1\}$.
   *
   * @param s the scalar to reduce
   * @return $s \bmod l$
   */
  byte[] reduce(byte[] s);

  /**
   * $r = (a * b + c) \bmod l$
   *
   * @param a a scalar
   * @param b a scalar
   * @param c a scalar
   * @return $(a*b + c) \bmod l$
   */
  byte[] multiplyAndAdd(byte[] a, byte[] b, byte[] c);
}
