/**
 * EdDSA-Java by str4d
 *
 * <p>To the extent possible under law, the person who associated CC0 with EdDSA-Java has waived all
 * copyright and related or neighboring rights to EdDSA-Java.
 *
 * <p>You should have received a copy of the CC0 legalcode along with this work. If not, see
 * <https://creativecommons.org/publicdomain/zero/1.0/>.
 */
package com.singlestore.jdbc.plugin.authentication.standard.ed25519.spec;

import java.security.spec.AlgorithmParameterSpec;

/**
 * Implementation of AlgorithmParameterSpec that holds the name of a named EdDSA curve
 * specification.
 *
 * @author str4d
 */
public class EdDSAGenParameterSpec implements AlgorithmParameterSpec {
  private final String name;

  public EdDSAGenParameterSpec(String stdName) {
    name = stdName;
  }

  public String getName() {
    return name;
  }
}
