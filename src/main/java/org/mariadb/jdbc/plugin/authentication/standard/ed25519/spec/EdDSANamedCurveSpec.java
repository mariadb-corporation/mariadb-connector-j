/**
 * EdDSA-Java by str4d
 *
 * <p>To the extent possible under law, the person who associated CC0 with EdDSA-Java has waived all
 * copyright and related or neighboring rights to EdDSA-Java.
 *
 * <p>You should have received a copy of the CC0 legalcode along with this work. If not, see
 * <https://creativecommons.org/publicdomain/zero/1.0/>.
 */
package org.mariadb.jdbc.plugin.authentication.standard.ed25519.spec;

import org.mariadb.jdbc.plugin.authentication.standard.ed25519.math.Curve;
import org.mariadb.jdbc.plugin.authentication.standard.ed25519.math.GroupElement;
import org.mariadb.jdbc.plugin.authentication.standard.ed25519.math.ScalarOps;

/**
 * EdDSA Curve specification that can also be referred to by name.
 *
 * @author str4d
 */
public class EdDSANamedCurveSpec extends EdDSAParameterSpec {
  private static final long serialVersionUID = -4080022735829727073L;
  private final String name;

  public EdDSANamedCurveSpec(
      String name, Curve curve, String hashAlgo, ScalarOps sc, GroupElement B) {
    super(curve, hashAlgo, sc, B);
    this.name = name;
  }

  public String getName() {
    return name;
  }
}
