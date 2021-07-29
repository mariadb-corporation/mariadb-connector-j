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
