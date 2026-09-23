/*
 * Rewrites target/bom.json (cyclonedx-maven-plugin output) in place with what the plugin cannot
 * express (CONJ-1353 / CONC-856, CISA 2026 SBOM minimum elements). Runs from the gmavenplus
 * execution in pom.xml right after makeBom, so the file attached for Maven Central and the file
 * published on GitHub releases are the same enriched document.
 *   - MariaDB plc as SBOM author, supplier and manufacturer
 *   - random serial number (the plugin derives a stable UUID from the GAV, so two builds of the
 *     same version would share it, defeating per-document uniqueness)
 *   - SHA-256 of the built jar and the package_name property on the root component
 *   - the ed25519 ref10 code vendored under org.mariadb.jdbc.plugin.authentication.standard.ed25519
 *   - supplier for dependencies whose POM has no <organization>, and SPDX ids where the POM only
 *     provides a license name
 */
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import java.security.MessageDigest

// Suppliers for dependency groups that publish no <organization> in their POM. Anything not
// listed here and without a publisher fails check-sbom.py, so this map must be extended when a
// new dependency is added.
def suppliers = [
  'software.amazon.awssdk'        : 'Amazon Web Services, Inc.',
  'software.amazon.eventstream'   : 'Amazon Web Services, Inc.',
  'net.java.dev.jna'              : 'Java Native Access project',
  'com.github.ben-manes.caffeine' : 'Ben Manes',
  'org.jspecify'                  : 'JSpecify',
  'org.checkerframework'          : 'Checker Framework developers',
  'org.reactivestreams'           : 'Reactive Streams Special Interest Group',
]

// Licenses declared by name only in the dependency POM, mapped to their SPDX identifier.
def licenseIds = [:]

def mariadb = { -> [name: 'MariaDB plc', url: ['https://mariadb.com']] }

def ed25519 = [
  type              : 'library',
  'bom-ref'         : 'pkg:maven/net.i2p.crypto/eddsa@0.3.0',
  supplier          : [name: 'str4d', url: ['https://github.com/str4d/ed25519-java']],
  name              : 'ed25519-java',
  version           : '0.3.0',
  description       : 'ref10 Ed25519 implementation (SUPERCOP), bundled as org.mariadb.jdbc.plugin.authentication.standard.ed25519 for client_ed25519 and parsec authentication',
  scope             : 'required',
  licenses          : [[license: [id: 'CC0-1.0']]],
  purl              : 'pkg:maven/net.i2p.crypto/eddsa@0.3.0',
  pedigree          : [notes: 'Sources copied into the connector and modified: not registered as a JCA provider, unused classes removed.'],
  externalReferences: [[type: 'vcs', url: 'https://github.com/str4d/ed25519-java']],
]

File bomFile = new File(project.build.directory, 'bom.json')
File jar = project.artifact.file
if (jar == null || !jar.isFile()) {
  throw new IllegalStateException("main artifact not built yet, cannot hash it for the SBOM: ${jar}")
}
def sha256 = MessageDigest.getInstance('SHA-256').digest(jar.bytes).collect { String.format('%02x', it) }.join()

def bom = new JsonSlurper().parse(bomFile, 'UTF-8')
def root = bom.metadata.component

bom.serialNumber = "urn:uuid:${UUID.randomUUID()}".toString()
bom.metadata.authors = [[name: 'MariaDB plc']]
bom.metadata.supplier = mariadb()
bom.metadata.manufacturer = mariadb()
// explicit key access: on a Map, '.properties' resolves to Groovy's object property view (Groovy 5+)
bom.metadata['properties'] = (bom.metadata['properties'] ?: []) + [[name: 'package_name', value: jar.name]]
root.supplier = mariadb()
root.hashes = [[alg: 'SHA-256', content: sha256]]

bom.components.each { c ->
  if (!c.supplier) {
    if (c.publisher) {
      c.supplier = [name: c.publisher]
    } else if (suppliers[c.group]) {
      c.supplier = [name: suppliers[c.group]]
    }
  }
  (c.licenses ?: []).each { l ->
    def id = l.license?.name ? licenseIds[l.license.name] : null
    if (id) {
      l.license = [id: id] + (l.license.url ? [url: l.license.url] : [:])
    }
  }
}
bom.components << ed25519
bom.dependencies.find { it.ref == root['bom-ref'] }.dependsOn << ed25519['bom-ref']
bom.dependencies << [ref: ed25519['bom-ref'], dependsOn: []]

bomFile.setText(JsonOutput.prettyPrint(JsonOutput.toJson(bom)) + '\n', 'UTF-8')
log.info("Enriched SBOM ${bomFile} (${bom.components.size()} components, jar sha256 ${sha256})")
