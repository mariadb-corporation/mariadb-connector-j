#!/usr/bin/env python3
"""Checks a CycloneDX 1.6 SBOM for the CISA 2026 minimum elements (CONJ-1353 / CONC-856).

Schema conformance (including the SPDX id enumeration) is left to `cyclonedx validate`; this
script checks what the schema leaves optional: SBOM author/supplier, generator tool, timestamp,
lifecycle, random serial number, and per component a name, version, supplier, hash, valid purl,
SPDX license and a resolvable dependency graph. Exits non-zero on the first set of failures.
"""
import json
import re
import sys

# https://github.com/package-url/purl-spec: pkg:type/namespace?/name@version?qualifiers#subpath
PURL = re.compile(
    r"^pkg:[a-z][a-z0-9.+-]*/(?:[^/@?#]+/)*[^/@?#]+(?:@[^?#]+)?(?:\?[^#]+)?(?:#.+)?$"
)
UUID4 = re.compile(r"^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")


def check(bom):
    errors = []

    def need(cond, msg):
        if not cond:
            errors.append(msg)

    need(bom.get("specVersion") == "1.6", "specVersion must be 1.6")
    need(UUID4.match(bom.get("serialNumber", "")), "serialNumber must be a random (v4) urn:uuid")

    meta = bom.get("metadata", {})
    need(meta.get("timestamp"), "metadata.timestamp missing")
    need("build" in [lc.get("phase") for lc in meta.get("lifecycles", [])], "metadata.lifecycles must contain build")
    tools = meta.get("tools", {}).get("components", [])
    need(tools and all(t.get("name") and t.get("version") for t in tools), "metadata.tools must name the generator and its version")
    need(meta.get("authors"), "metadata.authors missing")
    need(meta.get("supplier", {}).get("name"), "metadata.supplier missing")
    need(meta.get("manufacturer", {}).get("name"), "metadata.manufacturer missing")
    need("package_name" in [p.get("name") for p in meta.get("properties", [])], "metadata.properties must contain package_name")

    root = meta.get("component", {})
    components = [root] + bom.get("components", [])
    refs = set()
    for c in components:
        label = c.get("purl") or c.get("name") or "<unnamed>"
        need(c.get("name"), f"{label}: name missing")
        need(c.get("version"), f"{label}: version missing")
        need(c.get("supplier", {}).get("name"), f"{label}: supplier missing (add it to enrich.jq suppliers)")
        need(c.get("hashes") or c is not root and c.get("pedigree"), f"{label}: hashes missing")
        purl = c.get("purl", "")
        need(PURL.match(purl), f"{label}: invalid purl '{purl}'")
        if purl and c.get("version"):
            need(f"@{c['version']}" in purl, f"{label}: purl does not carry version {c['version']}")
        licenses = c.get("licenses", [])
        need(licenses, f"{label}: license missing")
        for lic in licenses:
            need(lic.get("expression") or lic.get("license", {}).get("id"), f"{label}: license must be an SPDX id or expression, got {lic}")
        ref = c.get("bom-ref")
        need(ref and ref not in refs, f"{label}: missing or duplicate bom-ref")
        refs.add(ref)

    deps = bom.get("dependencies", [])
    need(any(d.get("ref") == root.get("bom-ref") for d in deps), "root component has no dependency entry")
    for d in deps:
        for r in [d.get("ref")] + d.get("dependsOn", []):
            need(r in refs, f"dependency graph references unknown bom-ref {r}")
    return errors


def main():
    if len(sys.argv) != 2:
        sys.exit(f"usage: {sys.argv[0]} <sbom.cdx.json>")
    with open(sys.argv[1], encoding="utf-8") as f:
        bom = json.load(f)
    errors = check(bom)
    for e in errors:
        print(f"ERROR: {e}", file=sys.stderr)
    if errors:
        sys.exit(f"{sys.argv[1]}: {len(errors)} SBOM check(s) failed")
    print(f"{sys.argv[1]}: {len(bom.get('components', []))} components, all checks passed")


if __name__ == "__main__":
    main()
