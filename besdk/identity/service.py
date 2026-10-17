"""Service identity (protocol P8.5, P8.6): a member's mTLS certificate from the deployment's trust root, re-read from
its files so a rotation applies to new connections; the server requires and checks client certificates, and the
client checks that the server is the target component by the DNS name derived from its SPIFFE ID."""
from __future__ import annotations

from typing import Any, Callable

import grpc
from cryptography import x509
from cryptography.hazmat.primitives import serialization
from cryptography.x509.oid import ExtendedKeyUsageOID


def spiffe(deployment: str, component: str) -> str:
    """The service identity of a component in a deployment."""
    return f"spiffe://{deployment}/component/{component}"


def dns_name(deployment: str, component: str) -> str:
    """The DNS SAN derived from the SPIFFE ID, which clients that can only check a host name check."""
    return component.replace("/", ".") + ".component." + deployment


def _sans(cert: x509.Certificate) -> tuple[list[str], list[str]]:
    try:
        ext = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    except x509.ExtensionNotFound:
        return [], []
    return ext.get_values_for_type(x509.UniformResourceIdentifier), ext.get_values_for_type(x509.DNSName)


def _identity(cert: x509.Certificate) -> str:
    return next((u for u in _sans(cert)[0] if u.startswith("spiffe://")), "")


def _usable_for(cert: x509.Certificate, usage: x509.ObjectIdentifier) -> bool:
    try:
        return usage in cert.extensions.get_extension_for_class(x509.ExtendedKeyUsage).value
    except x509.ExtensionNotFound:
        return True  # no restriction


class ServiceTLS:
    """A member's mTLS identity."""

    def __init__(self, deployment: str, component: str, cert_file: str, key_file: str, ca_file: str,
                 read: Callable[[str], bytes]):
        self.deployment, self.component = deployment, component
        self._files, self._read = (cert_file, key_file, ca_file), read
        self.cert = self.key = self.ca = b""
        self.generation = 0  # grows with every change of the files' contents
        self.reload()

    def reload(self) -> bool:
        """Re-reads the files; True when they changed. The certificate must carry this member's identity."""
        try:
            cert, key, ca = (self._read(f) for f in self._files)
        except OSError as e:
            raise ValueError(f"service identity: {e}") from e
        if (cert, key, ca) == (self.cert, self.key, self.ca):
            return False
        try:
            leaf = x509.load_pem_x509_certificate(cert)
            private = serialization.load_pem_private_key(key, None)
            roots = x509.load_pem_x509_certificates(ca)
        except ValueError as e:
            raise ValueError(f"service identity: {e}") from e
        if private.public_key().public_bytes(serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo) \
                != leaf.public_key().public_bytes(serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo):
            raise ValueError("service certificate: the key does not belong to the certificate")
        if not roots:
            raise ValueError("service trust root: no certificate")
        if (got := _identity(leaf)) != (want := spiffe(self.deployment, self.component)):
            raise ValueError(f"service certificate: it names {got!r}, this member is {want!r}")
        if (dns := dns_name(self.deployment, self.component)) not in _sans(leaf)[1]:
            raise ValueError(f"service certificate: no DNS name {dns!r} (P8.5)")
        self.cert, self.key, self.ca = cert, key, ca
        self.generation += 1
        return True

    def server_credentials(self) -> grpc.ServerCredentials:
        """Requires a client certificate from the deployment's trust root; a rotation applies to new connections."""
        seen = [self.generation]

        def config() -> grpc.ServerCertificateConfiguration:
            return grpc.ssl_server_certificate_configuration([(self.key, self.cert)], root_certificates=self.ca)

        def fetch() -> grpc.ServerCertificateConfiguration | None:
            if self.generation == seen[0]:
                return None  # unchanged
            seen[0] = self.generation
            return config()

        return grpc.dynamic_ssl_server_credentials(config(), fetch, require_client_authentication=True)

    def channel(self, target_component: str) -> tuple[grpc.ChannelCredentials, list[tuple[str, Any]]]:
        """Credentials and options for a channel to a component: this member's certificate, the deployment's trust
        root, and the target's derived DNS name as the only name accepted."""
        creds = grpc.ssl_channel_credentials(root_certificates=self.ca, private_key=self.key, certificate_chain=self.cert)
        return creds, [("grpc.ssl_target_name_override", dns_name(self.deployment, target_component))]

    def caller(self, context: grpc.aio.ServicerContext) -> str:
        """The component a system call came from, as its verified client certificate says; "" when it is not a
        component of this deployment."""
        pem = (context.auth_context() or {}).get("x509_pem_cert") or []
        if not pem:
            return ""
        try:
            cert = x509.load_pem_x509_certificate(pem[0])
        except ValueError:
            return ""
        prefix = spiffe(self.deployment, "")
        ident = _identity(cert)
        if not ident.startswith(prefix) or not _usable_for(cert, ExtendedKeyUsageOID.CLIENT_AUTH):
            return ""
        return ident[len(prefix):]
