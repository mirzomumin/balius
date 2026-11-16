// Package tlsx builds the TLS server configuration of balius workers:
// certificate chain, (optionally encrypted) private key, minimum protocol
// version, client certificate verification and revocation lists.
package tlsx

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"

	"github.com/mirzomumin/balius/internal/config"
)

// ServerConfig returns the TLS configuration for cfg.
func ServerConfig(cfg *config.Config) (*tls.Config, error) {
	certPEM, err := os.ReadFile(cfg.SSLCert)
	if err != nil {
		return nil, fmt.Errorf("cannot open certificate file: %w", err)
	}
	keyPEM, err := os.ReadFile(cfg.SSLKey)
	if err != nil {
		return nil, fmt.Errorf("cannot load key: %w", err)
	}
	if cfg.SSLKeyPassword != "" {
		keyPEM, err = decryptKeyPEM(keyPEM, cfg.SSLKeyPassword)
		if err != nil {
			return nil, err
		}
	}
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("invalid certificate or key: %w", err)
	}

	tc := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
	}
	if cfg.SSLProtocolMin == "tls1.2" {
		tc.MinVersion = tls.VersionTLS12
	}
	switch cfg.HTTP {
	case config.HTTP1:
		tc.NextProtos = []string{"http/1.1"}
	case config.HTTP2:
		tc.NextProtos = []string{"h2"}
	default:
		tc.NextProtos = []string{"h2", "http/1.1"}
	}

	if cfg.SSLCA != "" {
		caPEM, err := os.ReadFile(cfg.SSLCA)
		if err != nil {
			return nil, fmt.Errorf("cannot open CA certificate file: %w", err)
		}
		cas, err := parseCerts(caPEM)
		if err != nil {
			return nil, fmt.Errorf("invalid CA certificate file: %w", err)
		}
		pool := x509.NewCertPool()
		for _, ca := range cas {
			pool.AddCert(ca)
		}
		tc.ClientCAs = pool
		tc.ClientAuth = tls.VerifyClientCertIfGiven
		if cfg.SSLClientVerify {
			tc.ClientAuth = tls.RequireAndVerifyClientCert
		}
		if len(cfg.SSLCRL) > 0 {
			revoked, err := loadCRLs(cfg.SSLCRL, cas)
			if err != nil {
				return nil, err
			}
			tc.VerifyPeerCertificate = revoked.verify
		}
	}
	return tc, nil
}

func parseCerts(data []byte) ([]*x509.Certificate, error) {
	var certs []*x509.Certificate
	for {
		var block *pem.Block
		block, data = pem.Decode(data)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, err
		}
		certs = append(certs, cert)
	}
	if len(certs) == 0 {
		return nil, errors.New("no certificates found")
	}
	return certs, nil
}

// revocations indexes revoked serial numbers by (raw) issuer name.
type revocations map[string]map[string]struct{}

func loadCRLs(files []string, cas []*x509.Certificate) (revocations, error) {
	out := revocations{}
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			return nil, fmt.Errorf("cannot read CRL file: %w", err)
		}
		var ders [][]byte
		rest := data
		for {
			var block *pem.Block
			block, rest = pem.Decode(rest)
			if block == nil {
				break
			}
			if block.Type == "X509 CRL" {
				ders = append(ders, block.Bytes)
			}
		}
		if len(ders) == 0 {
			ders = append(ders, data) // DER encoded
		}
		for _, der := range ders {
			crl, err := x509.ParseRevocationList(der)
			if err != nil {
				return nil, fmt.Errorf("invalid CRL file %s: %w", file, err)
			}
			for _, ca := range cas {
				if !bytes.Equal(ca.RawSubject, crl.RawIssuer) {
					continue
				}
				if err := crl.CheckSignatureFrom(ca); err != nil {
					return nil, fmt.Errorf("CRL %s is not signed by its issuer: %w", file, err)
				}
			}
			serials := out[string(crl.RawIssuer)]
			if serials == nil {
				serials = map[string]struct{}{}
				out[string(crl.RawIssuer)] = serials
			}
			for _, entry := range crl.RevokedCertificateEntries {
				serials[entry.SerialNumber.String()] = struct{}{}
			}
		}
	}
	return out, nil
}

func (r revocations) verify(_ [][]byte, chains [][]*x509.Certificate) error {
	for _, chain := range chains {
		for _, cert := range chain {
			if serials, ok := r[string(cert.RawIssuer)]; ok {
				if _, revoked := serials[cert.SerialNumber.String()]; revoked {
					return errors.New("tls: client certificate has been revoked")
				}
			}
		}
	}
	return nil
}
