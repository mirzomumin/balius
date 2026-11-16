package tlsx

import (
	"crypto/tls"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/mirzomumin/tulpar/internal/config"
)

func fixture(name string) string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..", "tests", "fixtures", "tls", name)
}

func TestServerConfig(t *testing.T) {
	cfg := &config.Config{SSLCert: fixture("cert.pem"), SSLKey: fixture("key.pem"), SSLProtocolMin: "tls1.2"}
	tc, err := ServerConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if tc.MinVersion != tls.VersionTLS12 || len(tc.Certificates) != 1 || tc.NextProtos[0] != "h2" {
		t.Errorf("unexpected config: min=%x protos=%v", tc.MinVersion, tc.NextProtos)
	}
}

func TestEncryptedKey(t *testing.T) {
	cfg := &config.Config{
		SSLCert: fixture("pcert.pem"), SSLKey: fixture("pkey.pem"), SSLKeyPassword: "foobar",
		SSLProtocolMin: "tls1.3", HTTP: config.HTTP1,
	}
	tc, err := ServerConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if tc.MinVersion != tls.VersionTLS13 || tc.NextProtos[0] != "http/1.1" {
		t.Errorf("unexpected config: min=%x protos=%v", tc.MinVersion, tc.NextProtos)
	}
	cfg.SSLKeyPassword = "wrong"
	if _, err := ServerConfig(cfg); err == nil {
		t.Error("a wrong password must be rejected")
	}
}

func TestClientVerification(t *testing.T) {
	cfg := &config.Config{
		SSLCert: fixture("cert.pem"), SSLKey: fixture("key.pem"), SSLProtocolMin: "tls1.3",
		SSLCA: fixture("cert.pem"),
	}
	tc, err := ServerConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if tc.ClientAuth != tls.VerifyClientCertIfGiven || tc.ClientCAs == nil {
		t.Errorf("client auth %v", tc.ClientAuth)
	}
	cfg.SSLClientVerify = true
	tc, _ = ServerConfig(cfg)
	if tc.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Errorf("client auth %v", tc.ClientAuth)
	}
}
