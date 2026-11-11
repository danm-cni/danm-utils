package v43to44

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"time"

	corev1 "k8s.io/api/core/v1"
)

const (
	CaCertKey           = "ca.crt"
	shimCertValidity    = 10 * 365 * 24 * time.Hour
	shimCertRenewBefore = 365 * 24 * time.Hour
)

type shimCertificates struct {
	ca   []byte
	cert []byte
	key  []byte
}

func generateShimCertificates(dnsNames []string, now time.Time) (shimCertificates, error) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return shimCertificates{}, fmt.Errorf("cannot generate the shim CA key: %w", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          serialNumber(),
		Subject:               pkix.Name{CommonName: ShimName + "-ca"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(shimCertValidity),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDer, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return shimCertificates{}, fmt.Errorf("cannot create the shim CA certificate: %w", err)
	}
	caCert, err := x509.ParseCertificate(caDer)
	if err != nil {
		return shimCertificates{}, err
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return shimCertificates{}, fmt.Errorf("cannot generate the shim serving key: %w", err)
	}
	template := &x509.Certificate{
		SerialNumber: serialNumber(),
		Subject:      pkix.Name{CommonName: dnsNames[0]},
		DNSNames:     dnsNames,
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(shimCertValidity),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, caCert, &key.PublicKey, caKey)
	if err != nil {
		return shimCertificates{}, fmt.Errorf("cannot create the shim serving certificate: %w", err)
	}
	keyDer, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return shimCertificates{}, err
	}
	return shimCertificates{
		ca:   pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDer}),
		cert: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		key:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDer}),
	}, nil
}

func serialNumber() *big.Int {
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	if err != nil {
		return big.NewInt(time.Now().UnixNano())
	}
	return serial
}

func shimCertificatesUsable(secret *corev1.Secret, dnsName string, now time.Time) bool {
	if secret == nil || len(secret.Data[corev1.TLSPrivateKeyKey]) == 0 {
		return false
	}
	caBlock, _ := pem.Decode(secret.Data[CaCertKey])
	certBlock, _ := pem.Decode(secret.Data[corev1.TLSCertKey])
	if caBlock == nil || certBlock == nil {
		return false
	}
	caCert, err := x509.ParseCertificate(caBlock.Bytes)
	if err != nil {
		return false
	}
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil || now.Add(shimCertRenewBefore).After(cert.NotAfter) || now.Add(shimCertRenewBefore).After(caCert.NotAfter) {
		return false
	}
	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	_, err = cert.Verify(x509.VerifyOptions{DNSName: dnsName, Roots: pool, CurrentTime: now, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}})
	return err == nil
}
