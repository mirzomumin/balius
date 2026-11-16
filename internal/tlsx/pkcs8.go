package tlsx

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/des"
	"crypto/pbkdf2"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/pem"
	"errors"
	"fmt"
	"hash"
)

// PKCS#5 v2 (RFC 8018) object identifiers.
var (
	oidPBES2  = asn1.ObjectIdentifier{1, 2, 840, 113549, 1, 5, 13}
	oidPBKDF2 = asn1.ObjectIdentifier{1, 2, 840, 113549, 1, 5, 12}

	oidHMACSHA1   = asn1.ObjectIdentifier{1, 2, 840, 113549, 2, 7}
	oidHMACSHA224 = asn1.ObjectIdentifier{1, 2, 840, 113549, 2, 8}
	oidHMACSHA256 = asn1.ObjectIdentifier{1, 2, 840, 113549, 2, 9}
	oidHMACSHA384 = asn1.ObjectIdentifier{1, 2, 840, 113549, 2, 10}
	oidHMACSHA512 = asn1.ObjectIdentifier{1, 2, 840, 113549, 2, 11}

	oidAES128CBC  = asn1.ObjectIdentifier{2, 16, 840, 1, 101, 3, 4, 1, 2}
	oidAES192CBC  = asn1.ObjectIdentifier{2, 16, 840, 1, 101, 3, 4, 1, 22}
	oidAES256CBC  = asn1.ObjectIdentifier{2, 16, 840, 1, 101, 3, 4, 1, 42}
	oidDESEDE3CBC = asn1.ObjectIdentifier{1, 2, 840, 113549, 3, 7}
)

type encryptedPrivateKeyInfo struct {
	Algo pkix.AlgorithmIdentifier
	Data []byte
}

type pbes2Params struct {
	KDF pkix.AlgorithmIdentifier
	Enc pkix.AlgorithmIdentifier
}

type pbkdf2Params struct {
	Salt   []byte
	Iter   int
	KeyLen int                      `asn1:"optional"`
	PRF    pkix.AlgorithmIdentifier `asn1:"optional"`
}

// decryptKeyPEM decrypts a password protected PKCS#8 private key
// ("ENCRYPTED PRIVATE KEY") and returns it re-encoded as a plain PKCS#8 PEM.
func decryptKeyPEM(data []byte, password string) ([]byte, error) {
	var block *pem.Block
	for {
		block, data = pem.Decode(data)
		if block == nil {
			return nil, errors.New("invalid key: no ENCRYPTED PRIVATE KEY section found (PKCS#8 format only)")
		}
		if block.Type == "ENCRYPTED PRIVATE KEY" {
			break
		}
	}
	der, err := decryptPKCS8(block.Bytes, password)
	if err != nil {
		return nil, fmt.Errorf("cannot decrypt key: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), nil
}

func decryptPKCS8(der []byte, password string) ([]byte, error) {
	var info encryptedPrivateKeyInfo
	if _, err := asn1.Unmarshal(der, &info); err != nil {
		return nil, fmt.Errorf("malformed key: %w", err)
	}
	if !info.Algo.Algorithm.Equal(oidPBES2) {
		return nil, fmt.Errorf("unsupported encryption scheme %v (only PBES2 is supported)", info.Algo.Algorithm)
	}
	var params pbes2Params
	if _, err := asn1.Unmarshal(info.Algo.Parameters.FullBytes, &params); err != nil {
		return nil, fmt.Errorf("malformed PBES2 parameters: %w", err)
	}
	if !params.KDF.Algorithm.Equal(oidPBKDF2) {
		return nil, fmt.Errorf("unsupported key derivation function %v (only PBKDF2 is supported)", params.KDF.Algorithm)
	}
	var kdf pbkdf2Params
	if _, err := asn1.Unmarshal(params.KDF.Parameters.FullBytes, &kdf); err != nil {
		return nil, fmt.Errorf("malformed PBKDF2 parameters: %w", err)
	}

	var prf func() hash.Hash
	switch {
	case len(kdf.PRF.Algorithm) == 0, kdf.PRF.Algorithm.Equal(oidHMACSHA1):
		prf = sha1.New
	case kdf.PRF.Algorithm.Equal(oidHMACSHA224):
		prf = sha256.New224
	case kdf.PRF.Algorithm.Equal(oidHMACSHA256):
		prf = sha256.New
	case kdf.PRF.Algorithm.Equal(oidHMACSHA384):
		prf = sha512.New384
	case kdf.PRF.Algorithm.Equal(oidHMACSHA512):
		prf = sha512.New
	default:
		return nil, fmt.Errorf("unsupported PBKDF2 PRF %v", kdf.PRF.Algorithm)
	}

	var keyLen int
	var newCipher func([]byte) (cipher.Block, error)
	switch {
	case params.Enc.Algorithm.Equal(oidAES128CBC):
		keyLen, newCipher = 16, aes.NewCipher
	case params.Enc.Algorithm.Equal(oidAES192CBC):
		keyLen, newCipher = 24, aes.NewCipher
	case params.Enc.Algorithm.Equal(oidAES256CBC):
		keyLen, newCipher = 32, aes.NewCipher
	case params.Enc.Algorithm.Equal(oidDESEDE3CBC):
		keyLen, newCipher = 24, des.NewTripleDESCipher
	default:
		return nil, fmt.Errorf("unsupported cipher %v", params.Enc.Algorithm)
	}
	var iv []byte
	if _, err := asn1.Unmarshal(params.Enc.Parameters.FullBytes, &iv); err != nil {
		return nil, fmt.Errorf("malformed cipher parameters: %w", err)
	}

	key, err := pbkdf2.Key(prf, password, kdf.Salt, kdf.Iter, keyLen)
	if err != nil {
		return nil, err
	}
	block, err := newCipher(key)
	if err != nil {
		return nil, err
	}
	bs := block.BlockSize()
	if len(iv) != bs || len(info.Data) == 0 || len(info.Data)%bs != 0 {
		return nil, errors.New("malformed encrypted data")
	}
	out := make([]byte, len(info.Data))
	cipher.NewCBCDecrypter(block, iv).CryptBlocks(out, info.Data)

	// PKCS#7 padding
	pad := int(out[len(out)-1])
	if pad == 0 || pad > bs || pad > len(out) {
		return nil, errors.New("wrong password or corrupted key")
	}
	for _, b := range out[len(out)-pad:] {
		if int(b) != pad {
			return nil, errors.New("wrong password or corrupted key")
		}
	}
	return out[:len(out)-pad], nil
}
