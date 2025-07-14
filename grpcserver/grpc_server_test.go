/*
Copyright © 2025 Acronis International GmbH.

Released under MIT license.
*/

package grpcserver

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/interop/grpc_testing"

	"github.com/acronis/go-appkit/log/logtest"
)

func TestNew(t *testing.T) {
	logger := logtest.NewRecorder()

	t.Run("basic server creation", func(t *testing.T) {
		cfg := NewDefaultConfig()

		server, err := New(cfg, logger)
		require.NoError(t, err)
		require.NotNil(t, server)
		require.Equal(t, cfg.Address, server.Address())
		require.NotNil(t, server.GRPCServer)
		require.Equal(t, logger, server.Logger)
	})

	t.Run("server with TLS", func(t *testing.T) {
		tmpDir := t.TempDir()
		certFile := filepath.Join(tmpDir, "cert.pem")
		keyFile := filepath.Join(tmpDir, "key.pem")

		// Generate test certificates
		require.NoError(t, generateTestCertificate(certFile, keyFile))

		cfg := NewDefaultConfig()
		cfg.Address = "localhost:0"
		cfg.TLS.Enabled = true
		cfg.TLS.Certificate = certFile
		cfg.TLS.Key = keyFile

		server, err := New(cfg, logger)
		require.NoError(t, err)
		require.NotNil(t, server)

		// Register test service
		grpc_testing.RegisterTestServiceServer(server.GRPCServer, &testGRPCService{})

		// Start server
		fatalErrorChan := make(chan error, 1)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			server.Start(fatalErrorChan)
		}()

		// Wait for server to start
		time.Sleep(100 * time.Millisecond)

		// Create TLS credentials for client using the generated certificate
		creds, err := buildGRPCTLSCredentials(certFile)
		require.NoError(t, err)

		// Test connection with TLS
		conn, err := grpc.NewClient(server.Address(), grpc.WithTransportCredentials(creds))
		require.NoError(t, err)
		defer conn.Close()

		// Create client and make a call
		client := grpc_testing.NewTestServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		resp, err := client.UnaryCall(ctx, &grpc_testing.SimpleRequest{
			Payload: &grpc_testing.Payload{Body: []byte("tls-test")},
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, "tls-test", string(resp.Payload.Body))

		// Stop server
		err = server.Stop(true)
		require.NoError(t, err)

		wg.Wait()

		// Check that no fatal error occurred
		select {
		case err = <-fatalErrorChan:
			t.Fatalf("unexpected fatal error: %v", err)
		default:
		}
	})

	t.Run("server with invalid TLS certificates", func(t *testing.T) {
		cfg := NewDefaultConfig()
		cfg.TLS.Enabled = true
		cfg.TLS.Certificate = "/nonexistent/cert.pem"
		cfg.TLS.Key = "/nonexistent/key.pem"

		server, err := New(cfg, logger)
		require.Error(t, err)
		require.Nil(t, server)
		require.Contains(t, err.Error(), "load TLS certificates")
	})

	t.Run("server with custom interceptors", func(t *testing.T) {
		cfg := NewDefaultConfig()

		customUnaryInterceptor := func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
			return handler(ctx, req)
		}
		customStreamInterceptor := func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			return handler(srv, stream)
		}

		server, err := New(cfg, logger,
			WithUnaryInterceptors(customUnaryInterceptor),
			WithStreamInterceptors(customStreamInterceptor))
		require.NoError(t, err)
		require.NotNil(t, server)
	})

	t.Run("server with metrics", func(t *testing.T) {
		cfg := NewDefaultConfig()

		server, err := New(cfg, logger, WithGRPCCallMetricsOptions(GRPCCallMetricsOptions{
			Namespace: "test",
		}))
		require.NoError(t, err)
		require.NotNil(t, server)
		require.NotNil(t, server.grpcReqPrometheusMetrics)
	})
}

func TestGRPCServer_StartAndStop(t *testing.T) {
	logger := logtest.NewRecorder()
	tempDirPath := t.TempDir() // t.TempDir() is called here to avoid long path issue with unix sockets

	t.Run("start and stop TCP server", func(t *testing.T) {
		cfg := NewDefaultConfig()
		cfg.Address = ":0" // Use random available port

		server, err := New(cfg, logger)
		require.NoError(t, err)

		// Register test service
		grpc_testing.RegisterTestServiceServer(server.GRPCServer, &testGRPCService{})

		// Test server start
		fatalErrorChan := make(chan error, 1)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			server.Start(fatalErrorChan)
		}()

		// Wait a bit for server to start
		time.Sleep(100 * time.Millisecond)

		// Check that server address is set
		require.NotEmpty(t, server.Address())

		// Test graceful stop
		err = server.Stop(true)
		require.NoError(t, err)

		wg.Wait()

		// Check that no fatal error occurred
		select {
		case err = <-fatalErrorChan:
			t.Fatalf("unexpected fatal error: %v", err)
		default:
		}
	})

	t.Run("start and stop with unix socket", func(t *testing.T) {
		socketPath := filepath.Join(tempDirPath, "s.sock")

		cfg := NewDefaultConfig()
		cfg.UnixSocketPath = socketPath

		server, err := New(cfg, logger)
		require.NoError(t, err)

		// Register test service
		grpc_testing.RegisterTestServiceServer(server.GRPCServer, &testGRPCService{})

		// Test server start
		fatalErrorChan := make(chan error, 1)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			server.Start(fatalErrorChan)
		}()

		// Wait a bit for server to start
		time.Sleep(200 * time.Millisecond)

		// Check if server had fatal error
		select {
		case err := <-fatalErrorChan:
			t.Fatalf("server failed to start: %v", err)
		default:
		}

		// Verify address is set for unix socket
		require.Equal(t, socketPath, server.Address())

		// Verify socket file exists
		_, err = os.Stat(socketPath)
		require.NoError(t, err, "socket file should exist")

		// Test connection via unix socket
		conn, err := grpc.NewClient("unix:"+socketPath, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		defer conn.Close()

		// Create client and make a call
		client := grpc_testing.NewTestServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		resp, err := client.UnaryCall(ctx, &grpc_testing.SimpleRequest{
			Payload: &grpc_testing.Payload{Body: []byte("unix-test")},
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, "unix-test", string(resp.Payload.Body))

		// Test force stop
		err = server.Stop(false)
		require.NoError(t, err)

		wg.Wait()

		// Check that no fatal error occurred
		select {
		case err := <-fatalErrorChan:
			t.Fatalf("unexpected fatal error: %v", err)
		default:
		}
	})

	t.Run("server start with invalid address", func(t *testing.T) {
		cfg := NewDefaultConfig()
		cfg.Address = "invalid-address"

		server, err := New(cfg, logger)
		require.NoError(t, err)

		fatalErrorChan := make(chan error, 1)
		go server.Start(fatalErrorChan)

		// Expect a fatal error
		select {
		case err := <-fatalErrorChan:
			require.Error(t, err)
		case <-time.After(time.Second):
			t.Fatal("expected fatal error but none received")
		}
	})
}

func TestGRPCServer_MetricsRegistration(t *testing.T) {
	logger := logtest.NewRecorder()
	cfg := NewDefaultConfig()

	t.Run("register and unregister metrics", func(t *testing.T) {
		server, err := New(cfg, logger, WithGRPCCallMetricsOptions(GRPCCallMetricsOptions{
			Namespace: "test",
		}))
		require.NoError(t, err)
		require.NotNil(t, server.grpcReqPrometheusMetrics)

		// Test metrics registration
		require.NotPanics(t, func() {
			server.MustRegisterMetrics()
		})

		// Test metrics unregistration
		server.UnregisterMetrics()
	})
}

func TestGRPCServer_Integration(t *testing.T) {
	logger := logtest.NewRecorder()

	t.Run("full server lifecycle with client connection", func(t *testing.T) {
		cfg := NewDefaultConfig()
		cfg.Address = ":0"

		server, err := New(cfg, logger)
		require.NoError(t, err)

		// Register test service
		grpc_testing.RegisterTestServiceServer(server.GRPCServer, &testGRPCService{})

		// Start server
		fatalErrorChan := make(chan error, 1)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			server.Start(fatalErrorChan)
		}()

		// Wait for server to start
		time.Sleep(100 * time.Millisecond)

		// Create client connection
		addr := server.Address()
		require.NotEmpty(t, addr)

		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		defer conn.Close()

		// Create client and make a call
		client := grpc_testing.NewTestServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		resp, err := client.UnaryCall(ctx, &grpc_testing.SimpleRequest{
			Payload: &grpc_testing.Payload{Body: []byte("test")},
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, "test", string(resp.Payload.Body))

		// Stop server
		err = server.Stop(true)
		require.NoError(t, err)

		wg.Wait()

		// Check that no fatal error occurred
		select {
		case err := <-fatalErrorChan:
			t.Fatalf("unexpected fatal error: %v", err)
		default:
		}

		// Verify logging occurred
		require.Greater(t, len(logger.Entries()), 0)

		// Check for server start log
		found := false
		for _, entry := range logger.Entries() {
			if strings.Contains(entry.Text, "starting gRPC server") {
				found = true
				break
			}
		}
		require.True(t, found, "expected to find 'starting gRPC server' log entry")
	})
}

type testGRPCService struct {
	grpc_testing.UnimplementedTestServiceServer
}

func (s *testGRPCService) UnaryCall(ctx context.Context, req *grpc_testing.SimpleRequest) (*grpc_testing.SimpleResponse, error) {
	return &grpc_testing.SimpleResponse{
		Payload: &grpc_testing.Payload{Body: req.Payload.Body},
	}, nil
}

// generateTestCertificate creates a temporary certificate for testing
func generateTestCertificate(certFilePath, privKeyPath string) error {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("failed to generate private key: %w", err)
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject: pkix.Name{
			Organization: []string{"Test Organization"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:              []string{"localhost"},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return fmt.Errorf("create certificate: %w", err)
	}

	// Write certificate
	certOut, err := os.Create(certFilePath)
	if err != nil {
		return fmt.Errorf("create %q for writing: %w", certFilePath, err)
	}
	defer certOut.Close()
	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: certDER}); err != nil {
		return fmt.Errorf("write certificate: %w", err)
	}

	// Write private key
	keyOut, err := os.Create(privKeyPath)
	if err != nil {
		return fmt.Errorf("create %q for writing: %w", privKeyPath, err)
	}
	defer keyOut.Close()

	privBytes, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		return fmt.Errorf("marshal private key: %w", err)
	}
	if err := pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: privBytes}); err != nil {
		return fmt.Errorf("write private key: %w", err)
	}

	return nil
}

// buildGRPCTLSCredentials creates gRPC TLS credentials using the provided certificate file
func buildGRPCTLSCredentials(certPath string) (credentials.TransportCredentials, error) {
	// Set up our own certificate pool
	certPool := x509.NewCertPool()

	// Load our trusted certificate
	pemData, err := os.ReadFile(certPath)
	if err != nil {
		return nil, fmt.Errorf("read certificate file: %w", err)
	}

	if !certPool.AppendCertsFromPEM(pemData) {
		return nil, fmt.Errorf("failed to append certificate to pool")
	}

	// Create TLS config with the certificate pool
	// Note: ServerName should match the server's hostname or be left empty for IP addresses
	tlsConfig := &tls.Config{
		RootCAs:    certPool,
		ServerName: "localhost", // Must match the certificate's DNS name
	}

	return credentials.NewTLS(tlsConfig), nil
}
