package integration_tests

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/apernet/hysteria/core/v2/client"
	coreErrs "github.com/apernet/hysteria/core/v2/errors"
	"github.com/apernet/hysteria/core/v2/internal/integration_tests/mocks"
	"github.com/apernet/hysteria/core/v2/server"
)

// Reproduce #1696 on real connections, then verify recovery and cleanup with
// its prerequisite fix. Kept on a companion branch, not bundled in the pool PR.
func TestConnectionPoolServerRestartCleanup(t *testing.T) {
	u, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := u.LocalAddr()
	auth := mocks.NewMockAuthenticator(t)
	auth.EXPECT().Authenticate(mock.Anything, "pool-test", uint64(0)).Return(true, "pool-test")
	s, err := server.NewServer(&server.Config{TLSConfig: serverTLSConfig(), Conn: u, Authenticator: auth})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	go s.Serve()
	factory := &poolTrackingFactory{}
	t.Cleanup(func() {
		for _, conn := range factory.snapshot() {
			_ = conn.Close()
		}
	})
	var members []client.Client
	c, err := client.NewReconnectableClientPool(func() (*client.Config, error) {
		return &client.Config{
			ConnFactory: factory, ServerAddr: addr, Auth: "pool-test",
			TLSConfig:  client.TLSConfig{InsecureSkipVerify: true},
			QUICConfig: client.QUICConfig{DisableChromeParrot: true, MaxIdleTimeout: 4 * time.Second, KeepAlivePeriod: 2 * time.Second},
		}, nil
	}, func(member client.Client, _ *client.HandshakeInfo, _ int) {
		members = append(members, member)
	}, false, 2)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	retired := factory.snapshot()
	require.Len(t, retired, 2)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	echo := &tcpEchoServer{Listener: l}
	t.Cleanup(func() { _ = echo.Close() })
	go echo.Serve()
	for cycle := range 3 {
		retired = factory.snapshot()
		require.NoError(t, s.Close())
		for _, member := range members[:2] {
			require.Eventually(t, func() bool {
				uc, err := member.UDP()
				if err == nil {
					_ = uc.Close()
					return false
				}
				_, closed := err.(coreErrs.ClosedError)
				return closed
			}, 8*time.Second, 5*time.Millisecond)
		}
		for index, conn := range retired {
			_, err = conn.WriteToUDP([]byte{0}, addr.(*net.UDPAddr))
			require.ErrorIs(t, err, net.ErrClosed, "retired socket %d remains open", index)
		}
		u, err = net.ListenPacket("udp", addr.String())
		require.NoError(t, err)
		s, err = server.NewServer(&server.Config{TLSConfig: serverTLSConfig(), Conn: u, Authenticator: auth})
		require.NoError(t, err)
		go s.Serve()
		for range 2 {
			conn, err := c.TCP(l.Addr().String())
			require.NoError(t, err)
			poolCheckEcho(t, conn, byte(cycle))
			require.NoError(t, conn.Close())
		}
		require.Len(t, factory.snapshot(), 2*(cycle+2))
	}
	require.NoError(t, c.Close())
	for _, conn := range factory.snapshot() {
		_, err = conn.WriteToUDP([]byte{0}, addr.(*net.UDPAddr))
		require.ErrorIs(t, err, net.ErrClosed)
	}
}
