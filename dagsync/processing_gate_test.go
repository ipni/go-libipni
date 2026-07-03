package dagsync

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
)

func TestSyncGate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := &handler{syncReadyGuard: make(chan struct{}, 1)}

		ctx := t.Context()
		requireNoWait := func() {
			t.Helper()
			require.NoError(t, h.acquireSyncGate(ctx))
			h.releaseSyncGate()
		}
		requireNoWait()

		require.NoError(t, h.acquireSyncGate(ctx))

		waitDone := make(chan struct{})
		go func() {
			defer close(waitDone)
			require.NoError(t, h.acquireSyncGate(ctx))
		}()

		synctest.Wait()
		select {
		case <-waitDone:
			t.Fatal("second acquire returned before release")
		default:
		}

		h.releaseSyncGate()

		synctest.Wait()
		select {
		case <-waitDone:
		default:
			t.Fatal("second acquire did not return after release")
		}

		h.releaseSyncGate()
		requireNoWait()
	})
}

func TestSyncGateWaitCanceled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := &handler{syncReadyGuard: make(chan struct{}, 1)}
		require.NoError(t, h.acquireSyncGate(t.Context()))

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		waitDone := make(chan error, 1)
		go func() {
			waitDone <- h.acquireSyncGate(ctx)
		}()

		synctest.Wait()

		cancel()
		synctest.Wait()

		select {
		case err := <-waitDone:
			require.ErrorIs(t, err, context.Canceled)
		default:
			t.Fatal("acquire did not return on cancel")
		}
	})
}
