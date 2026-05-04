package grpc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"
)

// LinkerdDetector provides methods to detect Linkerd presence and manage
// backward compatibility between Linkerd-managed mTLS and manual TLS.
type LinkerdDetector struct {
	// LinkerdCAPath is the path to Linkerd's identity CA bundle.
	// Default: /var/run/linkerd/identity/bundle.crt
	LinkerdCAPath string

	// ManualCertPath is the fallback path for manual TLS certificate.
	// Used when Linkerd is not available.
	ManualCertPath string

	// ManualKeyPath is the fallback path for manual TLS key.
	ManualKeyPath string

	// logger for observability
	logger *slog.Logger

	// cached detection result
	isLinkerdAvailable *bool
}

// NewLinkerdDetector creates a new LinkerdDetector with default paths.
func NewLinkerdDetector() *LinkerdDetector {
	return &LinkerdDetector{
		LinkerdCAPath:  "/var/run/linkerd/identity/bundle.crt",
		ManualCertPath: "/tls/tls.crt",
		ManualKeyPath:  "/tls/tls.key",
		logger:         slog.New(slog.NewTextHandler(os.Stderr, nil)),
	}
}

// IsLinkerdAvailable checks if Linkerd is deployed and injected into this pod.
// It does this by checking if the Linkerd identity CA bundle exists at the
// known path. If Linkerd is available, the sidecar proxy handles all mTLS.
//
// Returns:
//   - true if Linkerd identity CA exists (Linkerd is managing mTLS)
//   - false if the file doesn't exist (fallback to manual TLS required)
//   - error if file I/O fails (actual error condition)
func (ld *LinkerdDetector) IsLinkerdAvailable(ctx context.Context) (bool, error) {
	// Use cached result if available (avoid repeated file I/O)
	if ld.isLinkerdAvailable != nil {
		return *ld.isLinkerdAvailable, nil
	}

	// Check if Linkerd CA bundle exists
	_, err := os.Stat(ld.LinkerdCAPath)
	if err == nil {
		// File exists; Linkerd is available
		result := true
		ld.isLinkerdAvailable = &result

		ld.logger.InfoContext(
			ctx,
			"Linkerd mTLS detected",
			slog.String("ca_path", ld.LinkerdCAPath),
			slog.String("status", "available"),
			slog.String("component", "linkerd_detector"),
		)
		return true, nil
	}

	if errors.Is(err, os.ErrNotExist) {
		// File doesn't exist; Linkerd not injected or not deployed
		result := false
		ld.isLinkerdAvailable = &result

		ld.logger.InfoContext(
			ctx,
			"Linkerd mTLS not available, using manual TLS fallback",
			slog.String("ca_path", ld.LinkerdCAPath),
			slog.String("status", "not_available"),
			slog.String("component", "linkerd_detector"),
		)
		return false, nil
	}

	// Actual error (permission denied, etc.)
	ld.logger.ErrorContext(
		ctx,
		"failed to check Linkerd availability",
		slog.String("ca_path", ld.LinkerdCAPath),
		slog.Any("error", err),
		slog.String("component", "linkerd_detector"),
	)
	return false, fmt.Errorf("failed to stat Linkerd CA: %w", err)
}

// GetMTLSConfig returns the appropriate mTLS configuration based on Linkerd
// availability. If Linkerd is available, it returns the Linkerd mTLS config
// (managed by the sidecar proxy). If not, it returns manual TLS credentials.
//
// Returns an MTLSConfig that can be used to configure both gRPC server and
// client credentials.
func (ld *LinkerdDetector) GetMTLSConfig(ctx context.Context) (*MTLSConfig, error) {
	isLinkerd, err := ld.IsLinkerdAvailable(ctx)
	if err != nil {
		return nil, err
	}

	if isLinkerd {
		// Linkerd manages mTLS; we don't need manual certificates.
		// The sidecar proxy handles all TLS negotiation transparently.
		return &MTLSConfig{
			Type:          MTLSTypeLinkerd,
			IsLinkerd:     true,
			LinkerdCAPath: ld.LinkerdCAPath,
			CertPath:      "",              // Not used when Linkerd is available
			KeyPath:       "",              // Not used when Linkerd is available
			TrustDomain:   "cluster.local", // Default Linkerd trust domain
		}, nil
	}

	// Linkerd not available; use manual TLS
	return &MTLSConfig{
		Type:        MTLSTypeManualTLS,
		IsLinkerd:   false,
		CertPath:    ld.ManualCertPath,
		KeyPath:     ld.ManualKeyPath,
		TrustDomain: "", // Not used with manual TLS
	}, nil
}

// ValidateMTLSFiles checks that the required TLS files exist.
// For Linkerd, this is a no-op (mTLS is injected by sidecar).
// For manual TLS, this verifies that both cert and key files are readable.
func (ld *LinkerdDetector) ValidateMTLSFiles(ctx context.Context) error {
	config, err := ld.GetMTLSConfig(ctx)
	if err != nil {
		return fmt.Errorf("failed to get mTLS config: %w", err)
	}

	if config.IsLinkerd {
		// Linkerd validates certificates on the pod's behalf
		ld.logger.DebugContext(
			ctx,
			"Linkerd mTLS validation delegated to sidecar proxy",
			slog.String("component", "linkerd_detector"),
		)
		return nil
	}

	// Manual TLS: verify files exist and are readable
	if _, err := os.Stat(config.CertPath); err != nil {
		return fmt.Errorf("TLS cert file not found: %w", err)
	}

	if _, err := os.Stat(config.KeyPath); err != nil {
		return fmt.Errorf("TLS key file not found: %w", err)
	}

	ld.logger.DebugContext(
		ctx,
		"manual TLS files validated",
		slog.String("cert_path", config.CertPath),
		slog.String("key_path", config.KeyPath),
		slog.String("component", "linkerd_detector"),
	)
	return nil
}

// WatchMTLSRotation watches for certificate rotation events and triggers
// the provided callback when rotation is detected.
//
// For Linkerd, it polls the CA bundle modification time.
// For manual TLS, it polls the cert file modification time.
//
// This can be used to gracefully restart services when certificates are rotated.
func (ld *LinkerdDetector) WatchMTLSRotation(
	ctx context.Context,
	interval time.Duration,
	onRotation func(ctx context.Context, newConfig *MTLSConfig) error,
) error {
	if interval < 5*time.Second {
		interval = 5 * time.Second // Minimum 5s to avoid busy-looping
	}

	config, err := ld.GetMTLSConfig(ctx)
	if err != nil {
		return fmt.Errorf("failed to get initial mTLS config: %w", err)
	}

	watchPath := config.CertPath
	if config.IsLinkerd {
		watchPath = config.LinkerdCAPath
	}

	// Get initial file modification time
	initialStat, err := os.Stat(watchPath)
	if err != nil {
		return fmt.Errorf("failed to stat watch file: %w", err)
	}
	lastModTime := initialStat.ModTime()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-ticker.C:
			// Check if file has been updated
			stat, err := os.Stat(watchPath)
			if err != nil {
				ld.logger.ErrorContext(
					ctx,
					"failed to check cert file for rotation",
					slog.String("path", watchPath),
					slog.Any("error", err),
					slog.String("component", "linkerd_detector"),
				)
				continue
			}

			if stat.ModTime().After(lastModTime) {
				// File was updated; certificate rotated
				ld.logger.InfoContext(
					ctx,
					"certificate rotation detected",
					slog.String("path", watchPath),
					slog.Time("old_modtime", lastModTime),
					slog.Time("new_modtime", stat.ModTime()),
					slog.String("component", "linkerd_detector"),
				)

				// Reload config and notify callback
				newConfig, err := ld.GetMTLSConfig(ctx)
				if err != nil {
					ld.logger.ErrorContext(
						ctx,
						"failed to reload mTLS config after rotation",
						slog.Any("error", err),
						slog.String("component", "linkerd_detector"),
					)
					continue
				}

				if err := onRotation(ctx, newConfig); err != nil {
					ld.logger.ErrorContext(
						ctx,
						"callback failed to handle certificate rotation",
						slog.Any("error", err),
						slog.String("component", "linkerd_detector"),
					)
					return err
				}

				lastModTime = stat.ModTime()
			}
		}
	}
}

// MTLSType indicates the source of mTLS credentials
type MTLSType string

const (
	// MTLSTypeLinkerd means mTLS is managed by Linkerd sidecar proxy
	MTLSTypeLinkerd MTLSType = "linkerd"

	// MTLSTypeManualTLS means mTLS is managed manually via cert-manager or similar
	MTLSTypeManualTLS MTLSType = "manual"
)

// MTLSConfig holds the mTLS configuration for both Linkerd and manual TLS.
type MTLSConfig struct {
	// Type indicates whether mTLS is managed by Linkerd or manually
	Type MTLSType

	// IsLinkerd is true if this pod is injected and mTLS is Linkerd-managed
	IsLinkerd bool

	// LinkerdCAPath is the path to Linkerd's identity CA bundle (used when IsLinkerd=true)
	LinkerdCAPath string

	// CertPath is the path to the TLS certificate (used for manual TLS)
	CertPath string

	// KeyPath is the path to the TLS key (used for manual TLS)
	KeyPath string

	// TrustDomain is the Linkerd trust domain (default: cluster.local)
	TrustDomain string
}

// TLSDescription returns a human-readable description of the mTLS configuration
func (cfg *MTLSConfig) TLSDescription() string {
	if cfg.IsLinkerd {
		return fmt.Sprintf(
			"Linkerd-managed mTLS (trust-domain=%s, CA=%s)",
			cfg.TrustDomain, cfg.LinkerdCAPath,
		)
	}
	return fmt.Sprintf(
		"Manual TLS (cert=%s, key=%s)",
		cfg.CertPath, cfg.KeyPath,
	)
}
