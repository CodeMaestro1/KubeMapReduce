package main

import "testing"

func TestHasRealmRole_CaseInsensitive(t *testing.T) {
	claims := map[string]any{
		"realm_access": map[string]any{
			"roles": []any{"admin", "UsEr", "viewer"},
		},
	}

	tests := []struct {
		name string
		role string
		want bool
	}{
		{name: "lowercase query matches uppercase role", role: "user", want: true},
		{name: "uppercase query matches lowercase role", role: "ADMIN", want: true},
		{name: "mixed query matches mixed role", role: "uSeR", want: true},
		{name: "unknown role", role: "operator", want: false},
		{name: "empty role", role: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := hasRealmRole(claims, tt.role)
			if got != tt.want {
				t.Fatalf("hasRealmRole(%q) = %v, want %v", tt.role, got, tt.want)
			}
		})
	}
}

func TestHasRealmRole_TrimsRoleValues(t *testing.T) {
	claims := map[string]any{
		"realm_access": map[string]any{
			"roles": []any{"  ADMIN  "},
		},
	}

	if !hasRealmRole(claims, "admin") {
		t.Fatal("expected trimmed role to match case-insensitively")
	}
}

func TestHasAdminRole_CaseInsensitive(t *testing.T) {
	tests := []struct {
		name  string
		roles []any
		want  bool
	}{
		{name: "lowercase admin", roles: []any{"admin"}, want: true},
		{name: "uppercase admin", roles: []any{"ADMIN"}, want: true},
		{name: "mixed-case admin", roles: []any{"AdMiN"}, want: true},
		{name: "non-admin only", roles: []any{"USER"}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			claims := map[string]any{
				"realm_access": map[string]any{
					"roles": tt.roles,
				},
			}

			if got := hasAdminRole(claims); got != tt.want {
				t.Fatalf("hasAdminRole() = %v, want %v", got, tt.want)
			}
		})
	}
}
