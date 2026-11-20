package main

import "testing"

// La API y el consumer corren fuera del host de ClickHouse: sin credenciales
// explícitas el usuario `default` sólo acepta conexiones locales.
func TestClickHouseUsesCredentialsFromEnv(t *testing.T) {
	t.Setenv("CLICKHOUSE_URL", "http://clickhouse:8123")
	t.Setenv("CLICKHOUSE_USER", "analytics")
	t.Setenv("CLICKHOUSE_PASSWORD", "analytics")

	ch := newClickHouseFromEnv()
	if ch.user != "analytics" || ch.password != "analytics" {
		t.Fatalf("credenciales = %q/%q, quería analytics/analytics", ch.user, ch.password)
	}
}
